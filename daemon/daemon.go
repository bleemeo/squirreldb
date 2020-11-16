// Package daemon contains startup function of SquirrelDB
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"squirreldb/api"
	"squirreldb/batch"
	"squirreldb/cassandra/index"
	"squirreldb/cassandra/locks"
	"squirreldb/cassandra/session"
	"squirreldb/cassandra/states"
	"squirreldb/cassandra/tsdb"
	"squirreldb/cassandra/wal"
	"squirreldb/cluster"
	"squirreldb/cluster/seed"
	"squirreldb/config"
	"squirreldb/debug"
	"squirreldb/distributor"
	"squirreldb/dummy"
	"squirreldb/memorystore"
	"squirreldb/redis"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gocql/gocql"
)

// nolint: gochecknoglobals
var (
	Version string = "unset"
	Commit  string
	Date    string
)

const (
	backendCassandra = "cassandra"
	backendDummy     = "dummy"
	backendBatcher   = "batcher"
)

type LockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type MetricReadWriter interface {
	types.MetricReader
	types.MetricWriter
}

// SquirrelDB is the SquirrelDB process itself. The Prometheus remote-store.
type SquirrelDB struct {
	Config                   *config.Config
	cassandraSession         *gocql.Session
	cassandraKeyspaceCreated bool
	lockFactory              LockFactory
	states                   types.State
	temporaryStore           batch.TemporaryStore
	index                    types.Index
	persistentStore          MetricReadWriter
	store                    MetricReadWriter
	api                      api.API
	cluster                  types.Cluster
}

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

var errBadConfig = errors.New("configuration validation failed")

// RunRetry will run SquirrelDB (possibly Init()ializing it) and retry on errors.
// It may still fait on permanent error (like bad configuation).
func (s *SquirrelDB) RunRetry(ctx context.Context) error {
	return retry.Print(func() error {
		err := s.Run(ctx)
		if err == errBadConfig {
			return backoff.Permanent(err)
		}

		return err
	}, retry.NewExponentialBackOff(ctx, 30*time.Second),
		logger,
		"Running SquirrelDB",
	)
}

// Run will run SquirrelDB (possibly Init()ializing it).
// On error, we can retry calling Run() which will resume starting SquirrelDB.
func (s *SquirrelDB) Run(ctx context.Context) error {
	err := s.Init()
	if err != nil {
		return err
	}

	_, err = s.Index(true)
	if err != nil {
		return err
	}

	_, err = s.TSDB(true)
	if err != nil {
		return err
	}

	readiness := make(chan error)
	runTerminated := make(chan interface{})

	go func() {
		s.run(ctx, readiness)
		close(runTerminated)
	}()

	err = <-readiness
	if err != nil {
		return err
	}

	<-runTerminated

	return err
}

// Init initialize SquirrelDB Locks & State. It also validate configuration with cluster (need Cassandra access)
// Init could be retried.
func (s *SquirrelDB) Init() error {
	if !s.Config.Validate() {
		return errBadConfig
	}

	states, err := s.States()
	if err != nil {
		return err
	}

	if !s.Config.ValidateRemote(states) {
		return errBadConfig
	}

	return nil
}

// RunWithSignalHandler runs given function with a context that is canceled on kill or ctrl+c
// If a second ctrl+c is received, return even if f didn't completed.
func RunWithSignalHandler(f func(context.Context) error) error {
	signalChan := make(chan os.Signal, 1)
	runTerminated := make(chan error, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		signal.Stop(signalChan)
		close(signalChan)
		cancel()
	}()

	go func() {
		runTerminated <- f(ctx)
	}()

	firstStop := true

	for {
		select {
		case err := <-runTerminated:
			cancel()
			return err
		case <-signalChan:
			if firstStop {
				logger.Println("Received stop signal, start graceful shutdown")

				cancel()

				firstStop = false
			} else {
				logger.Println("Forced stop")

				return errors.New("forced shutdown")
			}
		}
	}
}

// New return a SquirrelDB not yet initialized. Only configuration is loaded and validated.
func New() (squirreldb *SquirrelDB, err error) {
	cfg, err := config.New()

	if err != nil {
		return nil, fmt.Errorf("error: Can't load config: %w", err)
	}

	squirreldb = &SquirrelDB{
		Config: cfg,
	}

	if cfg.Bool("help") {
		cfg.FlagSet.PrintDefaults()
		os.Exit(0)
	}

	if cfg.Bool("version") {
		fmt.Println(Version)
		os.Exit(0)
	}

	if cfg.Bool("build-info") {
		fmt.Printf("Built at %s using %s from commit %s\n", Date, runtime.Version(), Commit)
		fmt.Printf("Version %s\n", Version)

		os.Exit(0)
	}

	if !cfg.Validate() {
		return squirreldb, errBadConfig
	}

	debug.Level = cfg.Int("log.level")

	return squirreldb, nil
}

// SchemaLock return a lock to modify the Cassandra schema.
func (s *SquirrelDB) SchemaLock() (types.TryLocker, error) {
	lockFactory, err := s.LockFactory()
	if err != nil {
		return nil, err
	}

	return lockFactory.CreateLock("cassandra-schema", 10*time.Second), nil
}

// CassandraSession return the Cassandra session used for SquirrelDB.
func (s *SquirrelDB) CassandraSession() (*gocql.Session, error) {
	if s.cassandraSession == nil {
		options := session.Options{
			Addresses:         s.Config.Strings("cassandra.addresses"),
			ReplicationFactor: s.Config.Int("cassandra.replication_factor"),
			Keyspace:          s.Config.String("cassandra.keyspace"),
		}

		session, keyspaceCreated, err := session.New(options)
		if err != nil {
			return nil, err
		}

		s.cassandraSession = session
		s.cassandraKeyspaceCreated = keyspaceCreated
	}

	return s.cassandraSession, nil
}

// LockFactory return the Lock factory of SquirrelDB.
func (s *SquirrelDB) LockFactory() (LockFactory, error) {
	if s.lockFactory == nil {
		switch s.Config.String("internal.locks") {
		case backendCassandra:
			session, err := s.CassandraSession()
			if err != nil {
				return nil, err
			}

			factory, err := locks.New(session, s.cassandraKeyspaceCreated)
			if err != nil {
				return nil, err
			}

			s.lockFactory = factory
		case backendDummy:
			logger.Println("Warning: Using dummy lock factory (only work on single node)")

			s.lockFactory = &dummy.Locks{}
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.String("internal.locks"))
		}
	}

	return s.lockFactory, nil
}

func (s *SquirrelDB) States() (types.State, error) {
	if s.states == nil {
		switch s.Config.String("internal.states") {
		case backendCassandra:
			session, err := s.CassandraSession()
			if err != nil {
				return nil, err
			}

			lock, err := s.SchemaLock()
			if err != nil {
				return nil, err
			}

			states, err := states.New(session, lock)
			if err != nil {
				return nil, err
			}

			s.states = states
		case backendDummy:
			logger.Println("Warning: Cassandra is disabled for states. Using dummy states store (only in-memory and single-node)")

			s.states = &dummy.States{}
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.String("internal.states"))
		}
	}

	return s.states, nil
}

type namedTasks struct {
	Name string
	Task types.OldTask
}

func (s *SquirrelDB) apiTask(ctx context.Context, readiness chan error) {
	s.api.ListenAddress = s.Config.String("remote_storage.listen_address")
	s.api.Index = s.index
	s.api.Reader = s.store
	s.api.Writer = s.store
	s.api.PromQLMaxEvaluatedPoints = uint64(s.Config.Int64("promql.max_evaluated_points"))
	s.api.PromQLMaxEvaluatedSeries = uint32(s.Config.Int("promql.max_evaluated_series"))
	s.api.MaxConcurrentRemoteWrite = s.Config.Int("remote_storage.max_concurrent_write")

	s.api.Run(ctx, readiness)
}

// run start SquirrelDB.
func (s *SquirrelDB) run(ctx context.Context, readiness chan error) {
	tasks := []namedTasks{
		{
			Name: "cluster",
			Task: types.TaskFun(s.clusterTask),
		},
		{
			Name: "batching store",
			Task: types.TaskFun(s.batchStoreTask),
		},
		{
			Name: "API",
			Task: types.TaskFun(s.apiTask),
		},
	}

	ctxs := make([]context.Context, len(tasks))
	cancels := make([]context.CancelFunc, len(tasks))
	waitChan := make([]chan interface{}, len(tasks))

	for i, task := range tasks {
		task := task
		i := i
		subReadiness := make(chan error)

		ctxs[i], cancels[i] = context.WithCancel(context.Background())

		err := retry.Print(func() error {
			waitChan[i] = make(chan interface{})

			go func() {
				task.Task.Run(ctxs[i], subReadiness)
				close(waitChan[i])
			}()

			err := <-subReadiness
			if err != nil {
				<-waitChan[i]
			}

			return err
		}, retry.NewExponentialBackOff(ctx, 30*time.Second),
			logger,
			fmt.Sprintf("Starting %s", task.Name),
		)

		if ctx.Err() != nil {
			if err == nil {
				tasks = tasks[:i+1]
			} else {
				tasks = tasks[:i]
			}

			break
		} else {
			debug.Print(2, logger, "Task %s started", task.Name)
		}
	}

	readiness <- ctx.Err()

	s.api.Ready()

	<-ctx.Done()

	for i := len(tasks) - 1; i >= 0; i-- {
		cancels[i]()
		<-waitChan[i]
		debug.Print(2, logger, "Task %s stopped", tasks[i].Name)
	}

	if s.cassandraSession != nil {
		s.cassandraSession.Close()
	}
}

// Index return an Index. If started is true the index is started.
func (s *SquirrelDB) Index(started bool) (types.Index, error) {
	if s.index == nil {
		switch s.Config.String("internal.index") {
		case backendCassandra:
			session, err := s.CassandraSession()
			if err != nil {
				return nil, err
			}

			schemaLock, err := s.SchemaLock()
			if err != nil {
				return nil, err
			}

			options := index.Options{
				DefaultTimeToLive: s.Config.Duration("cassandra.default_time_to_live"),
				IncludeID:         s.Config.Bool("index.include_id"),
				LockFactory:       s.lockFactory,
				States:            s.states,
				SchemaLock:        schemaLock,
			}

			index, err := index.New(session, options)
			if err != nil {
				return nil, err
			}

			s.index = index
			s.api.IndexVerifyCallback = index.Verify
		case backendDummy:
			logger.Println("Warning: Using dummy for index (only do this for testing)")

			s.index = &dummy.Index{
				StoreMetricIDInMemory: s.Config.Bool("internal.dummy_index_check_conflict"),
				FixedValue:            types.MetricID(s.Config.Int64("internal.dummy_index_fixed_id")),
			}
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.String("internal.index"))
		}
	}

	if task, ok := s.index.(types.Task); started && ok {
		err := task.Start()
		if err != nil {
			return s.index, err
		}
	}

	return s.index, nil
}

// TSDB return the metric persistent store. If started is true the tsdb is started.
func (s *SquirrelDB) TSDB(preAggregationStarted bool) (MetricReadWriter, error) {
	if s.persistentStore == nil {
		switch s.Config.String("internal.tsdb") {
		case backendCassandra:
			session, err := s.CassandraSession()
			if err != nil {
				return nil, err
			}

			schemaLock, err := s.SchemaLock()
			if err != nil {
				return nil, err
			}

			index, err := s.Index(false)
			if err != nil {
				return nil, err
			}

			lockFactory, err := s.LockFactory()
			if err != nil {
				return nil, err
			}

			states, err := s.States()
			if err != nil {
				return nil, err
			}

			options := tsdb.Options{
				DefaultTimeToLive:         s.Config.Duration("cassandra.default_time_to_live"),
				BatchSize:                 s.Config.Duration("batch.size"),
				RawPartitionSize:          s.Config.Duration("cassandra.partition_size.raw"),
				AggregatePartitionSize:    s.Config.Duration("cassandra.partition_size.aggregate"),
				AggregateResolution:       s.Config.Duration("cassandra.aggregate.resolution"),
				AggregateSize:             s.Config.Duration("cassandra.aggregate.size"),
				AggregateIntendedDuration: s.Config.Duration("cassandra.aggregate.intended_duration"),
				SchemaLock:                schemaLock,
			}

			tsdb, err := tsdb.New(session, options, index, lockFactory, states)
			if err != nil {
				return nil, err
			}

			s.persistentStore = tsdb
			s.api.PreAggregateCallback = tsdb.ForcePreAggregation
		case backendDummy:
			logger.Println("Warning: Cassandra is disabled for TSDB. Using dummy states store that discard every write")

			s.persistentStore = &dummy.DiscardTSDB{}
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.String("internal.tsdb"))
		}
	}

	if task, ok := s.persistentStore.(types.Task); preAggregationStarted && ok {
		err := task.Start()
		if err != nil {
			return s.persistentStore, err
		}
	}

	return s.persistentStore, nil
}

func (s *SquirrelDB) clusterTask(ctx context.Context, readiness chan error) {
	if s.Config.Bool("cluster.enabled") {
		session, err := s.CassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		schemaLock, err := s.SchemaLock()
		if err != nil {
			readiness <- err
			return
		}

		seeds := &seed.Cassandra{
			Session:    session,
			SchemaLock: schemaLock,
		}

		var wg sync.WaitGroup

		subCtx, cancel := context.WithCancel(context.Background())
		subReady := make(chan error)

		wg.Add(1)

		go func() {
			defer wg.Done()
			seeds.Run(subCtx, subReady)
		}()

		err = <-subReady
		if err != nil {
			cancel()
			readiness <- err

			wg.Wait()

			return
		}

		m := &cluster.Cluster{
			SeedProvider:     seeds,
			APIListenAddress: s.Config.String("remote_storage.listen_address"),
			ClusterAddress:   s.Config.String("cluster.bind_address"),
			ClusterPort:      s.Config.Int("cluster.bind_port"),
		}

		s.cluster = m
		m.Run(ctx, readiness)
		cancel()
		wg.Wait()
	} else {
		s.cluster = nil
		readiness <- nil
		<-ctx.Done()
	}
}

func (s *SquirrelDB) temporaryStoreTask(ctx context.Context, readiness chan error) {
	switch s.Config.String("internal.temporary_store") {
	case "redis":
		redisAddresses := s.Config.Strings("redis.addresses")
		if len(redisAddresses) > 0 && redisAddresses[0] != "" {
			options := redis.Options{
				Addresses: redisAddresses,
			}

			s.temporaryStore = redis.New(options)
			readiness <- nil

			<-ctx.Done()
		} else {
			mem := memorystore.New()
			s.temporaryStore = mem
			readiness <- nil
			mem.Run(ctx)
		}
	default:
		readiness <- fmt.Errorf("unknown backend: %v", s.Config.String("internal.temporary_store"))
	}
}

func (s *SquirrelDB) batchStoreTask(ctx context.Context, readiness chan error) {
	switch s.Config.String("internal.store") {
	case backendBatcher:
		squirrelBatchSize := s.Config.Duration("batch.size")

		var wg sync.WaitGroup

		subCtx, cancel := context.WithCancel(context.Background())
		subReady := make(chan error)

		wg.Add(1)

		go func() {
			defer wg.Done()
			s.temporaryStoreTask(subCtx, subReady)
		}()

		err := <-subReady
		if err != nil {
			cancel()
			readiness <- err

			wg.Wait()

			return
		}

		batch := batch.New(squirrelBatchSize, s.temporaryStore, s.persistentStore, s.persistentStore)
		s.store = batch
		s.api.FlushCallback = batch.Flush

		readiness <- nil

		batch.Run(ctx)
		cancel()
		wg.Wait()
	case backendDummy:
		logger.Println("Warning: SquirrelDB is configured to discard every write")

		s.store = dummy.DiscardTSDB{}

		readiness <- nil

		<-ctx.Done()
	case "wal":
		session, err := s.CassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		schemaLock, err := s.SchemaLock()
		if err != nil {
			readiness <- err
			return
		}

		wal := &batch.WalBatcher{
			WalStore: &wal.Cassandra{
				ShardID:    1,
				Session:    session,
				SchemaLock: schemaLock,
			},
			PersitentStore: s.persistentStore,
		}

		s.store = wal
		s.api.FlushCallback = func() error { wal.Flush(); return nil }

		wal.Run(ctx, readiness)
	case "distributor", "distributor2discard":
		session, err := s.CassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		if s.cluster == nil {
			logger.Println("Cluster is disabled. Only one SquirrelDB should access Cassandra")
		}

		schemaLock, err := s.SchemaLock()
		if err != nil {
			readiness <- err
			return
		}

		store := &distributor.Distributor{
			Cluster:    s.cluster,
			ShardCount: s.Config.Int("cluster.shard"),
			StoreFactory: func(shardID int) distributor.FlushableStore {
				if s.Config.String("internal.store") == "distributor2discard" {
					return &dummy.DiscardTSDB{}
				}
				return &batch.WalBatcher{
					WalStore: &wal.Cassandra{
						ShardID:    shardID,
						Session:    session,
						SchemaLock: schemaLock,
					},
					PersitentStore: s.persistentStore,
				}
			},
		}

		if s.cluster == nil {
			// Cluster is disabled, no need to spread on multiple shard
			store.ShardCount = 1
		}

		s.store = store
		s.api.FlushCallback = store.Flush

		store.Run(ctx, readiness)
	default:
		readiness <- fmt.Errorf("unknown backend: %v", s.Config.String("internal.store"))
	}
}

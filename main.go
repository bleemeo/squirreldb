package main

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/gocql/gocql"

	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
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
	"syscall"
	"time"
)

const (
	backendCassandra = "cassandra"
	backendDummy     = "dummy"
	backendBatcher   = "batcher"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

// variable set by GoReleaser
//nolint: gochecknoglobals
var (
	version string
	commit  string
	date    string
)

type lockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type metricReadWriter interface {
	types.MetricReader
	types.MetricWriter
}

// SquirrelDB is the SquirrelDB process itself. The Prometheus remote-store.
type SquirrelDB struct {
	Config                   *config.Config
	cassandraSession         *gocql.Session
	cassandraKeyspaceCreated bool
	lockFactory              lockFactory
	states                   types.State
	temporaryStore           batch.TemporaryStore
	index                    types.Index
	persistentStore          metricReadWriter
	store                    metricReadWriter
	api                      api.API
	cluster                  types.Cluster
}

type namedTasks struct {
	Name string
	Task types.Task
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg, err := config.New()

	if err != nil {
		logger.Fatalf("Error: Can't load config: %v", err)
	}

	if cfg.Bool("help") {
		cfg.FlagSet.PrintDefaults()
		return
	}

	if cfg.Bool("version") {
		fmt.Println(version)
		return
	}

	if cfg.Bool("build-info") {
		fmt.Printf("Built at %s using %s from commit %s\n", date, runtime.Version(), commit)
		fmt.Printf("Version %s\n", version)

		return
	}

	if !cfg.Validate() {
		os.Exit(1)
	}

	logger.Printf("Starting SquirrelDB %s (commit %s)", version, commit)

	debug.Level = cfg.Int("log.level")
	squirreldb := &SquirrelDB{
		Config: cfg,
	}
	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	runTerminated := make(chan interface{})
	readiness := make(chan error)

	go func() {
		squirreldb.Run(ctx, readiness)
		close(runTerminated)
	}()

	running := true
	firstStop := true

	for running {
		select {
		case err := <-readiness:
			if err != nil {
				logger.Fatalf("SquirrelDB failed to start: %v", err)
			}

			logger.Println("SquirrelDB is ready")
		case <-runTerminated:
			debug.Print(2, logger, "All services have been successfully stopped")

			running = false
		case <-signalChan:
			if firstStop {
				logger.Println("Received stop signal, gracefully stopping SquirrelDB")
				cancel()

				firstStop = false
			} else {
				logger.Println("Forced stop")

				running = false
			}
		}
	}

	signal.Stop(signalChan)
	close(signalChan)

	debug.Print(1, logger, "SquirrelDB is stopped")
}

// SchemaLock return a lock to modify the Cassandra schema.
func (s *SquirrelDB) SchemaLock() types.TryLocker {
	return s.lockFactory.CreateLock("cassandra-schema", 10*time.Second)
}

func (s *SquirrelDB) apiTask(ctx context.Context, readiness chan error) {
	s.api.ListenAddress = s.Config.String("remote_storage.listen_address")
	s.api.Index = s.index
	s.api.Reader = s.store
	s.api.Writer = s.store
	s.api.PromQLMaxEvaluatedPoints = uint64(s.Config.Int64("promql.max_evaluated_points"))
	s.api.PromQLMaxEvaluatedSeries = uint32(s.Config.Int("promql.max_evaluated_series"))

	s.api.Run(ctx, readiness)
}

// Run start SquirrelDB.
func (s *SquirrelDB) Run(ctx context.Context, readiness chan error) {
	tasks := []namedTasks{
		{
			Name: "locks",
			Task: types.TaskFun(s.lockTask),
		},
		{
			Name: "states",
			Task: types.TaskFun(s.statesTask),
		},
		{
			Name: "index",
			Task: types.TaskFun(s.indexTask),
		},
		{
			Name: "persistent store",
			Task: types.TaskFun(s.tsdbTask),
		},
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
		}, backoff.WithContext(retry.NewExponentialBackOff(30*time.Second), ctx),
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

func (s *SquirrelDB) getCassandraSession() (*gocql.Session, error) {
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

func (s *SquirrelDB) indexTask(ctx context.Context, readiness chan error) {
	switch s.Config.String("internal.index") {
	case backendCassandra:
		session, err := s.getCassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		options := index.Options{
			DefaultTimeToLive: s.Config.Duration("cassandra.default_time_to_live"),
			IncludeID:         s.Config.Bool("index.include_id"),
			LockFactory:       s.lockFactory,
			States:            s.states,
			SchemaLock:        s.SchemaLock(),
		}

		index, err := index.New(session, options)
		if err != nil {
			readiness <- err
			return
		}

		s.index = index
		s.api.IndexVerifyCallback = index.Verify

		readiness <- nil

		index.Run(ctx)
	case backendDummy:
		logger.Println("Warning: Using dummy for index (only do this for testing)")

		s.index = &dummy.Index{
			StoreMetricIDInMemory: s.Config.Bool("internal.dummy_index_check_conflict"),
			FixedValue:            types.MetricID(s.Config.Int64("internal.dummy_index_fixed_id")),
		}
		readiness <- nil

		<-ctx.Done()
	default:
		readiness <- fmt.Errorf("unknown backend: %v", s.Config.String("internal.index"))
	}
}

func (s *SquirrelDB) lockTask(ctx context.Context, readiness chan error) {
	switch s.Config.String("internal.locks") {
	case backendCassandra:
		session, err := s.getCassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		factory, err := locks.New(session, s.cassandraKeyspaceCreated)
		if err != nil {
			readiness <- err
			return
		}

		s.lockFactory = factory
	case backendDummy:
		logger.Println("Warning: Using dummy lock factory (only work on single node)")

		s.lockFactory = &dummy.Locks{}
	default:
		err := fmt.Errorf("unknown backend: %v", s.Config.String("internal.locks"))
		readiness <- err

		return
	}

	readiness <- nil

	<-ctx.Done()
}

func (s *SquirrelDB) clusterTask(ctx context.Context, readiness chan error) {
	if s.Config.Bool("cluster.enabled") {
		session, err := s.getCassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		seeds := &seed.Cassandra{
			Session:    session,
			SchemaLock: s.SchemaLock(),
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

func (s *SquirrelDB) statesTask(ctx context.Context, readiness chan error) {
	switch s.Config.String("internal.states") {
	case backendCassandra:
		session, err := s.getCassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		states, err := states.New(session, s.SchemaLock())
		if err != nil {
			readiness <- err
			return
		}

		s.states = states
	case backendDummy:
		logger.Println("Warning: Cassandra is disabled for states. Using dummy states store (only in-memory and single-node)")

		s.states = &dummy.States{}
	default:
		readiness <- fmt.Errorf("unknown backend: %v", s.Config.String("internal.states"))
		return
	}

	if !s.Config.ValidateRemote(s.states) {
		os.Exit(1)
	}

	readiness <- nil

	<-ctx.Done()
}

func (s *SquirrelDB) tsdbTask(ctx context.Context, readiness chan error) {
	switch s.Config.String("internal.tsdb") {
	case backendCassandra:
		session, err := s.getCassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		options := tsdb.Options{
			DefaultTimeToLive:         s.Config.Duration("cassandra.default_time_to_live"),
			BatchSize:                 s.Config.Duration("batch.size"),
			RawPartitionSize:          s.Config.Duration("cassandra.partition_size.raw"),
			AggregatePartitionSize:    s.Config.Duration("cassandra.partition_size.aggregate"),
			AggregateResolution:       s.Config.Duration("cassandra.aggregate.resolution"),
			AggregateSize:             s.Config.Duration("cassandra.aggregate.size"),
			AggregateIntendedDuration: s.Config.Duration("cassandra.aggregate.intended_duration"),
			SchemaLock:                s.SchemaLock(),
		}

		tsdb, err := tsdb.New(session, options, s.index, s.lockFactory, s.states)
		if err != nil {
			readiness <- err
			return
		}

		s.persistentStore = tsdb
		s.api.PreAggregateCallback = tsdb.ForcePreAggregation
		readiness <- nil

		tsdb.Run(ctx)
	case backendDummy:
		logger.Println("Warning: Cassandra is disabled for TSDB. Using dummy states store that discard every write")

		s.persistentStore = &dummy.DiscardTSDB{}
		readiness <- nil

		<-ctx.Done()
	default:
		readiness <- fmt.Errorf("unknown backend: %v", s.Config.String("internal.tsdb"))
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
		session, err := s.getCassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		wal := &batch.WalBatcher{
			WalStore: &wal.Cassandra{
				ShardID:    1,
				Session:    session,
				SchemaLock: s.SchemaLock(),
			},
			PersitentStore: s.persistentStore,
		}

		s.store = wal
		s.api.FlushCallback = func() error { wal.Flush(); return nil }

		wal.Run(ctx, readiness)
	case "distributor", "distributor2discard":
		session, err := s.getCassandraSession()
		if err != nil {
			readiness <- err
			return
		}

		if s.cluster == nil {
			logger.Println("Cluster is disabled. Only one SquirrelDB should access Cassandra")
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
						SchemaLock: s.SchemaLock(),
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

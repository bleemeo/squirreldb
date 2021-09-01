// Package daemon contains startup function of SquirrelDB
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
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
	"squirreldb/config"
	"squirreldb/debug"
	"squirreldb/dummy"
	"squirreldb/dummy/temporarystore"
	"squirreldb/redis/client"
	"squirreldb/redis/cluster"
	redisTemporarystore "squirreldb/redis/temporarystore"
	"squirreldb/retry"
	"squirreldb/telemetry"
	"squirreldb/types"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

//nolint:gochecknoglobals
var (
	Version = "unset"
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
	Config          *config.Config
	ExistingCluster types.Cluster
	MetricRegistry  prometheus.Registerer

	cassandraSession         *gocql.Session
	lockFactory              LockFactory
	states                   types.State
	temporaryStore           batch.TemporaryStore
	index                    types.Index
	persistentStore          MetricReadWriter
	store                    MetricReadWriter
	api                      api.API
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
	cassandraKeyspaceCreated bool
}

//nolint:gochecknoglobals
var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

var errBadConfig = errors.New("configuration validation failed")

// Start will run SquirrelDB and Init()ializing it. It return when SquirrelDB
// is ready.
// On error, we can retry calling Start() which will resume starting SquirrelDB.
func (s *SquirrelDB) Start(ctx context.Context) error {
	err := s.Init()
	if err != nil {
		return err
	}

	_, err = s.Index(ctx, true)
	if err != nil {
		return err
	}

	_, err = s.TSDB(ctx, true)
	if err != nil {
		return err
	}

	err = s.Telemetry(ctx)
	if err != nil {
		return err
	}

	readiness := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		s.run(ctx, readiness)
	}()

	err = <-readiness
	if err != nil {
		s.cancel()
		s.cancel = nil
		s.wg.Wait()

		return err
	}

	return nil
}

func (s *SquirrelDB) Stop() {
	if s.cancel != nil {
		s.cancel()
	}

	s.wg.Wait()
}

// ListenPort return the port listenning on. Should not be used before Start().
// This is useful for tests that use port "0" to known the actual listenning port.
func (s *SquirrelDB) ListenPort() int {
	return s.api.ListenPort()
}

// Init initialize SquirrelDB Locks & State. It also validate configuration with cluster (need Cassandra access)
// Init could be retried.
func (s *SquirrelDB) Init() error {
	if s.MetricRegistry == nil {
		s.MetricRegistry = prometheus.DefaultRegisterer
	}

	if !s.Config.Validate() {
		return errBadConfig
	}

	return nil
}

func (s *SquirrelDB) Run(ctx context.Context) error {
	if err := s.Start(ctx); err != nil {
		return err
	}

	<-ctx.Done()
	s.Stop()

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

// Config return the configuration after validation.
//nolint:forbidigo // This function is allowed to use fmt.Print*
func Config() (cfg *config.Config, err error) {
	cfg, err = config.New()
	if err != nil {
		return nil, fmt.Errorf("error: Can't load config: %w", err)
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
		return cfg, errBadConfig
	}

	debug.Level = cfg.Int("log.level")

	return cfg, nil
}

// DropCassandraData delete the Cassandra keyspace. If forceNonTestKeyspace also drop if the
// keyspace is not "squirreldb_test".
// This method is intended for testing, where keyspace is overrided to squirreldb_test.
func (s *SquirrelDB) DropCassandraData(ctx context.Context, forceNonTestKeyspace bool) error {
	keyspace := s.Config.String("cassandra.keyspace")

	if keyspace != "squirreldb_test" && !forceNonTestKeyspace {
		return fmt.Errorf("refuse to drop keyspace %s without forceNonTestKeyspace", keyspace)
	}

	session, err := s.CassandraSessionNoKeyspace()
	if err != nil {
		return err
	}

	err = session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
	if err != nil {
		return fmt.Errorf("failed to drop keyspace: %w", err)
	}

	session.Close()

	return nil
}

// DropTemporaryStore delete the temporary store data. If forceNonTestKeyspace also drop if the
// namespace prefix is not "test:".
// This method is intended for testing, where namespace prefix is overrided to "test:".
// Currently it drop Redis keys that start with namespace prefix.
func (s *SquirrelDB) DropTemporaryStore(ctx context.Context, forceNonTestKeyspace bool) error {
	redisAddresses := s.Config.Strings("redis.addresses")
	prefix := s.Config.String("internal.redis_keyspace")

	if prefix != "test:" && !forceNonTestKeyspace {
		return fmt.Errorf("refuse to drop with prefix \"%s\" without forceNonTestKeyspace", prefix)
	}

	if len(redisAddresses) == 0 || redisAddresses[0] == "" {
		return nil
	}

	wrappedClient := &client.Client{
		Addresses: redisAddresses,
	}

	defer wrappedClient.Close()

	err := wrappedClient.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
		scan := client.Scan(ctx, 0, prefix+"*", 100)
		it := scan.Iterator()
		keys := make([]string, 0, 100)

		for {
			keys = keys[:0]

			for it.Next(ctx) {
				keys = append(keys, it.Val())

				if len(keys) >= 100 {
					break
				}
			}

			if len(keys) == 0 {
				break
			}

			pipeline := client.Pipeline()

			for _, k := range keys {
				pipeline.Del(ctx, k)
			}

			_, err := pipeline.Exec(ctx)
			if err != nil {
				return fmt.Errorf("del failed: %w", err)
			}
		}

		if err := it.Err(); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		return nil
	})

	return err
}

// SetTestEnvironment configure few environment variable used in testing.
// This method MUST be called before first usage of Config().
// It will set Cassandra & Redis to use alternative keyspace to avoid confict with
// normal SquirrelDB. It will only do it if not explicitly set.
func SetTestEnvironment() {
	if _, ok := os.LookupEnv("SQUIRRELDB_CASSANDRA_KEYSPACE"); !ok {
		// If not explicitly changed, use squirreldb_test as keyspace. We do
		// not want to touch real data
		os.Setenv("SQUIRRELDB_CASSANDRA_KEYSPACE", "squirreldb_test")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_INTERNAL_REDIS_KEYSPACE"); !ok {
		// If not explicitly changed, use test: as namespace. We do
		// not want to touch real data
		os.Setenv("SQUIRRELDB_INTERNAL_REDIS_KEYSPACE", "test:")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_REMOTE_STORAGE_LISTEN_ADDRESS"); !ok {
		// If not explicitly set, use a dynamic port.
		os.Setenv("SQUIRRELDB_REMOTE_STORAGE_LISTEN_ADDRESS", "127.0.0.1:0")
	}
}

// SchemaLock return a lock to modify the Cassandra schema.
func (s *SquirrelDB) SchemaLock() (types.TryLocker, error) {
	lockFactory, err := s.LockFactory()
	if err != nil {
		return nil, err
	}

	return lockFactory.CreateLock("cassandra-schema", 10*time.Second), nil
}

// CassandraSessionNoKeyspace return a Cassandra without keyspace selected.
func (s *SquirrelDB) CassandraSessionNoKeyspace() (*gocql.Session, error) {
	cluster := gocql.NewCluster(s.Config.Strings("cassandra.addresses")...)
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}

	return session, nil
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

			factory, err := locks.New(s.MetricRegistry, session, s.cassandraKeyspaceCreated)
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
			logger.Println(
				"Warning: Cassandra is disabled for states. Using dummy states store (only in-memory and single-node)",
			)

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
	s.api.MaxConcurrentRemoteRequests = s.Config.Int("remote_storage.max_concurrent_requests")
	s.api.MetricRegistry = s.MetricRegistry

	s.api.Run(ctx, readiness)
}

// run start SquirrelDB.
func (s *SquirrelDB) run(ctx context.Context, readiness chan error) {
	tasks := []namedTasks{
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

	s.api.Ready()

	readiness <- ctx.Err()

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

// Cluster return an types.Cluster. The returned cluster should be closed after use.
func (s *SquirrelDB) Cluster(ctx context.Context) (types.Cluster, error) {
	if s.ExistingCluster == nil {
		redisAddresses := s.Config.Strings("redis.addresses")
		if len(redisAddresses) > 0 && redisAddresses[0] != "" {
			c := &cluster.Cluster{
				Addresses:      redisAddresses,
				MetricRegistry: s.MetricRegistry,
				Keyspace:       s.Config.String("internal.redis_keyspace"),
			}

			err := c.Start(ctx)
			if err != nil {
				_ = c.Stop()

				return c, err
			}

			s.ExistingCluster = c
		} else {
			s.ExistingCluster = &dummy.LocalCluster{}
		}
	}

	return s.ExistingCluster, nil
}

// Index return an Index. If started is true the index is started.
func (s *SquirrelDB) Index(ctx context.Context, started bool) (types.Index, error) {
	if s.index == nil { //nolint:nestif
		switch s.Config.String("internal.index") {
		case backendCassandra:
			session, err := s.CassandraSession()
			if err != nil {
				return nil, err
			}

			states, err := s.States()
			if err != nil {
				return nil, err
			}

			schemaLock, err := s.SchemaLock()
			if err != nil {
				return nil, err
			}

			cluster, err := s.Cluster(ctx)
			if err != nil {
				return nil, err
			}

			options := index.Options{
				DefaultTimeToLive: s.Config.Duration("cassandra.default_time_to_live"),
				LockFactory:       s.lockFactory,
				States:            states,
				SchemaLock:        schemaLock,
				Cluster:           cluster,
			}

			index, err := index.New(ctx, s.MetricRegistry, session, options)
			if err != nil {
				return nil, err
			}

			s.index = index
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
		err := task.Start(ctx)
		if err != nil {
			return s.index, fmt.Errorf("start index task: %w", err)
		}
	}

	return s.index, nil
}

// TSDB return the metric persistent store. If started is true the tsdb is started.
func (s *SquirrelDB) TSDB(ctx context.Context, preAggregationStarted bool) (MetricReadWriter, error) {
	if s.persistentStore == nil { //nolint:nestif
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

			index, err := s.Index(ctx, false)
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
				AggregateIntendedDuration: s.Config.Duration("cassandra.aggregate.intended_duration"),
				SchemaLock:                schemaLock,
			}

			tsdb, err := tsdb.New(s.MetricRegistry, session, options, index, lockFactory, states)
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
		err := task.Start(ctx)
		if err != nil {
			return s.persistentStore, fmt.Errorf("start persitent store task: %w", err)
		}
	}

	return s.persistentStore, nil
}

func (s *SquirrelDB) Telemetry(ctx context.Context) error {
	if !s.Config.Bool("telemetry.enabled") {
		return nil
	}

	var clusterID string

	lock := s.lockFactory.CreateLock("create cluster id", 5*time.Second)
	if ok := lock.TryLock(ctx, 10*time.Second); !ok {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		return errors.New("newMetricLock is not acquired")
	}

	state, _ := s.States()
	stateBool, err := state.Read("cluster_id", &clusterID)

	if err != nil || !stateBool {
		clusterID = uuid.New().String()

		err := s.states.Write("cluster_id", clusterID)
		if err != nil {
			logger.Printf("Waring: unable to set cluster id for telemetry: %v", err)
		}
	}

	defer lock.Unlock()

	addFacts := map[string]string{
		"installation_format": s.Config.String("internal.installation.format"),
		"cluster_id":          clusterID,
		"version":             Version,
	}

	runOption := map[string]string{
		"filepath": s.Config.String("telemetry.id.path"),
		"url":      s.Config.String("telemetry.address"),
	}

	rand.Seed(time.Now().UnixNano())

	tlm := telemetry.New(addFacts, runOption)
	tlm.Start(ctx)

	return nil
}

func (s *SquirrelDB) temporaryStoreTask(ctx context.Context, readiness chan error) {
	switch s.Config.String("internal.temporary_store") {
	case "redis":
		redisAddresses := s.Config.Strings("redis.addresses")
		if len(redisAddresses) > 0 && redisAddresses[0] != "" {
			options := redisTemporarystore.Options{
				Addresses: redisAddresses,
				Keyspace:  s.Config.String("internal.redis_keyspace"),
			}

			tmp, err := redisTemporarystore.New(ctx, s.MetricRegistry, options)
			s.temporaryStore = tmp

			readiness <- err

			if err != nil {
				return
			}

			<-ctx.Done()
		} else {
			mem := temporarystore.New(s.MetricRegistry)
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

		batch := batch.New(s.MetricRegistry, squirrelBatchSize, s.temporaryStore, s.persistentStore, s.persistentStore)
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
	default:
		readiness <- fmt.Errorf("unknown backend: %v", s.Config.String("internal.store"))
	}
}

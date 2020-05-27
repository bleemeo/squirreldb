package main

import (
	"fmt"
	"runtime"

	"github.com/gocql/gocql"
	ledisConfig "github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/server"

	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"squirreldb/api"
	"squirreldb/badger"
	"squirreldb/batch"
	"squirreldb/cassandra/index"
	"squirreldb/cassandra/locks"
	"squirreldb/cassandra/session"
	"squirreldb/cassandra/states"
	"squirreldb/cassandra/tsdb"
	"squirreldb/cassandra/wal"
	"squirreldb/config"
	"squirreldb/debug"
	"squirreldb/dummy"
	"squirreldb/ledis"
	"squirreldb/memorystore"
	"squirreldb/olric"
	"squirreldb/redis"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
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

// SquirrelDB is the SquirrelDB process itself. The Prometheus remote-store
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

	tasks     []func(context.Context)
	finalizer []func()
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

	squirreldb.Init()
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	runTerminated := make(chan bool)

	go func() {
		squirreldb.Run(ctx)
		close(runTerminated)
	}()
	logger.Println("SquirrelDB is ready")

	running := true
	firstStop := true

	for running {
		select {
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

// SchemaLock return a lock to modify the Cassandra schema
func (s *SquirrelDB) SchemaLock() types.TryLocker {
	return s.lockFactory.CreateLock("cassandra-schema", 10*time.Second)
}

// Init initialize and open connection to 3rd party stores
func (s *SquirrelDB) Init() {
	s.api.ListenAddress = s.Config.String("remote_storage.listen_address")

	retry.Print(
		s.createLockFactory,
		retry.NewExponentialBackOff(30*time.Second), logger,
		"create locks",
	)

	retry.Print(
		s.createStates,
		retry.NewExponentialBackOff(30*time.Second), logger,
		"create states",
	)

	if !s.Config.ValidateRemote(s.states) {
		os.Exit(1)
	}

	retry.Print(
		s.createTemporaryStore,
		retry.NewExponentialBackOff(30*time.Second), logger,
		"create temporary store",
	)

	retry.Print(
		s.createIndex,
		retry.NewExponentialBackOff(30*time.Second), logger,
		"create index",
	)

	retry.Print(
		s.createTSDB,
		retry.NewExponentialBackOff(30*time.Second), logger,
		"create persistent TSDB",
	)

	retry.Print(
		s.createStore,
		retry.NewExponentialBackOff(30*time.Second), logger,
		"create store",
	)

	s.api.Index = s.index
	s.api.Reader = s.store
	s.api.Writer = s.store
}

// Run start SquirrelDB
func (s *SquirrelDB) Run(ctx context.Context) {
	var wg sync.WaitGroup

	s.tasks = append(s.tasks, s.api.Run)

	wg.Add(len(s.tasks))

	for _, runner := range s.tasks {
		runner := runner

		go func() {
			defer wg.Done()
			runner(ctx)
		}()
	}

	wg.Wait()

	for i := len(s.finalizer) - 1; i >= 0; i-- {
		s.finalizer[i]()
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
		s.finalizer = append(s.finalizer, session.Close)
	}

	return s.cassandraSession, nil
}

func (s *SquirrelDB) createIndex() error {
	switch s.Config.String("internal.index") {
	case backendCassandra:
		session, err := s.getCassandraSession()
		if err != nil {
			return err
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
			return err
		}

		s.index = index
		s.tasks = append(s.tasks, index.Run)
	case backendDummy:
		logger.Println("Warning: Using dummy for index (only do this for testing)")

		s.index = &dummy.Index{
			StoreMetricIDInMemory: s.Config.Bool("internal.dummy_index_check_conflict"),
			FixedValue:            types.MetricID(s.Config.Int64("internal.dummy_index_fixed_id")),
		}
	default:
		return fmt.Errorf("unknown backend: %v", s.Config.String("internal.index"))
	}

	return nil
}

func (s *SquirrelDB) createLockFactory() error {
	switch s.Config.String("internal.locks") {
	case backendCassandra:
		session, err := s.getCassandraSession()
		if err != nil {
			return err
		}

		factory, err := locks.New(session, s.cassandraKeyspaceCreated)
		if err != nil {
			return err
		}

		s.lockFactory = factory
	case backendDummy:
		logger.Println("Warning: Using dummy lock factory (only work on single node)")

		s.lockFactory = &dummy.Locks{}
	default:
		return fmt.Errorf("unknown backend: %v", s.Config.String("internal.locks"))
	}

	return nil
}

func (s *SquirrelDB) createStates() error {
	switch s.Config.String("internal.states") {
	case backendCassandra:
		session, err := s.getCassandraSession()
		if err != nil {
			return err
		}

		states, err := states.New(session, s.SchemaLock())
		if err != nil {
			return err
		}

		s.states = states
	case backendDummy:
		logger.Println("Warning: Cassandra is disabled for states. Using dummy states store (only in-memory and single-node)")

		s.states = &dummy.States{}
	default:
		return fmt.Errorf("unknown backend: %v", s.Config.String("internal.states"))
	}

	return nil
}

func (s *SquirrelDB) createTSDB() error {
	switch s.Config.String("internal.tsdb") {
	case backendCassandra:
		session, err := s.getCassandraSession()
		if err != nil {
			return err
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
			return err
		}

		s.tasks = append(s.tasks, tsdb.Run)
		s.persistentStore = tsdb
		s.api.PreAggregateCallback = tsdb.ForcePreAggregation
	case backendDummy:
		logger.Println("Warning: Cassandra is disabled for TSDB. Using dummy states store that discard every write")

		s.persistentStore = &dummy.DiscardTSDB{}
	default:
		return fmt.Errorf("unknown backend: %v", s.Config.String("internal.tsdb"))
	}

	return nil
}

func (s *SquirrelDB) createTemporaryStore() error {
	switch s.Config.String("internal.temporary_store") {
	case "redis":
		redisAddresses := s.Config.Strings("redis.addresses")
		if len(redisAddresses) > 0 && redisAddresses[0] != "" {
			options := redis.Options{
				Addresses: redisAddresses,
			}

			s.temporaryStore = redis.New(options)
		} else {
			mem := memorystore.New()
			s.temporaryStore = mem
			s.tasks = append(s.tasks, mem.Run)
		}
	case "ledis":
		mem := &ledis.Ledis{}
		mem.Init()
		s.temporaryStore = mem

		s.finalizer = append(s.finalizer, mem.Close)
	case "ledisServer":
		cfg := ledisConfig.NewConfigDefault()
		cfg.Addr = "127.0.0.1:6380"
		cfg.DataDir = "/tmp/ledis"

		app, err := server.NewApp(cfg)
		if err != nil {
			return err
		}

		waitChan := make(chan interface{})

		go func() {
			app.Run()
			close(waitChan)
		}()

		options := redis.Options{
			Addresses: []string{"localhost:6380"},
		}

		s.temporaryStore = redis.New(options)

		s.finalizer = append(s.finalizer, func() {
			app.Close()
			<-waitChan
		})
	default:
		return fmt.Errorf("unknown backend: %v", s.Config.String("internal.temporary_store"))
	}

	return nil
}

func (s *SquirrelDB) createStore() error {
	switch s.Config.String("internal.store") {
	case backendBatcher:
		squirrelBatchSize := s.Config.Duration("batch.size")

		batch := batch.New(squirrelBatchSize, s.temporaryStore, s.persistentStore, s.persistentStore)
		s.store = batch
		s.tasks = append(s.tasks, batch.Run)
		s.api.FlushCallback = batch.Flush
	case backendDummy:
		logger.Println("Warning: SquirrelDB is configured to discard every write")

		s.store = dummy.DiscardTSDB{}
	case "cassandraWal":
		wal := &wal.Cassandra{
			Session:    s.cassandraSession,
			SchemaLock: s.SchemaLock(),
		}
		err := wal.Init()

		if err != nil {
			return err
		}

		s.store = wal
		s.finalizer = append(s.finalizer, wal.Close)
	case "badgerWal":
		wal := &badger.Badger{}
		err := wal.Init()

		if err != nil {
			return err
		}

		s.store = wal
		s.finalizer = append(s.finalizer, wal.Close)
	case "olricWal":
		wal := &olric.Wal{}

		err := wal.Init()
		if err != nil {
			return err
		}

		s.store = wal
		s.finalizer = append(s.finalizer, wal.Close)
	default:
		return fmt.Errorf("unknown backend: %v", s.Config.String("internal.store"))
	}

	return nil
}

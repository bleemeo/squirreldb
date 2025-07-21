// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package daemon contains startup function of SquirrelDB
package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/bleemeo/squirreldb/api"
	"github.com/bleemeo/squirreldb/batch"
	"github.com/bleemeo/squirreldb/cassandra/connection"
	"github.com/bleemeo/squirreldb/cassandra/index"
	"github.com/bleemeo/squirreldb/cassandra/locks"
	"github.com/bleemeo/squirreldb/cassandra/mutable"
	"github.com/bleemeo/squirreldb/cassandra/states"
	"github.com/bleemeo/squirreldb/cassandra/tsdb"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/dummy/temporarystore"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/redis/client"
	"github.com/bleemeo/squirreldb/redis/cluster"
	redisTemporarystore "github.com/bleemeo/squirreldb/redis/temporarystore"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	ID              uuid.UUID
	Config          config.Config
	ExistingCluster types.Cluster
	MetricRegistry  prometheus.Registerer
	Logger          zerolog.Logger

	cassandraConnection      *connection.Connection
	lockFactory              LockFactory
	states                   types.State
	temporaryStore           batch.TemporaryStore
	index                    types.Index
	persistentStore          MetricReadWriter
	store                    MetricReadWriter
	api                      api.API
	mutableLabelProvider     mutable.LabelProvider
	mutableLabelProcessor    *mutable.LabelProcessor
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
	cassandraKeyspaceCreated bool
}

// Start will run SquirrelDB and Init()ializing it. It return when SquirrelDB
// is ready.
// On error, we can retry calling Start() which will resume starting SquirrelDB.
func (s *SquirrelDB) Start(ctx context.Context) error {
	if s.Config.Internal.ReadOnly {
		s.Logger.Warn().Msg("internal.read_only is enabled. All writes will fail.")

		if s.Config.Telemetry.Enabled {
			s.Logger.Debug().Msg("Disable telemetry since read-only mode is activated")
			s.Config.Telemetry.Enabled = false
		}
	}

	if s.Config.Internal.DisablePreAggregation {
		s.Logger.Warn().Msg("internal.disable_pre_aggregation is enabled, pre-aggregated data won't be calculated")
	}

	err := s.Init()
	if err != nil {
		return err
	}

	_, err = s.Index(ctx, false, nil)
	if err != nil {
		return err
	}

	_, err = s.TSDB(ctx)
	if err != nil {
		return err
	}

	err = s.Telemetry(ctx)
	if err != nil {
		return err
	}

	readiness := make(chan error)

	ctx, cancel := context.WithCancel(context.Background()) //nolint: contextcheck
	s.cancel = cancel

	s.wg.Add(1)

	go func() {
		defer logger.ProcessPanic()
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

	if s.cassandraConnection != nil {
		s.cassandraConnection.Close()
	}
}

// ListenPort return the port listening on. Should not be used before Start().
// This is useful for tests that use port "0" to known the actual listenning port.
func (s *SquirrelDB) ListenPort() int {
	return s.api.ListenPort()
}

// Init initialize SquirrelDB Locks & State.
// Init could be retried.
func (s *SquirrelDB) Init() error {
	s.ID = uuid.New()

	if s.MetricRegistry == nil {
		s.MetricRegistry = prometheus.DefaultRegisterer
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
		defer logger.ProcessPanic()

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
				log.Info().Msg("Received stop signal, start graceful shutdown")

				cancel()

				firstStop = false
			} else {
				return errors.New("forced shutdown")
			}
		}
	}
}

// Config return the configuration after validation.
//
//nolint:forbidigo // This function is allowed to use fmt.Print*
func Config() (config.Config, error, error) {
	flags, err := config.ParseFlags()
	if err != nil {
		return config.Config{}, nil, err
	}

	if showHelp, _ := flags.GetBool("help"); showHelp {
		flags.PrintDefaults()
		os.Exit(0)
	}

	if showVersion, _ := flags.GetBool("version"); showVersion {
		fmt.Printf("Version %s\n", Version)
		os.Exit(0)
	}

	if showBuildInfo, _ := flags.GetBool("build-info"); showBuildInfo {
		fmt.Printf("Built at %s using %s from commit %s\n", Date, runtime.Version(), Commit)
		fmt.Printf("Version %s\n", Version)

		os.Exit(0)
	}

	configFiles, _ := flags.GetStringSlice("config")

	cfg, warnings, err := config.Load(true, configFiles...)
	if err != nil {
		return config.Config{}, nil, fmt.Errorf("can't load config: %w", err)
	}

	if err := validateConfig(cfg); err != nil {
		return cfg, nil, err
	}

	return cfg, warnings, nil
}

// validateConfig checks if the configuration is valid and consistent.
func validateConfig(cfg config.Config) error {
	var warnings prometheus.MultiError

	if cfg.Cassandra.Keyspace == "" {
		warnings.Append(fmt.Errorf("%w: 'cassandra.keyspace' must be set", config.ErrInvalidValue))
	}

	if cfg.Cassandra.ReplicationFactor <= 0 {
		err := fmt.Errorf("%w: 'cassandra.replication_factor' must be strictly greater than 0", config.ErrInvalidValue)
		warnings.Append(err)
	}

	if cfg.Batch.Size <= 0 {
		warnings.Append(fmt.Errorf("%w: 'batch.size' must be strictly greater than 0", config.ErrInvalidValue))
	}

	if cfg.Cassandra.Aggregate.IntendedDuration <= 0 {
		err := fmt.Errorf(
			"%w: 'cassandra.aggregate.intended_duration' must be strictly greater than 0",
			config.ErrInvalidValue,
		)
		warnings.Append(err)
	}

	return warnings.MaybeUnwrap()
}

// DropCassandraData delete the Cassandra keyspace. If forceNonTestKeyspace also drop if the
// keyspace is not "squirreldb_test".
// This method is intended for testing, where keyspace is overrided to squirreldb_test.
func (s *SquirrelDB) DropCassandraData(ctx context.Context, forceNonTestKeyspace bool) error {
	if s.Config.Cassandra.Keyspace != "squirreldb_test" && !forceNonTestKeyspace {
		return fmt.Errorf("refuse to drop keyspace %s without forceNonTestKeyspace", s.Config.Cassandra.Keyspace)
	}

	session, err := s.CassandraSessionNoKeyspace()
	if err != nil {
		return err
	}

	err = session.Query("DROP KEYSPACE IF EXISTS " + s.Config.Cassandra.Keyspace).WithContext(ctx).Exec()
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
	prefix := s.Config.Redis.Keyspace

	if prefix != "test:" && !forceNonTestKeyspace {
		return fmt.Errorf("refuse to drop with prefix \"%s\" without forceNonTestKeyspace", prefix)
	}

	if len(s.Config.Redis.Addresses) == 0 || s.Config.Redis.Addresses[0] == "" {
		return nil
	}

	wrappedClient := client.New(s.Config.Redis)

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
		_ = os.Setenv("SQUIRRELDB_CASSANDRA_KEYSPACE", "squirreldb_test")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_REDIS_KEYSPACE"); !ok {
		// If not explicitly changed, use test: as namespace. We do
		// not want to touch real data
		_ = os.Setenv("SQUIRRELDB_REDIS_KEYSPACE", "test:")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_LISTEN_ADDRESS"); !ok {
		// If not explicitly set, use a dynamic port.
		_ = os.Setenv("SQUIRRELDB_LISTEN_ADDRESS", "127.0.0.1:0")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_TELEMETRY_ENABLED"); !ok {
		// If not explicitly set, disable telemetry.
		_ = os.Setenv("SQUIRRELDB_TELEMETRY_ENABLED", "false")
	}

	// Initialize the logger temporarily before loading the config.
	log.Logger = logger.NewLogger(logger.NewConsoleWriter(false), zerolog.TraceLevel)

	cfg, _, _ := Config()

	log.Logger = logger.NewTestLogger(cfg.Log.DisableColor)
}

// SchemaLock return a lock to modify the Cassandra schema.
func (s *SquirrelDB) SchemaLock(ctx context.Context) (types.TryLocker, error) {
	lockFactory, err := s.LockFactory(ctx)
	if err != nil {
		return nil, err
	}

	return lockFactory.CreateLock("cassandra-schema", 10*time.Second), nil
}

// CassandraSessionNoKeyspace return a Cassandra without keyspace selected.
func (s *SquirrelDB) CassandraSessionNoKeyspace() (*gocql.Session, error) {
	cluster := gocql.NewCluster(s.Config.Cassandra.Addresses...)
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}

	return session, nil
}

// CassandraConnection return the Cassandra connection used for SquirrelDB.
func (s *SquirrelDB) CassandraConnection(ctx context.Context) (*connection.Connection, error) {
	if s.cassandraConnection == nil {
		session, keyspaceCreated, err := connection.New(
			ctx,
			s.Config.Cassandra,
			s.Config.Internal.ReadOnly,
			s.Logger.With().Str("component", "connection").Logger(),
		)
		if err != nil {
			return nil, err
		}

		s.cassandraConnection = session
		s.cassandraKeyspaceCreated = keyspaceCreated
	}

	return s.cassandraConnection, nil
}

// LockFactory return the Lock factory of SquirrelDB.
func (s *SquirrelDB) LockFactory(ctx context.Context) (LockFactory, error) {
	if s.lockFactory == nil {
		switch s.Config.Internal.Locks {
		case backendCassandra:
			connection, err := s.CassandraConnection(ctx)
			if err != nil {
				return nil, err
			}

			factory, err := locks.New(
				ctx,
				s.MetricRegistry,
				connection,
				s.cassandraKeyspaceCreated,
				s.Config.Internal.ReadOnly,
				s.Logger.With().Str("component", "locks").Logger(),
			)
			if err != nil {
				return nil, err
			}

			s.lockFactory = factory
		case backendDummy:
			s.Logger.Warn().Msg("Using dummy lock factory (only work on single node)")

			s.lockFactory = &dummy.Locks{}
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.Internal.Locks)
		}
	}

	return s.lockFactory, nil
}

func (s *SquirrelDB) States(ctx context.Context) (types.State, error) {
	if s.states == nil {
		switch s.Config.Internal.States {
		case backendCassandra:
			connection, err := s.CassandraConnection(ctx)
			if err != nil {
				return nil, err
			}

			lock, err := s.SchemaLock(ctx)
			if err != nil {
				return nil, err
			}

			states, err := states.New(ctx, states.Options{
				Connection: connection,
				Lock:       lock,
				ReadOnly:   s.Config.Internal.ReadOnly,
				Logger:     s.Logger.With().Str("component", "states").Logger(),
			})
			if err != nil {
				return nil, err
			}

			s.states = states
		case backendDummy:
			s.Logger.Warn().Msg(
				"Cassandra is disabled for states. Using dummy states store (only in-memory and single-node)",
			)

			s.states = &dummy.States{}
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.Internal.States)
		}
	}

	return s.states, nil
}

type namedTasks struct {
	Task types.OldTask
	Name string
}

func (s *SquirrelDB) apiTask(ctx context.Context, readiness chan error) {
	s.api.ListenAddress = s.Config.ListenAddress
	s.api.ReadOnly = s.Config.Internal.ReadOnly
	s.api.Index = s.index
	s.api.Reader = s.store
	s.api.Writer = s.store
	s.api.PromQLMaxEvaluatedPoints = uint64(s.Config.PromQL.MaxEvaluatedPoints) //nolint:gosec
	s.api.PromQLMaxEvaluatedSeries = uint32(s.Config.PromQL.MaxEvaluatedSeries) //nolint:gosec
	s.api.MaxConcurrentRemoteRequests = s.Config.RemoteStorage.MaxConcurrentRequests
	s.api.MaxRequestBodySizeBytes = s.Config.MaxRequestBodySize << 20 // MiB to B
	s.api.TenantLabelName = s.Config.TenantLabelName
	s.api.MutableLabelDetector = s.mutableLabelProcessor
	s.api.RequireTenantHeader = s.Config.RequireTenantHeader
	s.api.UseThanosPromQLEngine = s.Config.Internal.UseThanosPromQLEngine
	s.api.MetricRegistry = s.MetricRegistry
	s.api.Logger = s.Logger.With().Str("component", "api").Logger()

	s.api.FuturePointsBackdateOffset = s.Config.MaxAllowedTimeInFuture

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

	var err error

	ctxs := make([]context.Context, len(tasks))
	cancels := make([]context.CancelFunc, len(tasks))
	waitChan := make([]chan any, len(tasks))

	for i, task := range tasks {
		subReadiness := make(chan error)

		ctxs[i], cancels[i] = context.WithCancel(context.Background()) //nolint:fatcontext

		waitChan[i] = make(chan any)

		go func() { //nolint:contextcheck
			defer logger.ProcessPanic()

			task.Task.Run(ctxs[i], subReadiness)
			close(waitChan[i])
		}()

		err = <-subReadiness
		if err != nil {
			break
		}

		if ctx.Err() == nil {
			s.Logger.Trace().Msgf("Task %s started", task.Name)
		} else {
			err = ctx.Err()

			break
		}
	}

	if err == nil {
		s.api.Ready()
	}

	readiness <- err

	<-ctx.Done()

	for i := len(tasks) - 1; i >= 0; i-- {
		if cancels[i] == nil {
			continue
		}

		cancels[i]()
		<-waitChan[i]
		s.Logger.Trace().Msgf("Task %s stopped", tasks[i].Name)
	}

	if s.cassandraConnection != nil {
		s.cassandraConnection.Close()
	}
}

// Cluster return an types.Cluster. The returned cluster should be closed after use.
func (s *SquirrelDB) Cluster(ctx context.Context) (types.Cluster, error) {
	if s.ExistingCluster == nil {
		if len(s.Config.Redis.Addresses) > 0 && s.Config.Redis.Addresses[0] != "" {
			c := &cluster.Cluster{
				ID:             s.ID,
				RedisOptions:   s.Config.Redis,
				MetricRegistry: s.MetricRegistry,
				Keyspace:       s.Config.Redis.Keyspace,
				Logger:         s.Logger.With().Str("component", "cluster").Logger(),
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

// Index return an Index.
// If manualRunOnce is true, the index should only be used by tests and the test should call InternalRunOnce itself.
// If fakeTime is not nil, the index should only be used by tests and this allow to simulate longer time range.
func (s *SquirrelDB) Index(ctx context.Context, manualRunOnce bool, fakeTime func() time.Time) (types.Index, error) {
	if s.index == nil { //nolint:nestif
		var wrappedIndex types.Index

		switch s.Config.Internal.Index {
		case backendCassandra:
			connection, err := s.CassandraConnection(ctx)
			if err != nil {
				return nil, err
			}

			states, err := s.States(ctx)
			if err != nil {
				return nil, err
			}

			schemaLock, err := s.SchemaLock(ctx)
			if err != nil {
				return nil, err
			}

			cluster, err := s.Cluster(ctx)
			if err != nil {
				return nil, err
			}

			options := index.Options{
				DefaultTimeToLive:      s.Config.Cassandra.DefaultTimeToLive,
				PreCreateShardDuration: s.Config.Cassandra.PreCreateShardDuration,
				PreCreateShardFraction: s.Config.Cassandra.PreCreateShardFraction,
				TenantLabelName:        s.Config.TenantLabelName,
				LockFactory:            s.lockFactory,
				States:                 states,
				SchemaLock:             schemaLock,
				Cluster:                cluster,
				ReadOnly:               s.Config.Internal.ReadOnly,
				Logger:                 s.Logger.With().Str("component", "index").Logger(),
				InternalRunOnceCalled:  manualRunOnce,
				InternalNowFunction:    fakeTime,
			}

			wrappedIndex, err = index.New(
				ctx,
				s.MetricRegistry,
				connection,
				options,
				s.Logger.With().Str("component", "index").Logger(),
			)
			if err != nil {
				return nil, err
			}
		case backendDummy:
			s.Logger.Warn().Msg("Using dummy for index (only do this for testing)")

			wrappedIndex = &dummy.Index{
				StoreMetricIDInMemory: s.Config.Internal.IndexDummyCheckConflict,
				FixedValue:            types.MetricID(s.Config.Internal.IndexDummyFixedID),
			}
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.Internal.Index)
		}

		mutableLabelProcessor, err := s.MutableLabelProcessor(ctx)
		if err != nil {
			return nil, err
		}

		indexWrapper := mutable.NewIndexWrapper(
			wrappedIndex,
			mutableLabelProcessor,
			s.Logger.With().Str("component", "index_wrapper").Logger(),
		)

		s.index = indexWrapper

		if task, ok := s.index.(types.Task); ok {
			err := task.Start(ctx)
			if err != nil {
				return s.index, fmt.Errorf("start index task: %w", err)
			}
		}
	}

	return s.index, nil
}

// TSDB return the metric persistent store. If started is true the tsdb is started.
func (s *SquirrelDB) TSDB(ctx context.Context) (MetricReadWriter, error) {
	if s.persistentStore == nil { //nolint:nestif
		switch s.Config.Internal.TSDB {
		case backendCassandra:
			connection, err := s.CassandraConnection(ctx)
			if err != nil {
				return nil, err
			}

			schemaLock, err := s.SchemaLock(ctx)
			if err != nil {
				return nil, err
			}

			index, err := s.Index(ctx, false, nil)
			if err != nil {
				return nil, err
			}

			lockFactory, err := s.LockFactory(ctx)
			if err != nil {
				return nil, err
			}

			states, err := s.States(ctx)
			if err != nil {
				return nil, err
			}

			options := tsdb.Options{
				DefaultTimeToLive:         s.Config.Cassandra.DefaultTimeToLive,
				AggregateIntendedDuration: s.getAggregateIntendedDuration,
				SchemaLock:                schemaLock,
				ReadOnly:                  s.Config.Internal.ReadOnly,
				DisablePreAggregation:     s.Config.Internal.DisablePreAggregation,
			}

			tsdb, err := tsdb.New(
				ctx,
				s.MetricRegistry,
				connection,
				options,
				index,
				lockFactory,
				states,
				s.Logger.With().Str("component", "tsdb").Logger(),
			)
			if err != nil {
				return nil, err
			}

			s.persistentStore = tsdb
			s.api.PreAggregateCallback = tsdb.ForcePreAggregation
		case backendDummy:
			s.Logger.Warn().Msg("Cassandra is disabled for TSDB. Using dummy states store that discard every write")

			s.persistentStore = &dummy.DiscardTSDB{}
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.Internal.TSDB)
		}
	}

	if task, ok := s.persistentStore.(types.Task); ok {
		err := task.Start(ctx)
		if err != nil {
			return s.persistentStore, fmt.Errorf("start persitent store task: %w", err)
		}
	}

	return s.persistentStore, nil
}

func (s *SquirrelDB) Telemetry(ctx context.Context) error {
	if !s.Config.Telemetry.Enabled {
		return nil
	}

	state, _ := s.States(ctx)
	tlm := telemetry.New(
		telemetry.Options{
			URL:                s.Config.Telemetry.Address,
			Version:            Version,
			InstallationFormat: s.Config.Internal.Installation.Format,
			ClusterSizeFn:      s.ExistingCluster.Size,
			LockFactory:        s.lockFactory,
			State:              state,
			Logger:             s.Logger.With().Str("component", "telemetry").Logger(),
		},
	)
	tlm.Start(ctx)

	return nil
}

func (s *SquirrelDB) MutableLabelProcessor(ctx context.Context) (*mutable.LabelProcessor, error) {
	if s.mutableLabelProcessor == nil {
		labelProvider, err := s.MutableLabelProvider(ctx)
		if err != nil {
			return nil, err
		}

		tenantLabelName := s.Config.TenantLabelName
		labelProcessor := mutable.NewLabelProcessor(labelProvider, tenantLabelName)

		s.mutableLabelProcessor = labelProcessor
	}

	return s.mutableLabelProcessor, nil
}

func (s *SquirrelDB) MutableLabelProvider(ctx context.Context) (mutable.LabelProvider, error) {
	if s.mutableLabelProvider == nil {
		var store mutable.Store

		switch s.Config.Internal.MutableLabelsProvider {
		case backendCassandra:
			connection, err := s.CassandraConnection(ctx)
			if err != nil {
				return nil, err
			}

			store, err = mutable.NewCassandraStore(ctx, mutable.Options{
				Connection: connection,
				ReadOnly:   s.Config.Internal.ReadOnly,
				Logger:     s.Logger.With().Str("component", "mutable-labels").Logger(),
			})
			if err != nil {
				return nil, err
			}
		case backendDummy:
			s.Logger.Warn().Msg("Cassandra is disabled for mutable labels. Using dummy store that returns no label.")

			store = dummy.NewMutableLabelStore(dummy.MutableLabels{})
		default:
			return nil, fmt.Errorf("unknown backend: %v", s.Config.Internal.MutableLabelsProvider)
		}

		cluster, err := s.Cluster(ctx)
		if err != nil {
			return nil, err
		}

		logger := s.Logger.With().Str("component", "label_provider").Logger()
		labelProvider := mutable.NewProvider(ctx, s.MetricRegistry, cluster, store, logger)

		s.mutableLabelProvider = labelProvider
		s.api.MutableLabelWriter = labelProvider
	}

	return s.mutableLabelProvider, nil
}

func (s *SquirrelDB) temporaryStoreTask(ctx context.Context, readiness chan error) {
	switch s.Config.Internal.TemporaryStore {
	case "redis":
		if len(s.Config.Redis.Addresses) > 0 && s.Config.Redis.Addresses[0] != "" {
			options := redisTemporarystore.Options{
				RedisOptions: s.Config.Redis,
				Keyspace:     s.Config.Redis.Keyspace,
			}

			tmp, err := redisTemporarystore.New(
				ctx,
				s.MetricRegistry,
				options,
				s.Logger.With().Str("component", "temporary_store").Logger(),
			)
			s.temporaryStore = tmp

			readiness <- err

			if err != nil {
				return
			}

			<-ctx.Done()
		} else {
			mem := temporarystore.New(s.MetricRegistry, s.Logger.With().Str("component", "temporary_store").Logger())
			s.temporaryStore = mem

			readiness <- nil

			mem.Run(ctx)
		}
	default:
		readiness <- fmt.Errorf("unknown backend: %v", s.Config.Internal.TemporaryStore)
	}
}

func (s *SquirrelDB) batchStoreTask(ctx context.Context, readiness chan error) { //nolint: contextcheck
	switch s.Config.Internal.Store {
	case backendBatcher:
		var wg sync.WaitGroup

		subCtx, cancel := context.WithCancel(context.Background())
		subReady := make(chan error)

		wg.Add(1)

		go func() {
			defer logger.ProcessPanic()
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

		batch := batch.New(
			s.MetricRegistry,
			s.Config.Batch.Size,
			s.temporaryStore,
			s.persistentStore,
			s.persistentStore,
			s.Logger.With().Str("component", "batch").Logger(),
		)
		s.store = batch
		s.api.FlushCallback = batch.Flush

		readiness <- nil

		batch.Run(ctx)
		cancel()
		wg.Wait()
	case backendDummy:
		s.Logger.Warn().Msg("SquirrelDB is configured to discard every write")

		s.store = dummy.DiscardTSDB{}

		readiness <- nil

		<-ctx.Done()
	default:
		readiness <- fmt.Errorf("unknown backend: %v", s.Config.Internal.Store)
	}
}

// getAggregateIntendedDuration returns the duration the aggregation should last,
// depending on the size of the SquirrelDB cluster.
func (s *SquirrelDB) getAggregateIntendedDuration() time.Duration {
	// The largest the cluster, the slower we can run pre-aggregation.
	return s.Config.Cassandra.Aggregate.IntendedDuration * time.Duration(s.ExistingCluster.Size())
}

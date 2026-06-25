// Copyright 2015-2026 Bleemeo
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
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/bleemeo/squirreldb/api"
	cassandrabackend "github.com/bleemeo/squirreldb/cassandra/backend"
	clickhousebackend "github.com/bleemeo/squirreldb/clickhouse/backend"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/redis/client"
	"github.com/bleemeo/squirreldb/redis/cluster"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"

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
	backendCassandra  = "cassandra"
	backendClickHouse = "clickhouse"
)

type LockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

// ErrSkipBackend is returned by a test tool's run function when the configured
// backend does not support what the tool exercises (e.g. the lock benchmark or
// the Cassandra-index benchmarks on a ClickHouse backend). Tools treat it as a
// clean skip — no fatal error, minimal output — so run-tests.sh can launch every
// tool against any backend and let unsupported ones skip themselves.
var ErrSkipBackend = errors.New("test skipped: not supported by the configured backend")

// Backend is the uniform storage interface both Cassandra and ClickHouse satisfy.
// A single backend (selected by the "backend" config option) provides every
// store; mixing backends is not supported.
type Backend interface {
	Start(ctx context.Context) error
	TSDB(ctx context.Context) (types.TSDB, error)
	MutableProvider(ctx context.Context) (*mutable.Provider, error)
	MutableProcessor(ctx context.Context) (*mutable.LabelProcessor, error)
	TelemetryStore(ctx context.Context) (telemetry.Store, error)
	CloseAll(ctx context.Context)
}

// SquirrelDB is the SquirrelDB process itself. The Prometheus remote-store.
type SquirrelDB struct {
	ID              uuid.UUID
	Config          config.Config
	ExistingCluster types.Cluster
	MetricRegistry  prometheus.Registerer
	Logger          zerolog.Logger

	backend         Backend
	persistentStore types.TSDB
	api             api.API
	cancel          context.CancelFunc
	wg              sync.WaitGroup
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
		return fmt.Errorf("init fail: %w", err)
	}

	_, err = s.TSDB(ctx)
	if err != nil {
		return fmt.Errorf("TSDB start fail: %w", err)
	}

	err = s.Telemetry(ctx)
	if err != nil {
		return fmt.Errorf("Telemetry start fail: %w", err)
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

	if _, ok := os.LookupEnv("SQUIRRELDB_CLICKHOUSE_DATABASE"); !ok {
		// If not explicitly changed, use the test database (the only one DropData
		// will drop). We do not want to touch real data.
		_ = os.Setenv("SQUIRRELDB_CLICKHOUSE_DATABASE", clickhousebackend.TestDatabase)
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

// cassandraBackendOptions builds the Cassandra backend options from the config
// and the daemon-provided cluster. Index test hooks (DontStartIndex/
// InternalNowFunction) are set later via Backend.SetCassandraIndexTestingOption.
func (s *SquirrelDB) cassandraBackendOptions(cluster types.Cluster) cassandrabackend.Options {
	options := cassandrabackend.Options{
		CassandraConfig:           s.Config.Cassandra,
		ReadOnly:                  s.Config.Internal.ReadOnly,
		MetricRegistry:            s.MetricRegistry,
		Cluster:                   cluster,
		TenantLabel:               s.Config.TenantLabelName,
		DefaultTimeToLive:         s.Config.DefaultTimeToLive,
		AggregateIntendedDuration: s.getAggregateIntendedDuration,
		DisablePreAggregation:     s.Config.Internal.DisablePreAggregation,
		BatchSize:                 s.Config.Batch.Size,
		PreCreateShardDuration:    s.Config.Cassandra.PreCreateShardDuration,
		PreCreateShardFraction:    s.Config.Cassandra.PreCreateShardFraction,
	}

	if s.Config.Internal.TemporaryStore == "redis" {
		options.Redis = s.Config.Redis
	}

	return options
}

// clickHouseBackendOptions builds the ClickHouse backend options.
func (s *SquirrelDB) clickHouseBackendOptions(cluster types.Cluster) clickhousebackend.Options {
	options := clickhousebackend.Options{
		Config:            s.Config.ClickHouse,
		MetricRegistry:    s.MetricRegistry,
		ReadOnly:          s.Config.Internal.ReadOnly,
		Cluster:           cluster,
		TenantLabel:       s.Config.TenantLabelName,
		BatchSize:         s.Config.Batch.Size,
		DefaultTimeToLive: s.Config.DefaultTimeToLive,
	}

	if s.Config.Internal.TemporaryStore == "redis" {
		options.Redis = s.Config.Redis
	}

	return options
}

// Backend returns the (started, cached) storage backend. Test tools use it to
// reach backend-specific capabilities (e.g. a type-assert for the Cassandra lock
// factory or index test hooks); a tool needing a capability the configured
// backend doesn't provide can detect the failed assertion and skip itself.
func (s *SquirrelDB) Backend(ctx context.Context) (Backend, error) {
	return s.getBackend(ctx)
}

// getBackend returns the (started, cached) storage backend selected by the
// "backend" config option.
func (s *SquirrelDB) getBackend(ctx context.Context) (Backend, error) {
	if s.backend != nil {
		return s.backend, nil
	}

	cluster, err := s.Cluster(ctx)
	if err != nil {
		return nil, err
	}

	var b Backend

	switch s.Config.Backend {
	case backendCassandra:
		b, err = cassandrabackend.New(
			ctx, s.Logger.With().Str("backend", "cassandra").Logger(), s.cassandraBackendOptions(cluster))
	case backendClickHouse:
		b, err = clickhousebackend.New(
			ctx, s.Logger.With().Str("backend", "clickhouse").Logger(), s.clickHouseBackendOptions(cluster))
	default:
		return nil, fmt.Errorf("unknown backend: %v", s.Config.Backend)
	}

	if err != nil {
		return nil, err
	}

	if err := b.Start(ctx); err != nil {
		return nil, err
	}

	s.backend = b

	return s.backend, nil
}

// DropData drops the persisted data of the configured backend (Cassandra
// keyspace or ClickHouse tables). If force is set it also drops when the
// keyspace/database is not "squirreldb_test". It uses its own short-lived,
// unstarted backend so it can run before anything else.
// This method is intended for testing, where the keyspace/database is overridden
// to squirreldb_test.
func (s *SquirrelDB) DropData(ctx context.Context, force bool) error {
	switch s.Config.Backend {
	case backendCassandra:
		b, err := cassandrabackend.New(
			ctx,
			s.Logger.With().Str("backend", "cassandra").Logger(),
			cassandrabackend.Options{CassandraConfig: s.Config.Cassandra},
		)
		if err != nil {
			return err
		}

		return b.DropData(ctx, force)
	case backendClickHouse:
		b, err := clickhousebackend.New(
			ctx,
			s.Logger.With().Str("backend", "clickhouse").Logger(),
			clickhousebackend.Options{Config: s.Config.ClickHouse},
		)
		if err != nil {
			return err
		}

		return b.DropData(ctx, force)
	default:
		return fmt.Errorf("unknown backend: %v", s.Config.Backend)
	}
}

type namedTasks struct {
	Task types.OldTask
	Name string
}

func (s *SquirrelDB) apiTask(ctx context.Context, readiness chan error) {
	// Always go through the getter so the mutable label processor is initialized,
	// regardless of the TSDB backend (only the Cassandra TSDB would otherwise
	// trigger its creation as a side effect).
	mutableLabelProcessor, err := s.MutableLabelProcessor(ctx)
	if err != nil {
		readiness <- fmt.Errorf("mutable label processor start fail: %w", err)

		return
	}

	s.api.ListenAddress = s.Config.ListenAddress
	s.api.ReadOnly = s.Config.Internal.ReadOnly
	s.api.TSDB = s.persistentStore
	s.api.PromQLMaxEvaluatedPoints = uint64(s.Config.PromQL.MaxEvaluatedPoints) //nolint:gosec
	s.api.PromQLMaxEvaluatedSeries = uint32(s.Config.PromQL.MaxEvaluatedSeries) //nolint:gosec
	s.api.MaxConcurrentRemoteRequests = s.Config.RemoteStorage.MaxConcurrentRequests
	s.api.MaxRequestBodySizeBytes = s.Config.MaxRequestBodySize << 20 // MiB to B
	s.api.TenantLabelName = s.Config.TenantLabelName
	s.api.MutableLabelDetector = mutableLabelProcessor
	s.api.RequireTenantHeader = s.Config.RequireTenantHeader
	s.api.MetricRegistry = s.MetricRegistry
	s.api.Logger = s.Logger.With().Str("component", "api").Logger()
	s.api.DefaultTTL = s.Config.DefaultTimeToLive

	s.api.FuturePointsBackdateOffset = s.Config.MaxAllowedTimeInFuture

	s.api.Run(ctx, readiness)
}

// run start SquirrelDB.
func (s *SquirrelDB) run(ctx context.Context, readiness chan error) {
	tasks := []namedTasks{
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

		ctxs[i], cancels[i] = context.WithCancel(context.Background()) //nolint:fatcontext,gosec

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

	for i, task := range slices.Backward(tasks) {
		if cancels[i] == nil {
			continue
		}

		cancels[i]()
		<-waitChan[i]
		s.Logger.Trace().Msgf("Task %s stopped", task.Name)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// At this point, API is shutdown, so external writes are stopped. CloseAll
	// stops the backend's stores (the TSDB flushes its batch and stops
	// aggregation, telemetry stops its refresh loops) then closes the connection.
	if s.backend != nil {
		s.backend.CloseAll(stopCtx) //nolint:contextcheck
	}

	if err := s.ExistingCluster.Close(); err != nil {
		s.Logger.Error().Msgf("Failed to stop cluster: %s", err.Error())
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

// TSDB returns the metric persistent store. Cassandra-index test hooks
// (don't-start-index, fake time) are set beforehand via
// Backend().SetCassandraIndexTestingOption, not here.
func (s *SquirrelDB) TSDB(ctx context.Context) (types.TSDB, error) {
	if s.persistentStore != nil {
		return s.persistentStore, nil
	}

	b, err := s.getBackend(ctx)
	if err != nil {
		return nil, err
	}

	tsdb, err := b.TSDB(ctx)
	if err != nil {
		return nil, err
	}

	s.persistentStore = tsdb

	return s.persistentStore, nil
}

func (s *SquirrelDB) Telemetry(ctx context.Context) error {
	if !s.Config.Telemetry.Enabled {
		return nil
	}

	cluster, err := s.Cluster(ctx)
	if err != nil {
		return err
	}

	b, err := s.getBackend(ctx)
	if err != nil {
		return err
	}

	store, err := b.TelemetryStore(ctx)
	if err != nil {
		return err
	}

	tlm := telemetry.New(
		telemetry.Options{
			URL:                s.Config.Telemetry.Address,
			Version:            Version,
			InstallationFormat: s.Config.Internal.Installation.Format,
			ClusterSizeFn:      cluster.Size,
			Store:              store,
			Logger:             s.Logger.With().Str("component", "telemetry").Logger(),
		},
	)
	tlm.Start(ctx)

	return nil
}

// MutableLabelProcessor returns the mutable label processor from the backend. It
// also ensures the API mutable label writer is wired (via MutableLabelProvider).
func (s *SquirrelDB) MutableLabelProcessor(ctx context.Context) (*mutable.LabelProcessor, error) {
	if _, err := s.MutableLabelProvider(ctx); err != nil {
		return nil, err
	}

	b, err := s.getBackend(ctx)
	if err != nil {
		return nil, err
	}

	return b.MutableProcessor(ctx)
}

// MutableLabelProvider returns the mutable label provider from the backend and
// wires it as the API mutable label writer.
func (s *SquirrelDB) MutableLabelProvider(ctx context.Context) (*mutable.Provider, error) {
	b, err := s.getBackend(ctx)
	if err != nil {
		return nil, err
	}

	provider, err := b.MutableProvider(ctx)
	if err != nil {
		return nil, err
	}

	s.api.MutableLabelWriter = provider

	return provider, nil
}

// getAggregateIntendedDuration returns the duration the aggregation should last,
// depending on the size of the SquirrelDB cluster.
func (s *SquirrelDB) getAggregateIntendedDuration() time.Duration {
	// The largest the cluster, the slower we can run pre-aggregation.
	return s.Config.Cassandra.Aggregate.IntendedDuration * time.Duration(s.ExistingCluster.Size())
}

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
	goflag "flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"time"

	zlogsentry "github.com/archdx/zerolog-sentry"
	"github.com/bleemeo/squirreldb/api"
	cassandrabackend "github.com/bleemeo/squirreldb/cassandra/backend"
	clickhousebackend "github.com/bleemeo/squirreldb/clickhouse/backend"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	dummybackend "github.com/bleemeo/squirreldb/dummy/backend"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/redis/client"
	"github.com/bleemeo/squirreldb/redis/cluster"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"
	"github.com/spf13/pflag"

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
	backendDummy      = "dummy"
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

// ErrNoExtraWriteBackend is returned by MigrateToBackend when no
// extra_write_backend is configured.
var ErrNoExtraWriteBackend = errors.New("no extra_write_backend configured")

// ErrNormalExit is returned when process should exit normal (return code 0).
var ErrNormalExit = errors.New("normal exit")

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
	// RegisterHTTPEndpoints registers backend-specific debug endpoints. The API
	// prepends a per-backend prefix (/debug/<backend>/), so paths are relative;
	// description feeds the /debug/ index.
	RegisterHTTPEndpoints(register func(method, path, description string, handler http.HandlerFunc))
}

// SquirrelDB is the SquirrelDB process itself. The Prometheus remote-store.
type SquirrelDB struct {
	ID              uuid.UUID
	Config          config.Config
	ExistingCluster types.Cluster
	MetricRegistry  prometheus.Registerer
	Logger          zerolog.Logger
	// TelemetryStoreTiming overrides the telemetry store's ownership timing (lease
	// refresh for ClickHouse, per-ID lock for Cassandra). It is a test hook — set
	// directly by test tools, never from the config file — so a test can run the
	// node-ID dance in seconds. Zero fields keep the production defaults.
	TelemetryStoreTiming telemetry.StoreTiming
	// DropDataOnStartup makes the backend drop its (test) data when it starts,
	// before it (re)creates the schema. It is a test hook, set directly by test
	// tools and never from the config file, and only ever drops the test
	// keyspace/database. To reset a shared store, set it on a single instance and
	// start that instance before any other.
	DropDataOnStartup bool
	// PrimaryBackendReadOnly forces the primary backend (Backend()) read-only,
	// on top of config's internal.read_only. The migrate subcommand sets it so the
	// source is read-only (double safety: no writes to the store being migrated
	// away); MigrateToBackend (the write target) is unaffected.
	PrimaryBackendReadOnly bool
	// MigrateTargetImportOnly builds the dual-write target (MigrateToBackend) for
	// bulk import only: no WAL drainer. The migrate subcommand sets it so the target
	// does not join the drainer lease of the live daemon that owns the shared WAL
	// (and does not emit a shutdown "WAL not fully drained" warning about that WAL,
	// which it bypasses). The daemon's dual-write leaves it false — it wants the
	// drainer.
	MigrateTargetImportOnly bool

	backend         Backend
	persistentStore types.TSDB
	api             api.API
	cancel          context.CancelFunc
	wg              sync.WaitGroup

	// extraBackend is the best-effort dual-write target (MigrateToBackend); nil
	// unless configured. It is set by MigrateToBackend, which may run from the
	// background retry goroutine (see attachExtraWhenReady) concurrently with the
	// shutdown CloseAll, so it is guarded by extraBackendMu.
	extraBackendMu sync.Mutex
	extraBackend   Backend

	// extraPending is set in TSDB() when the dual-write extra backend could not be
	// started at startup; extraUnavailableSince stamps when primary-only began. The
	// retry goroutine spawned in Start() attaches the extra once it is reachable and
	// logs the [since, now] window to repair.
	extraPending          bool
	extraUnavailableSince time.Time
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

	// If the dual-write extra backend was unreachable at startup (see TSDB), keep
	// retrying it in the background on the daemon's long-lived context so it attaches
	// when it comes up — without blocking startup or the primary.
	if s.extraPending {
		s.wg.Add(1)

		go func() {
			defer logger.ProcessPanic()
			defer s.wg.Done()

			s.attachExtraWhenReady(ctx)
		}()
	}

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

func (s *SquirrelDB) Run(ctx context.Context, shutdownCtx context.Context) error {
	_ = shutdownCtx

	if err := s.Start(ctx); err != nil {
		return err
	}

	<-ctx.Done()
	s.Stop()

	return nil
}

// RunWithSignalHandler runs given function with two context:
//   - The first context is canceled on kill or ctrl+c
//   - The second context is canceled on a second kill or ctrl+c
//
// The function is allowed to perform closing action once first context expire
// (but it should be "quick") and the function should exit immediately when second
// context expire.
//
// In any case, on a third kill or ctrl+c, RunWithSignalHandler exit in any case.
func RunWithSignalHandler(f func(context.Context, context.Context) error) error {
	signalChan := make(chan os.Signal, 1)
	runTerminated := make(chan error, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	largeCtx, largeCancel := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancel(largeCtx)

	defer func() {
		signal.Stop(signalChan)
		close(signalChan)
		cancel()
		largeCancel()
	}()

	go func() {
		defer logger.ProcessPanic()

		runTerminated <- f(ctx, largeCtx)
	}()

	stopCount := 0

	for {
		select {
		case err := <-runTerminated:
			cancel()
			largeCancel()

			return err
		case <-signalChan:
			switch stopCount {
			case 0:
				log.Info().Msg("Received stop signal, start graceful shutdown")

				cancel()

				stopCount++
			case 1:
				log.Warn().Msg("Received 2 stop signals, request immediate shutdown")

				largeCancel()

				stopCount++
			default:
				return errors.New("forced shutdown")
			}
		}
	}
}

// SetupSentryLogger sets the default zerolog logger to a Sentry logger, or a simple
// console logger if Sentry is disabled. The closer returned should be closed at the
// end of the program to flush the events to Sentry.
func SetupSentryLogger(cfg config.Config) io.Closer {
	consoleWriter := logger.NewConsoleWriter(cfg.Log.DisableColor)
	logLevel := zerolog.Level(cfg.Log.Level) //nolint:gosec
	release := zlogsentry.WithRelease(fmt.Sprintf("squirreldb@%s-%s", Version, Commit))

	if cfg.Sentry.DSN == "" {
		log.Logger = logger.NewLogger(consoleWriter, logLevel)

		return nil
	}

	sentryWriter, err := logger.NewSentryWriter(cfg.Sentry.DSN, zerolog.ErrorLevel, release)
	if err != nil {
		log.Err(err).Msg("Failed to initialize sentry")

		log.Logger = logger.NewLogger(consoleWriter, logLevel)

		return nil
	}

	// Set up logger with level filter.
	multiWriter := zerolog.MultiLevelWriter(consoleWriter, sentryWriter)
	log.Logger = logger.NewLogger(multiWriter, logLevel)

	return sentryWriter
}

func CommonFlags() []config.Flag {
	return []config.Flag{
		{Name: "help", Short: "h", Value: false, Usage: "Display help"},
		{Name: "version", Short: "v", Value: false, Usage: "Show version and exit"},
		{Name: "build-info", Value: false, Usage: "Show build-info and exit"},
		{
			Name:  "config",
			Short: "c",
			Value: config.DefaultConfigPaths(),
			Usage: "Files and folders from which to load the configuration",
		},
	}
}

// LoadConfigFromFlags will parse flags and load the configuration.
// commandFlags is the sub-command specific flags (daemon, migrate, ...) and will
// be available in the returned FlagSet. Some common flags (see CommonFlags) are
// always added and processed; some of them stop the process normally (e.g.
// --version, --help, ...), in which case ErrNormalExit is returned.
// The two errors are warnings (from configuration loading) and error.
//
//nolint:forbidigo // This function is allowed to use fmt.Print* for CLI output.
func LoadConfigFromFlags(
	subcommandName string,
	commandFlags []config.Flag,
	args []string,
) (*pflag.FlagSet, config.Config, error, error) {
	allFlags := append(CommonFlags(), commandFlags...)
	allFlags = append(allFlags, config.ConfigFlags()...)
	flagSet := config.NewFlagSet("squirreldb "+subcommandName, allFlags)

	// Add "Go" flags that are used in tests.
	flagSet.AddGoFlagSet(goflag.CommandLine)

	if err := flagSet.Parse(args); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return flagSet, config.Config{}, nil, ErrNormalExit
		}

		return flagSet, config.Config{}, nil, err
	}

	// Process common flags
	if showHelp, _ := flagSet.GetBool("help"); showHelp {
		flagSet.PrintDefaults()

		return flagSet, config.Config{}, nil, ErrNormalExit
	}

	if showVersion, _ := flagSet.GetBool("version"); showVersion {
		fmt.Printf("Version %s\n", Version)

		return flagSet, config.Config{}, nil, ErrNormalExit
	}

	if showBuildInfo, _ := flagSet.GetBool("build-info"); showBuildInfo {
		fmt.Printf("Built at %s using %s from commit %s\n", Date, runtime.Version(), Commit)
		fmt.Printf("Version %s\n", Version)

		return flagSet, config.Config{}, nil, ErrNormalExit
	}

	configFiles, _ := flagSet.GetStringSlice("config")

	cfg, warnings, err := LoadConfig(configFiles)

	return flagSet, cfg, warnings, err
}

// Config parses the daemon command line from os.Args (used by the test tools),
// handling help/version/build-info, then loads and validates the configuration.
func Config() (config.Config, error, error) {
	_, cfg, warnings, err := LoadConfigFromFlags("daemon", daemonCommandFlags(), os.Args)
	if errors.Is(err, ErrNormalExit) {
		os.Exit(0)
	}

	return cfg, warnings, err
}

// LoadConfig loads the configuration from the given files (plus environment and
// command-line config overrides) and validates it. It is shared by the daemon
// and the migrate subcommand so both load config identically.
func LoadConfig(configFiles []string) (config.Config, config.Warnings, error) {
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

	if cfg.ExtraWriteBackend != "" {
		knownBackends := map[string]bool{backendCassandra: true, backendClickHouse: true, backendDummy: true}

		if !knownBackends[cfg.ExtraWriteBackend] {
			warnings.Append(fmt.Errorf(
				"%w: unknown backend %q in 'extra_write_backend'", config.ErrInvalidValue, cfg.ExtraWriteBackend,
			))
		}

		if cfg.ExtraWriteBackend == cfg.Backend {
			warnings.Append(fmt.Errorf(
				"%w: 'extra_write_backend' (%q) must differ from 'backend' "+
					"(which is already the primary, must-succeed write target)",
				config.ErrInvalidValue, cfg.ExtraWriteBackend,
			))
		}
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
func (s *SquirrelDB) cassandraBackendOptions(cluster types.Cluster, readOnly bool) cassandrabackend.Options {
	options := cassandrabackend.Options{
		CassandraConfig:           s.Config.Cassandra,
		ReadOnly:                  readOnly,
		MetricRegistry:            s.MetricRegistry,
		Cluster:                   cluster,
		TenantLabel:               s.Config.TenantLabelName,
		DefaultTimeToLive:         s.Config.DefaultTimeToLive,
		AggregateIntendedDuration: s.getAggregateIntendedDuration,
		DisablePreAggregation:     s.Config.Internal.DisablePreAggregation,
		BatchSize:                 s.Config.Batch.Size,
		PreCreateShardDuration:    s.Config.Cassandra.PreCreateShardDuration,
		PreCreateShardFraction:    s.Config.Cassandra.PreCreateShardFraction,
		TelemetryTiming:           s.TelemetryStoreTiming,
		DropDataOnStartup:         s.DropDataOnStartup,
	}

	if s.Config.Internal.TemporaryStore == "redis" {
		options.Redis = s.Config.Redis
	}

	return options
}

// clickHouseBackendOptions builds the ClickHouse backend options. importOnly builds
// the TSDB for bulk import only (no WAL drainer) — see MigrateTargetImportOnly.
func (s *SquirrelDB) clickHouseBackendOptions(
	cluster types.Cluster,
	readOnly bool,
	importOnly bool,
) clickhousebackend.Options {
	options := clickhousebackend.Options{
		Config:            s.Config.ClickHouse,
		MetricRegistry:    s.MetricRegistry,
		ReadOnly:          readOnly,
		Cluster:           cluster,
		TenantLabel:       s.Config.TenantLabelName,
		DefaultTimeToLive: s.Config.DefaultTimeToLive,
		ClusterName:       s.Config.ClickHouse.ClusterName,
		TelemetryTiming:   s.TelemetryStoreTiming,
		DropDataOnStartup: s.DropDataOnStartup,
		ImportOnly:        importOnly,
	}

	if s.Config.Internal.TemporaryStore == "redis" {
		options.Redis = s.Config.Redis
	}

	return options
}

// dummyBackendOptions builds the dummy backend options. The dummy backend
// discards all writes; the batcher (and its temporary store) are only wired when
// dummy.batcher is set.
func (s *SquirrelDB) dummyBackendOptions(cluster types.Cluster) dummybackend.Options {
	options := dummybackend.Options{
		MetricRegistry: s.MetricRegistry,
		Cluster:        cluster,
		TenantLabel:    s.Config.TenantLabelName,
		Batch:          s.Config.Batch,
		Batcher:        s.Config.Internal.Dummy.Batcher,
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

// getBackend returns the (started, cached) primary storage backend
// (Config.Backend) — the source of truth that serves reads, mutable labels and
// telemetry. The optional extra write backend is a separate instance
// (MigrateToBackend); the two are combined only at the TSDB level (see TSDB()).
func (s *SquirrelDB) getBackend(ctx context.Context) (Backend, error) {
	if s.backend != nil {
		return s.backend, nil
	}

	cluster, err := s.Cluster(ctx)
	if err != nil {
		return nil, err
	}

	b, err := s.newBackendByName(
		ctx, s.Config.Backend, cluster, s.Config.Internal.ReadOnly || s.PrimaryBackendReadOnly, false)
	if err != nil {
		return nil, err
	}

	if err := b.Start(ctx); err != nil {
		return nil, err
	}

	s.backend = b

	return s.backend, nil
}

// MigrateToBackend returns the (started, cached) extra write backend — the
// dual-write / migration target named by Config.ExtraWriteBackend. It is a plain
// backend (not best-effort in itself: best-effort is a per-appender choice made
// by the dual-write composite; the migrator writes to it reliably). Returns
// ErrNoExtraWriteBackend if none is configured.
func (s *SquirrelDB) MigrateToBackend(ctx context.Context) (Backend, error) {
	if s.Config.ExtraWriteBackend == "" {
		return nil, ErrNoExtraWriteBackend
	}

	s.extraBackendMu.Lock()
	cached := s.extraBackend
	s.extraBackendMu.Unlock()

	if cached != nil {
		return cached, nil
	}

	cluster, err := s.Cluster(ctx)
	if err != nil {
		return nil, err
	}

	// The migration target must be writable — it is never forced read-only, even
	// when the source (primary) is. It is import-only for the migrate subcommand
	// (no WAL drainer), but not for the daemon's dual-write, which owns the WAL.
	b, err := s.newBackendByName(
		ctx, s.Config.ExtraWriteBackend, cluster, s.Config.Internal.ReadOnly, s.MigrateTargetImportOnly)
	if err != nil {
		return nil, err
	}

	if err := b.Start(ctx); err != nil {
		return nil, err
	}

	s.extraBackendMu.Lock()
	s.extraBackend = b
	s.extraBackendMu.Unlock()

	return b, nil
}

// extraAttachRetryInterval is how often attachExtraWhenReady retries starting the
// dual-write extra backend after it was unavailable at startup.
const extraAttachRetryInterval = 30 * time.Second

// newBackendByName builds (unstarted) the concrete backend for a single name.
// readOnly makes the backend refuse writes (the migrate source uses it).
func (s *SquirrelDB) newBackendByName(
	ctx context.Context,
	name string,
	cluster types.Cluster,
	readOnly bool,
	importOnly bool,
) (Backend, error) {
	switch name {
	case backendCassandra:
		return cassandrabackend.New(
			ctx, s.Logger.With().Str("backend", "cassandra").Logger(), s.cassandraBackendOptions(cluster, readOnly))
	case backendClickHouse:
		return clickhousebackend.New(
			ctx, s.Logger.With().Str("backend", "clickhouse").Logger(),
			s.clickHouseBackendOptions(cluster, readOnly, importOnly))
	case backendDummy:
		return dummybackend.New(
			ctx, s.Logger.With().Str("backend", "dummy").Logger(), s.dummyBackendOptions(cluster))
	default:
		return nil, fmt.Errorf("unknown backend: %v", name)
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

	if backend, err := s.getBackend(ctx); err == nil {
		s.api.BackendName = s.Config.Backend
		s.api.BackendDebug = backend
	}

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

	// Read under the lock: the background retry goroutine (attachExtraWhenReady) may
	// still be setting extraBackend as shutdown runs.
	s.extraBackendMu.Lock()
	extra := s.extraBackend
	s.extraBackendMu.Unlock()

	if extra != nil {
		extra.CloseAll(stopCtx) //nolint:contextcheck
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

	primaryTSDB, err := b.TSDB(ctx)
	if err != nil {
		return nil, err
	}

	// No extra write backend → the primary TSDB is used directly.
	if s.Config.ExtraWriteBackend == "" {
		s.persistentStore = primaryTSDB

		return s.persistentStore, nil
	}

	// Dual-write: combine the primary (must-succeed) with the extra (best-effort)
	// at the TSDB level only. Reads still come from the primary.
	composite := newCompositeTSDB(primaryTSDB, nil, s.Logger)
	s.persistentStore = composite

	// The extra (dual-write target) is best-effort, including at startup: if it
	// cannot be started now — e.g. ClickHouse is unreachable — start primary-only
	// and attach it in the background once it becomes reachable, rather than failing
	// the whole daemon. Points written meanwhile are not dual-written and must be
	// repaired for that window (logged when the extra attaches). The retry goroutine
	// is spawned in Start(), which owns the daemon's long-lived context.
	extraTSDB, err := s.startExtraTSDB(ctx)
	if err != nil {
		s.Logger.Warn().Err(err).
			Msg("dual-write target unavailable at startup; starting primary-only and retrying in the background")

		s.extraPending = true
		s.extraUnavailableSince = time.Now()
	} else {
		composite.attachExtra(extraTSDB)
	}

	return s.persistentStore, nil
}

// startExtraTSDB starts the extra (dual-write target) backend and returns its
// TSDB. A failure (e.g. ClickHouse unreachable) leaves no started state to leak:
// MigrateToBackend does not cache on failure and routing.New cleans itself up.
func (s *SquirrelDB) startExtraTSDB(ctx context.Context) (types.TSDB, error) {
	extra, err := s.MigrateToBackend(ctx)
	if err != nil {
		return nil, err
	}

	return extra.TSDB(ctx)
}

// attachExtraWhenReady retries starting the dual-write extra backend until it
// succeeds (or ctx is cancelled), then attaches its TSDB to the composite so
// dual-write resumes without a restart. It logs the window during which the extra
// was unavailable — points written then were not dual-written and must be
// repaired over that range.
func (s *SquirrelDB) attachExtraWhenReady(ctx context.Context) {
	ticker := time.NewTicker(extraAttachRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		extraTSDB, err := s.startExtraTSDB(ctx)
		if err != nil {
			s.Logger.Debug().Err(err).Msg("dual-write target still unavailable, will retry")

			continue
		}

		composite, ok := s.persistentStore.(*compositeTSDB)
		if !ok {
			s.Logger.Error().Msg("cannot attach dual-write target: persistent store is not a composite")

			return
		}

		composite.attachExtra(extraTSDB)

		s.Logger.Warn().
			Str("repair_from", s.extraUnavailableSince.Format(time.RFC3339)).
			Str("repair_to", time.Now().Format(time.RFC3339)).
			Msg("dual-write target attached; points written while it was unavailable were not dual-written — " +
				"repair that window with `squirreldb migrate --repair-from <repair_from> --repair-to <repair_to>`")

		return
	}
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

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
	"slices"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/api"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/core"
	"github.com/bleemeo/squirreldb/core/version"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// ErrNoExtraWriteBackend is returned by MigrateToBackend when no
// extra_write_backend is configured.
var ErrNoExtraWriteBackend = errors.New("no extra_write_backend configured")

// SquirrelDB is the SquirrelDB process itself. The Prometheus remote-store.
type SquirrelDB struct {
	Config         config.Config
	MetricRegistry prometheus.Registerer
	Logger         zerolog.Logger
	// DropDataOnStartup makes the backend drop its (test) data when it starts,
	// before it (re)creates the schema. It is a test hook, set directly by test
	// tools and never from the config file, and only ever drops the test
	// keyspace/database. To reset a shared store, set it on a single instance and
	// start that instance before any other.
	DropDataOnStartup bool

	// squirreldb owns the started primary and optional extra (dual-write) backends and
	// composes the data-plane accessors (read/write/dual-write). It replaces the
	// former per-TSDB cache: the backends memoize their own TSDB, so nothing here
	// caches a TSDB() result.
	squirrelDBOnce sync.Once
	squirreldb     *core.SquirrelDB

	api    api.API
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// getSquirrelDB lazily builds the core.SquirrelDB facade from the daemon's config.
// The build closures own how a backend is constructed (name, read-only,
// import-only); core.SquirrelDB owns the lifecycle and the composition.
func (s *SquirrelDB) getSquirrelDB() *core.SquirrelDB {
	s.squirrelDBOnce.Do(func() {
		s.squirreldb = core.New(
			core.Deps{
				Logger:         s.Logger,
				MetricRegistry: s.MetricRegistry,
			},
			s.Config,
			core.SquirrelDBOption{
				PrimaryBackendReadOnly:  false,
				MigrateTargetImportOnly: false,
				InternalTesting: core.InternalTestingOption{
					DropDataOnStartup: s.DropDataOnStartup,
				},
			},
		)
	})

	return s.squirreldb
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

	_, err := s.tsdb(ctx)
	if err != nil {
		return fmt.Errorf("TSDB start fail: %w", err)
	}

	err = s.telemetry(ctx)
	if err != nil {
		return fmt.Errorf("telemetry start fail: %w", err)
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

func (s *SquirrelDB) Run(ctx context.Context, shutdownCtx context.Context) error {
	_ = shutdownCtx

	if err := s.Start(ctx); err != nil {
		return err
	}

	<-ctx.Done()
	s.Stop()

	return nil
}

// backend returns the (started, cached) storage backend. Test tools use it to
// reach backend-specific capabilities (e.g. a type-assert for the Cassandra lock
// factory or index test hooks); a tool needing a capability the configured
// backend doesn't provide can detect the failed assertion and skip itself.
func (s *SquirrelDB) backend(ctx context.Context) (core.Backend, error) {
	return s.getSquirrelDB().Primary(ctx)
}

// SquirrelDBCore returns core.SquirrelDB.
func (s *SquirrelDB) SquirrelDBCore() *core.SquirrelDB {
	return s.getSquirrelDB()
}

// MigrateToBackend returns the (started, cached) extra write backend — the
// dual-write / migration target named by Config.ExtraWriteBackend. It is a plain
// backend (not best-effort in itself: best-effort is a per-appender choice made
// by the dual-write combiner; the migrator writes to it reliably). Returns
// ErrNoExtraWriteBackend if none is configured.
func (s *SquirrelDB) MigrateToBackend(ctx context.Context) (core.Backend, error) {
	if s.Config.ExtraWriteBackend == "" {
		return nil, ErrNoExtraWriteBackend
	}

	return s.getSquirrelDB().Extra(ctx)
}

type namedTasks struct {
	Task types.OldTask
	Name string
}

func (s *SquirrelDB) apiTask(ctx context.Context, readiness chan error) {
	// Always go through the getter so the mutable label processor is initialized,
	// regardless of the TSDB backend (only the Cassandra TSDB would otherwise
	// trigger its creation as a side effect).
	mutableLabelProcessor, err := s.mutableLabelProcessor(ctx)
	if err != nil {
		readiness <- fmt.Errorf("mutable label processor start fail: %w", err)

		return
	}

	// The read path carries mutable-label translation; the write path is the
	// (possibly dual-write) combiner. Both are narrow interfaces, so the API never
	// reaches into a wrapped TSDB — the backend-specific capabilities below come
	// straight from the unwrapped primary backend.
	queryable, err := s.getSquirrelDB().Queryable(ctx)
	if err != nil {
		readiness <- fmt.Errorf("queryable start fail: %w", err)

		return
	}

	appendable, err := s.getSquirrelDB().Appendable(ctx)
	if err != nil {
		readiness <- fmt.Errorf("appendable start fail: %w", err)

		return
	}

	s.api.ListenAddress = s.Config.ListenAddress
	s.api.Queryable = queryable
	s.api.Appendable = appendable
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

	if s.api.MetricRegistry == nil {
		s.api.MetricRegistry = prometheus.DefaultRegisterer
	}

	if backend, err := s.backend(ctx); err == nil {
		s.api.BackendName = s.Config.Backend
		s.api.BackendDebug = backend

		// The header option parser and the (legacy) debug endpoints are backend
		// capabilities, reached on the unwrapped backend — not by asserting a TSDB.
		if parser, ok := backend.(types.HTTPOptionsParser); ok {
			s.api.OptionsParser = parser
		}

		if debug, ok := backend.(api.TSDBDebugEndpoint); ok {
			s.api.DebugEndpoint = debug
		}
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
	// aggregation, telemetry stops its refresh loops) then closes the connection —
	// for the primary and, if started, the dual-write extra.
	s.getSquirrelDB().CloseAll(stopCtx) //nolint:contextcheck
}

// tsdb returns the primary backend's tsdb — the concrete data-plane store, no
// mutable-label wrapping and no dual-write (the API reads via SquirrelDB.Queryable
// and writes via SquirrelDB.Appendable; this is the raw primary store used by tests
// and internal callers). Cassandra-index test hooks (don't-start-index, fake time)
// are set beforehand via Backend().SetCassandraIndexTestingOption, not here.
func (s *SquirrelDB) tsdb(ctx context.Context) (types.TSDB, error) {
	b, err := s.backend(ctx)
	if err != nil {
		return nil, err
	}

	return b.TSDB(ctx)
}

func (s *SquirrelDB) telemetry(ctx context.Context) error {
	if !s.Config.Telemetry.Enabled {
		return nil
	}

	sqDB := s.getSquirrelDB()

	cluster, err := sqDB.Cluster(ctx)
	if err != nil {
		return err
	}

	b, err := sqDB.Primary(ctx)
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
			Version:            version.Version,
			InstallationFormat: s.Config.Internal.Installation.Format,
			ClusterSizeFn:      cluster.Size,
			Store:              store,
			Logger:             s.Logger.With().Str("component", "telemetry").Logger(),
		},
	)
	tlm.Start(ctx)

	return nil
}

// mutableLabelProcessor returns the mutable label processor from the backend. It
// also ensures the API mutable label writer is wired (via MutableLabelProvider).
func (s *SquirrelDB) mutableLabelProcessor(ctx context.Context) (*mutable.LabelProcessor, error) {
	if _, err := s.mutableLabelProvider(ctx); err != nil {
		return nil, err
	}

	b, err := s.backend(ctx)
	if err != nil {
		return nil, err
	}

	return b.MutableProcessor(ctx)
}

// mutableLabelProvider returns the mutable label provider from the backend and
// wires it as the API mutable label writer.
func (s *SquirrelDB) mutableLabelProvider(ctx context.Context) (*mutable.Provider, error) {
	b, err := s.backend(ctx)
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

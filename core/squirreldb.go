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

// Package core is the storage glue of SquirrelDB: from the configured backends
// it composes the data-plane accessors (read, write, dual-write, per-request
// routing) that the API, the migration tool and the tests consume. Backends are
// never wrapped — only the narrow read/write interfaces are decorated (mutable
// labels on read, dual-write on write, routing on both) — so every
// backend-specific capability stays reachable on the Backend itself.
package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/appendable/fixedappender"
	cassandrabackend "github.com/bleemeo/squirreldb/cassandra/backend"
	clickhousebackend "github.com/bleemeo/squirreldb/clickhouse/backend"
	"github.com/bleemeo/squirreldb/config"
	dummybackend "github.com/bleemeo/squirreldb/dummy/backend"
	sandboxbackend "github.com/bleemeo/squirreldb/sandbox/backend"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/redis/cluster"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"
	"github.com/google/uuid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

var (
	// ErrNoExtra is returned by Extra when no extra (dual-write) backend is configured.
	ErrNoExtra      = errors.New("no extra backend configured")
	ErrNotSupported = errors.New("backend doesn't support the feature")
	ErrIsReadOnly   = errors.New("trying to write but read-only mode is enabled")
)

// Backend is the uniform storage interface both Cassandra and ClickHouse satisfy.
// A single backend (selected by the "backend" config option) provides every
// store; mixing backends is not supported, but a request can be routed to
// another, complete, backend (see docs/routing.md). Backend-specific capabilities beyond
// this interface (raw export/import, HTTP option parsing, the debug endpoints,
// the Cassandra index, ...) are reached by type-asserting the concrete Backend,
// which is never wrapped.
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
	BackendInfo() types.BackendInfo
}

// bestEffortAppendable is implemented by a TSDB (ClickHouse) that can hand out a
// best-effort, non-blocking appender for dual-write. When an extra TSDB
// implements it, the combiner uses it so an unavailable extra never blocks or
// fails the request; otherwise it falls back to the normal appender.
type bestEffortAppendable interface {
	BestEffortAppenderWithOptions(ctx context.Context, options types.AppenderOptions) storage.Appender
}

// extraAttachRetryInterval is how often AttachExtraWhenReady retries starting the
// dual-write extra backend after it was unavailable at startup.
const extraAttachRetryInterval = 30 * time.Second

type SquirrelDBOption struct {
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
	ExistingCluster         types.Cluster

	// InternalTesting are option useful for test and shouldn't be used for normal run.
	InternalTesting InternalTestingOption
}

type InternalTestingOption struct {
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
}

type Deps struct {
	Logger         zerolog.Logger
	MetricRegistry prometheus.Registerer
}

// SquirrelDB owns the started primary, optional extra and routed backends and
// hands out the composed data-plane accessors. It caches the (started-once)
// backend instances but never the result of Backend.TSDB() — the backends
// memoize their own TSDB.
//
// The primary backend ("backend" option) is the default route; the routed
// backends ("routing.backends" option, see docs/routing.md) are selectable per
// request. The extra backend only receives the dual-write of the default route.
// Telemetry, cluster, mutable label API and debug endpoints stay bound to the
// primary.
type SquirrelDB struct {
	config config.Config
	option SquirrelDBOption
	deps   Deps
	router *backendRouter
	// routingMetrics is nil when routing is disabled.
	routingMetrics *routingMetrics

	mu                sync.Mutex
	cluster           types.Cluster
	primary           Backend
	extra             Backend
	routed            map[string]Backend
	wg                sync.WaitGroup
	cancelExtraAttach context.CancelFunc

	// extraUnavailableSince stamps when a dual-write start attempt first failed, so
	// AttachExtraWhenReady can log the [since, now] window to repair once it attaches.
	extraUnavailableSince time.Time
}

func New(deps Deps, config config.Config, option SquirrelDBOption) *SquirrelDB {
	if deps.MetricRegistry == nil {
		deps.MetricRegistry = prometheus.DefaultRegisterer
	}

	b := &SquirrelDB{
		deps:   deps,
		config: config,
		option: option,
		router: newBackendRouter(config),
	}

	if config.Routing.Enabled() {
		b.routingMetrics = newRoutingMetrics(deps.MetricRegistry)
	}

	return b
}

// Cluster return an types.Cluster. The returned cluster should be closed after use.
func (b *SquirrelDB) Cluster(ctx context.Context) (types.Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.clusterLocked(ctx)
}

// clusterLocked builds/caches the cluster. Caller must hold b.mu.
func (b *SquirrelDB) clusterLocked(ctx context.Context) (types.Cluster, error) {
	if b.cluster == nil && b.option.ExistingCluster != nil {
		b.cluster = b.option.ExistingCluster
	}

	if b.cluster == nil {
		if len(b.config.Redis.Addresses) > 0 && b.config.Redis.Addresses[0] != "" {
			c := &cluster.Cluster{
				ID:             uuid.New(),
				RedisOptions:   b.config.Redis,
				MetricRegistry: b.deps.MetricRegistry,
				Keyspace:       b.config.Redis.Keyspace,
				Logger:         b.deps.Logger.With().Str("component", "cluster").Logger(),
			}

			err := c.Start(ctx)
			if err != nil {
				_ = c.Stop()

				return c, err
			}

			b.cluster = c
		} else {
			b.cluster = &dummy.LocalCluster{}
		}
	}

	return b.cluster, nil
}

// Primary returns the (started, cached) primary backend.
func (b *SquirrelDB) Primary(ctx context.Context) (Backend, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.primary != nil {
		return b.primary, nil
	}

	cluster, err := b.clusterLocked(ctx)
	if err != nil {
		return nil, err
	}

	p, err := b.startBackend(
		ctx,
		b.config.Backend,
		b.config.Internal.ReadOnly || b.option.PrimaryBackendReadOnly,
		false,
		cluster,
		b.deps.MetricRegistry,
	)
	if err != nil {
		return nil, err
	}

	b.primary = p

	return p, nil
}

// Extra returns the (started, cached) extra (dual-write target) backend, or
// ErrNoExtra if none is configured.
func (b *SquirrelDB) Extra(ctx context.Context) (Backend, error) {
	if b.config.ExtraWriteBackend == "" {
		return nil, ErrNoExtra
	}

	b.mu.Lock()
	extra := b.extra
	b.mu.Unlock()

	if extra == nil {
		var err error

		cluster, err := b.Cluster(ctx)
		if err != nil {
			return nil, err
		}

		extra, err = b.startBackend(
			ctx,
			b.config.ExtraWriteBackend,
			b.config.Internal.ReadOnly,
			b.option.MigrateTargetImportOnly,
			cluster,
			b.deps.MetricRegistry,
		)
		if err != nil {
			return nil, err
		}

		b.mu.Lock()
		// re-check to ensure no concurrent call started the backend at the same time
		if b.extra != nil {
			extra.CloseAll(ctx)

			extra = b.extra
		}

		b.extra = extra

		b.mu.Unlock()
	}

	return extra, nil
}

// Routed returns the (started, cached) routed backends ("routing.backends"
// option), by name. They are all started on the first call, which fails if any
// of them fails to start. It returns an empty map when no routed backend is
// configured.
func (b *SquirrelDB) Routed(ctx context.Context) (map[string]Backend, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.config.Routing.Backends) == 0 {
		return map[string]Backend{}, nil
	}

	cluster, err := b.clusterLocked(ctx)
	if err != nil {
		return nil, err
	}

	if b.routed == nil {
		b.routed = make(map[string]Backend, len(b.config.Routing.Backends))
	}

	for _, name := range b.config.Routing.Backends {
		if _, ok := b.routed[name]; ok {
			continue
		}

		// The routed backends share the registry of the primary: the duplicate
		// metrics of their shared components are ignored.
		backend, err := b.startBackend(
			ctx,
			name,
			b.config.Internal.ReadOnly,
			false,
			cluster,
			&routedRegisterer{reg: b.deps.MetricRegistry},
		)
		if err != nil {
			return nil, fmt.Errorf("start routed backend %q: %w", name, err)
		}

		b.routed[name] = backend
	}

	return maps.Clone(b.routed), nil
}

// ResolveBackend returns the name of the backend serving a request of tenant,
// which requested the backend requested (X-SquirrelDB-Backend header, empty when
// absent) and may only use the allowed backends (the backends of the
// authenticated user, empty meaning no restriction). It returns an error wrapping
// types.ErrBackendNotSelectable if the requested backend isn't enabled, or
// types.ErrBackendNotAllowed if the request is routed to a backend it isn't
// allowed to use. The API calls it when a request starts, to reject the request
// early; the routing queryable/appendable resolve the backend the same way for
// each querier/appender, from the same values carried by their options.
func (b *SquirrelDB) ResolveBackend(_ context.Context, tenant, requested string, allowed []string) (string, error) {
	name, _, err := b.router.resolveBackend(tenant, requested, allowed)

	return name, err
}

// startBackend builds and starts one backend by name. reg is the registry of its
// metrics.
func (b *SquirrelDB) startBackend(
	ctx context.Context,
	name string,
	readOnly bool,
	importOnly bool,
	cluster types.Cluster,
	reg prometheus.Registerer,
) (Backend, error) {
	result, err := b.newBackendByName(ctx, name, cluster, reg, readOnly, importOnly)
	if err != nil {
		return nil, err
	}

	if err := result.Start(ctx); err != nil {
		return nil, err
	}

	return result, nil
}

// newBackendByName builds (unstarted) the concrete backend for a single name.
// readOnly makes the backend refuse writes (the migrate source uses it).
func (b *SquirrelDB) newBackendByName(
	ctx context.Context,
	name string,
	cluster types.Cluster,
	reg prometheus.Registerer,
	readOnly bool,
	importOnly bool,
) (Backend, error) {
	switch name {
	case config.BackendCassandra:
		return cassandrabackend.New(
			ctx, b.deps.Logger.With().Str("backend", "cassandra").Logger(),
			b.cassandraBackendOptions(cluster, reg, readOnly))
	case config.BackendClickHouse:
		return clickhousebackend.New(
			ctx, b.deps.Logger.With().Str("backend", "clickhouse").Logger(),
			b.clickHouseBackendOptions(cluster, reg, readOnly, importOnly))
	case config.BackendDummy:
		return dummybackend.New(
			ctx, b.deps.Logger.With().Str("backend", "dummy").Logger(), b.dummyBackendOptions(cluster, reg))
	case config.BackendSandbox:
		return sandboxbackend.New(
			ctx, b.deps.Logger.With().Str("backend", "sandbox").Logger(), b.sandboxBackendOptions(cluster, reg))
	default:
		return nil, fmt.Errorf("unknown backend: %v", name)
	}
}

// cassandraBackendOptions builds the Cassandra backend options from the config
// and the daemon-provided cluster. Index test hooks (DontStartIndex/
// InternalNowFunction) are set later via Backend.SetCassandraIndexTestingOption.
func (b *SquirrelDB) cassandraBackendOptions(
	cluster types.Cluster,
	reg prometheus.Registerer,
	readOnly bool,
) cassandrabackend.Options {
	getAggregateIntendedDuration := func() time.Duration {
		// The largest the cluster, the slower we can run pre-aggregation.
		return b.config.Cassandra.Aggregate.IntendedDuration * time.Duration(cluster.Size())
	}

	options := cassandrabackend.Options{
		CassandraConfig:           b.config.Cassandra,
		ReadOnly:                  readOnly,
		MetricRegistry:            reg,
		Cluster:                   cluster,
		TenantLabel:               b.config.TenantLabelName,
		DefaultTimeToLive:         b.config.DefaultTimeToLive,
		AggregateIntendedDuration: getAggregateIntendedDuration,
		DisablePreAggregation:     b.config.Internal.DisablePreAggregation,
		BatchSize:                 b.config.Batch.Size,
		PreCreateShardDuration:    b.config.Cassandra.PreCreateShardDuration,
		PreCreateShardFraction:    b.config.Cassandra.PreCreateShardFraction,
		TelemetryTiming:           b.option.InternalTesting.TelemetryStoreTiming,
		DropDataOnStartup:         b.option.InternalTesting.DropDataOnStartup,
	}

	if b.config.Internal.TemporaryStore == "redis" {
		options.Redis = b.config.Redis
	}

	return options
}

// clickHouseBackendOptions builds the ClickHouse backend options. importOnly builds
// the TSDB for bulk import only (no WAL drainer) — see MigrateTargetImportOnly.
func (b *SquirrelDB) clickHouseBackendOptions(
	cluster types.Cluster,
	reg prometheus.Registerer,
	readOnly bool,
	importOnly bool,
) clickhousebackend.Options {
	options := clickhousebackend.Options{
		Config:            b.config.ClickHouse,
		MetricRegistry:    reg,
		ReadOnly:          readOnly,
		Cluster:           cluster,
		TenantLabel:       b.config.TenantLabelName,
		DefaultTimeToLive: b.config.DefaultTimeToLive,
		ClusterName:       b.config.ClickHouse.ClusterName,
		TelemetryTiming:   b.option.InternalTesting.TelemetryStoreTiming,
		DropDataOnStartup: b.option.InternalTesting.DropDataOnStartup,
		ImportOnly:        importOnly,
	}

	if b.config.Internal.TemporaryStore == "redis" {
		options.Redis = b.config.Redis
	}

	return options
}

// dummyBackendOptions builds the dummy backend options. The dummy backend
// discards all writes; the batcher (and its temporary store) are only wired when
// dummy.batcher is set.
func (b *SquirrelDB) dummyBackendOptions(cluster types.Cluster, reg prometheus.Registerer) dummybackend.Options {
	options := dummybackend.Options{
		MetricRegistry: reg,
		Cluster:        cluster,
		TenantLabel:    b.config.TenantLabelName,
		Batch:          b.config.Batch,
		Batcher:        b.config.Internal.Dummy.Batcher,
	}

	if b.config.Internal.TemporaryStore == "redis" {
		options.Redis = b.config.Redis
	}

	return options
}

// sandboxBackendOptions builds the sandbox backend options. The sandbox backend
// serves fake data from a dataset and discards all writes.
func (b *SquirrelDB) sandboxBackendOptions(cluster types.Cluster, reg prometheus.Registerer) sandboxbackend.Options {
	return sandboxbackend.Options{
		MetricRegistry: reg,
		Cluster:        cluster,
		TenantLabel:    b.config.TenantLabelName,
		DatasetConfig:  b.config.Sandbox.DatasetConfig,
	}
}

// Queryable returns the read path: the primary backend's queryable with
// mutable-label translation applied (the normal API read path). When routing is
// enabled, it's a routing queryable serving each querier from the backend
// resolved from its options, each backend with its own mutable-label translation.
func (b *SquirrelDB) Queryable(ctx context.Context) (types.QueryableWithOptions, error) {
	primary, err := b.Primary(ctx)
	if err != nil {
		return nil, err
	}

	primaryQueryable, err := b.backendQueryable(ctx, primary)
	if err != nil {
		return nil, err
	}

	if !b.config.Routing.Enabled() {
		return primaryQueryable, nil
	}

	routed, err := b.Routed(ctx)
	if err != nil {
		return nil, err
	}

	targets := map[string]types.QueryableWithOptions{b.config.Backend: primaryQueryable}

	for name, backend := range routed {
		targets[name], err = b.backendQueryable(ctx, backend)
		if err != nil {
			return nil, fmt.Errorf("routed backend %q: %w", name, err)
		}
	}

	return &routingQueryable{router: b.router, metrics: b.routingMetrics, targets: targets}, nil
}

// backendQueryable returns the backend's queryable with mutable-label
// translation applied, using the backend's own mutable label processor, unless
// the backend applies the mutable labels itself.
func (b *SquirrelDB) backendQueryable(ctx context.Context, backend Backend) (types.QueryableWithOptions, error) {
	inner, err := backend.TSDB(ctx)
	if err != nil {
		return nil, err
	}

	if backend.BackendInfo().ApplyMutableLabelInternally {
		return inner, nil
	}

	processor, err := backend.MutableProcessor(ctx)
	if err != nil {
		return nil, err
	}

	return mutable.NewQueryable(inner, processor, b.deps.Logger.With().Str("component", "mutable").Logger()), nil
}

// Appendable returns the write path. When routing is enabled, it's a routing
// appendable writing each request only to the backend resolved from its options:
// the default route uses the primary write path (see primaryAppendable), a routed
// backend is written alone (no dual-write).
func (b *SquirrelDB) Appendable(ctx context.Context) (types.AppendableWithOptions, error) {
	if b.config.Internal.ReadOnly {
		return fixedappender.NewErrorAppendableWithOptions(ErrIsReadOnly), nil
	}

	primaryAppendable, err := b.primaryAppendable(ctx)
	if err != nil {
		return nil, err
	}

	if !b.config.Routing.Enabled() {
		return primaryAppendable, nil
	}

	routed, err := b.Routed(ctx)
	if err != nil {
		return nil, err
	}

	targets := map[string]types.AppendableWithOptions{b.config.Backend: primaryAppendable}

	for name, backend := range routed {
		targets[name], err = backend.TSDB(ctx)
		if err != nil {
			return nil, fmt.Errorf("routed backend %q: %w", name, err)
		}
	}

	return &routingAppendable{router: b.router, metrics: b.routingMetrics, targets: targets}, nil
}

// primaryAppendable returns the primary write path. With an extra backend
// configured it is a dual-write combiner (primary must-succeed + extra
// best-effort); otherwise it is the primary backend's appendable directly. The
// combiner reads the live extra on each request, so an extra attached later
// (AttachExtraWhenReady) starts receiving writes without rebuilding the appendable.
func (b *SquirrelDB) primaryAppendable(ctx context.Context) (types.AppendableWithOptions, error) {
	primary, err := b.Primary(ctx)
	if err != nil {
		return nil, err
	}

	primaryTSDB, err := primary.TSDB(ctx)
	if err != nil {
		return nil, err
	}

	if b.config.ExtraWriteBackend == "" {
		return primaryTSDB, nil
	}

	// The dual-write extra backend is best-effort at startup: if it cannot be
	// started now (e.g. ClickHouse unreachable), start primary-only and attach it in
	// the background once it becomes reachable (see AttachExtraWhenReady), rather
	// than failing the whole daemon.
	b.tryStartExtra(ctx)

	return &dualWriteAppendable{
		primary: primaryTSDB,
		extras:  b.extraTSDBs,
		logger:  b.deps.Logger,
	}, nil
}

// HTTPOptionsParser returns the parser of the per-request backend options
// (BackendOptions of the querier/appender options), or nil when no backend has
// one. Without routing, it's the primary backend's parser. With routing, the
// headers are parsed by the parser of every selectable backend and the routing
// queryable/appendable hands to the backend serving a request the options it
// parsed.
func (b *SquirrelDB) HTTPOptionsParser(ctx context.Context) (types.HTTPOptionsParser, error) {
	primary, err := b.Primary(ctx)
	if err != nil {
		return nil, err
	}

	primaryParser, primaryOk := primary.(types.HTTPOptionsParser)

	if !b.config.Routing.Enabled() {
		if !primaryOk {
			return nil, nil //nolint:nilnil // no parser is not an error.
		}

		return primaryParser, nil
	}

	routed, err := b.Routed(ctx)
	if err != nil {
		return nil, err
	}

	parsers := make(map[string]types.HTTPOptionsParser, len(routed)+1)

	if primaryOk {
		parsers[b.config.Backend] = primaryParser
	}

	for name, backend := range routed {
		if parser, ok := backend.(types.HTTPOptionsParser); ok {
			parsers[name] = parser
		}
	}

	if len(parsers) == 0 {
		return nil, nil //nolint:nilnil // no parser is not an error.
	}

	return routedOptionsParser{parsers: parsers}, nil
}

// extraTSDBs returns the current extra TSDB(s) for dual-write, or nil when the
// extra is not (yet) started. A build error is logged and treated as no extra
// (best-effort): a point is written to the primary and repaired later.
func (b *SquirrelDB) extraTSDBs(ctx context.Context) []types.TSDB {
	b.mu.Lock()
	extra := b.extra
	b.mu.Unlock()

	if extra == nil {
		return nil
	}

	t, err := extra.TSDB(ctx)
	if err != nil {
		b.deps.Logger.Warn().Err(err).Msg("dual-write extra TSDB unavailable; writing to the primary only")

		return nil
	}

	return []types.TSDB{t}
}

// tryStartExtra makes a single best-effort attempt to start the extra backend at
// startup. On failure (e.g. ClickHouse unreachable) it starts attachExtraWhenReady
// in the background to retry the connection asynchronously, rather than failing the daemon.
func (b *SquirrelDB) tryStartExtra(ctx context.Context) {
	if b.config.ExtraWriteBackend == "" {
		return
	}

	if _, err := b.Extra(ctx); err != nil {
		b.deps.Logger.Warn().Err(err).
			Msg("dual-write target unavailable at startup; starting primary-only and retrying in the background")

		b.mu.Lock()
		defer b.mu.Unlock()

		// Make sure we only start the background task once
		if b.extraUnavailableSince.IsZero() {
			b.extraUnavailableSince = time.Now()
			b.startBackgroundAttachExtra(ctx)
		}
	}
}

// startBackgroundAttachExtra starts attachExtraWhenReady in a background goroutine
// that is  stopped and waited on by CloseAll. Caller must hold b.mu.
func (b *SquirrelDB) startBackgroundAttachExtra(ctx context.Context) {
	subCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	b.cancelExtraAttach = cancel
	b.wg.Add(1)

	go func() {
		defer logger.ProcessPanic()
		defer b.wg.Done()

		b.attachExtraWhenReady(subCtx)
	}()
}

// attachExtraWhenReady retries starting the extra backend until it succeeds (or
// ctx is cancelled). Once attached, dual-write resumes without a restart; it logs
// the window during which the extra was unavailable — points written then were
// not dual-written and must be repaired over that range.
func (b *SquirrelDB) attachExtraWhenReady(ctx context.Context) {
	ticker := time.NewTicker(extraAttachRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if _, err := b.Extra(ctx); err != nil {
			b.deps.Logger.Debug().Err(err).Msg("dual-write target still unavailable, will retry")

			continue
		}

		b.mu.Lock()
		since := b.extraUnavailableSince
		b.mu.Unlock()

		b.deps.Logger.Warn().
			Str("repair_from", since.Format(time.RFC3339)).
			Str("repair_to", time.Now().Format(time.RFC3339)).
			Msg("dual-write target attached; points written while it was unavailable were not dual-written — " +
				"repair that window with `squirreldb migrate --repair-from <repair_from> --repair-to <repair_to>`")

		return
	}
}

// CloseAll closes the primary, the routed and (if started) the extra backend.
func (b *SquirrelDB) CloseAll(ctx context.Context) {
	b.mu.Lock()

	if b.cancelExtraAttach != nil {
		b.cancelExtraAttach()
		b.cancelExtraAttach = nil
	}

	b.mu.Unlock()

	b.wg.Wait()

	b.mu.Lock()

	primary := b.primary
	extra := b.extra
	routed := b.routed
	cluster := b.cluster

	b.mu.Unlock()

	if primary != nil {
		primary.CloseAll(ctx)
	}

	for _, name := range slices.Sorted(maps.Keys(routed)) {
		routed[name].CloseAll(ctx)
	}

	if extra != nil {
		extra.CloseAll(ctx)
	}

	if cluster != nil {
		if err := cluster.Close(); err != nil {
			b.deps.Logger.Error().Msgf("Failed to stop cluster: %s", err.Error())
		}
	}
}

// BackendStates returns the state of the primary and (if configured) extra
// backends, for the admin status page. It never starts a backend nor blocks on
// one: it only reads what is already known.
func (b *SquirrelDB) BackendStates() []types.BackendState {
	b.mu.Lock()
	defer b.mu.Unlock()

	states := []types.BackendState{backendState(b.config.Backend, types.BackendRolePrimary, b.primary)}

	if b.config.ExtraWriteBackend != "" {
		extra := backendState(b.config.ExtraWriteBackend, types.BackendRoleExtra, b.extra)
		extra.UnavailableSince = b.extraUnavailableSince
		states = append(states, extra)
	}

	return states
}

func backendState(name string, role string, backend Backend) types.BackendState {
	state := types.BackendState{Name: name, Role: role}

	if backend != nil {
		state.Started = true
		state.Backend = backend
		state.Info = backend.BackendInfo()
	}

	return state
}

// StartedCluster returns the cluster, or nil if it was not built yet. Unlike
// Cluster, it never starts it.
func (b *SquirrelDB) StartedCluster() types.Cluster {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.cluster
}

// TestTools return tools that could be used during tests.
func (b *SquirrelDB) TestTools() *SquirrelDBTestTools {
	return &SquirrelDBTestTools{b: b}
}

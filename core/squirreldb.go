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
// it composes the data-plane accessors (read, write, dual-write) that the API,
// the migration tool and the tests consume. Backends are never wrapped — only
// the narrow read/write interfaces are decorated (mutable labels on read,
// dual-write on write) — so every backend-specific capability stays reachable on
// the Backend itself.
package core

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/appendable/fixedappender"
	cassandrabackend "github.com/bleemeo/squirreldb/cassandra/backend"
	clickhousebackend "github.com/bleemeo/squirreldb/clickhouse/backend"
	"github.com/bleemeo/squirreldb/config"
	dummybackend "github.com/bleemeo/squirreldb/dummy/backend"

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
// store; mixing backends is not supported. Backend-specific capabilities beyond
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

// SquirrelDB owns the started primary and optional extra backends and hands out the
// composed data-plane accessors. It caches the (started-once) backend instances
// but never the result of Backend.TSDB() — the backends memoize their own TSDB.
type SquirrelDB struct {
	config config.Config
	option SquirrelDBOption
	deps   Deps

	mu                sync.Mutex
	cluster           types.Cluster
	primary           Backend
	extra             Backend
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

	return &SquirrelDB{
		deps:   deps,
		config: config,
		option: option,
	}
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

	p, err := b.startBackend(
		ctx,
		b.config.Backend,
		b.config.Internal.ReadOnly || b.option.PrimaryBackendReadOnly,
		false,
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

		extra, err = b.startBackend(
			ctx,
			b.config.ExtraWriteBackend,
			b.config.Internal.ReadOnly,
			b.option.MigrateTargetImportOnly,
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

// startBackend builds and starts one backend by name.
func (b *SquirrelDB) startBackend(
	ctx context.Context,
	name string,
	readOnly bool,
	importOnly bool,
) (Backend, error) {
	cluster, err := b.clusterLocked(ctx)
	if err != nil {
		return nil, err
	}

	result, err := b.newBackendByName(ctx, name, cluster, readOnly, importOnly)
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
	readOnly bool,
	importOnly bool,
) (Backend, error) {
	switch name {
	case config.BackendCassandra:
		return cassandrabackend.New(
			ctx, b.deps.Logger.With().Str("backend", "cassandra").Logger(), b.cassandraBackendOptions(cluster, readOnly))
	case config.BackendClickHouse:
		return clickhousebackend.New(
			ctx, b.deps.Logger.With().Str("backend", "clickhouse").Logger(),
			b.clickHouseBackendOptions(cluster, readOnly, importOnly))
	case config.BackendDummy:
		return dummybackend.New(
			ctx, b.deps.Logger.With().Str("backend", "dummy").Logger(), b.dummyBackendOptions(cluster))
	default:
		return nil, fmt.Errorf("unknown backend: %v", name)
	}
}

// cassandraBackendOptions builds the Cassandra backend options from the config
// and the daemon-provided cluster. Index test hooks (DontStartIndex/
// InternalNowFunction) are set later via Backend.SetCassandraIndexTestingOption.
func (b *SquirrelDB) cassandraBackendOptions(cluster types.Cluster, readOnly bool) cassandrabackend.Options {
	getAggregateIntendedDuration := func() time.Duration {
		// The largest the cluster, the slower we can run pre-aggregation.
		return b.config.Cassandra.Aggregate.IntendedDuration * time.Duration(cluster.Size())
	}

	options := cassandrabackend.Options{
		CassandraConfig:           b.config.Cassandra,
		ReadOnly:                  readOnly,
		MetricRegistry:            b.deps.MetricRegistry,
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
	readOnly bool,
	importOnly bool,
) clickhousebackend.Options {
	options := clickhousebackend.Options{
		Config:            b.config.ClickHouse,
		MetricRegistry:    b.deps.MetricRegistry,
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
func (b *SquirrelDB) dummyBackendOptions(cluster types.Cluster) dummybackend.Options {
	options := dummybackend.Options{
		MetricRegistry: b.deps.MetricRegistry,
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

// Queryable returns the primary backend's queryable with mutable-label
// translation applied (the normal API read path).
func (b *SquirrelDB) Queryable(ctx context.Context) (types.QueryableWithOptions, error) {
	primary, err := b.Primary(ctx)
	if err != nil {
		return nil, err
	}

	inner, err := primary.TSDB(ctx)
	if err != nil {
		return nil, err
	}

	if primary.BackendInfo().ApplyMutableLabelInternally {
		return inner, nil
	}

	processor, err := primary.MutableProcessor(ctx)
	if err != nil {
		return nil, err
	}

	return mutable.NewQueryable(inner, processor, b.deps.Logger.With().Str("component", "mutable").Logger()), nil
}

// Appendable returns the write path. With an extra backend configured it is a
// dual-write combiner (primary must-succeed + extra best-effort); otherwise it is
// the primary backend's appendable directly. The combiner reads the live extra on
// each request, so an extra attached later (AttachExtraWhenReady) starts receiving
// writes without rebuilding the appendable.
func (b *SquirrelDB) Appendable(ctx context.Context) (types.AppendableWithOptions, error) {
	if b.config.Internal.ReadOnly {
		return fixedappender.NewErrorAppendableWithOptions(ErrIsReadOnly), nil
	}

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

// CloseAll closes the primary and (if started) the extra backend.
func (b *SquirrelDB) CloseAll(ctx context.Context) {
	b.mu.Lock()
	primary := b.primary
	extra := b.extra
	cluster := b.cluster

	if b.cancelExtraAttach != nil {
		b.cancelExtraAttach()
	}

	b.mu.Unlock()

	b.wg.Wait()

	if primary != nil {
		primary.CloseAll(ctx)
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

// TestTools return tools that could be used during tests.
func (b *SquirrelDB) TestTools() *SquirrelDBTestTools {
	return &SquirrelDBTestTools{b: b}
}

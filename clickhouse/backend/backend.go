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

// Package backend owns the single ClickHouse connection shared by every
// ClickHouse-backed store (TSDB, mutable labels, telemetry), mirroring
// cassandra/backend. The connection is a clickhouse-go pool, safe for
// concurrent use, so all stores share it instead of opening their own.
package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"
	chmutable "github.com/bleemeo/squirreldb/clickhouse/mutable"
	"github.com/bleemeo/squirreldb/clickhouse/routing"
	chstate "github.com/bleemeo/squirreldb/clickhouse/state"
	"github.com/bleemeo/squirreldb/clickhouse/telemetrystore"
	"github.com/bleemeo/squirreldb/clickhouse/tsdb"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// TestDatabase is the ClickHouse database used by tests. DropData refuses to
// drop tables in any other database, to guard against wiping production data.
const TestDatabase = "squirreldb_test"

var (
	ErrNotStarted     = errors.New("backend not started")
	ErrAlreadyStarted = errors.New("already started")
)

// Options carries everything the ClickHouse backend needs. The TSDB-specific
// options are derived from it; there is a single TSDB instance per backend.
type Options struct {
	Config            config.ClickHouse
	Redis             config.Redis // If unset, the TSDB uses an in-memory batching store.
	MetricRegistry    prometheus.Registerer
	Cluster           types.Cluster
	TenantLabel       string
	DefaultTimeToLive time.Duration
	// ClusterName enables ClickHouse cluster mode (ON CLUSTER + Replicated) for
	// every store's schema. Empty means a single ClickHouse node.
	ClusterName string
	ReadOnly    bool
	// ImportOnly builds the TSDB for bulk import only (no WAL drainer) — set by the
	// `squirreldb migrate` target, which bypasses the WAL owned by the live daemon.
	ImportOnly bool
	// TelemetryTiming overrides the telemetry store's lease timing. It is a test
	// hook (zero fields keep the defaults), not a user-facing config option.
	TelemetryTiming telemetry.StoreTiming
	// DropDataOnStartup makes Start drop every SquirrelDB table before recreating
	// the schema. It is a test hook, not a user-facing config option, and refuses
	// to run on any database other than the test one.
	DropDataOnStartup bool
}

type Backend struct {
	options Options
	logger  zerolog.Logger

	l                sync.Mutex
	routing          *routing.Conn
	mutableStore     mutable.Store
	mutableProvider  *mutable.Provider
	mutableProcessor *mutable.LabelProcessor
	telemetryStore   *telemetrystore.Store
	stateStore       *chstate.Store
	tsdb             *tsdb.ClickHouseTSDB
}

// New returns a new unstarted backend.
func New(_ context.Context, logger zerolog.Logger, options Options) (*Backend, error) {
	return &Backend{options: options, logger: logger}, nil
}

// distributedDDLTaskTimeout caps, in seconds, how long an "ON CLUSTER" statement
// waits for every node to apply it. The server default (180s) means a slow node
// blocks for three minutes. We only ever run "ON CLUSTER" DDL at a genuine
// bootstrap (existing tables are skipped — see ddl.EnsureTables), and a failed
// bootstrap should surface to the operator, so we keep the default throwing
// behaviour and just shorten the wait to 30s.
const distributedDDLTaskTimeout = 30

// chSettings are applied to every per-host connection.
//
//nolint:gochecknoglobals
var chSettings = clickhouse.Settings{
	"distributed_ddl_task_timeout": distributedDDLTaskTimeout,
}

// chCompression is applied to every per-host connection. LZ4 block compression on
// the native protocol markedly cuts the wire size — and the flush latency — of the
// large, repetitive INSERT blocks the write path sends during a backlog catch-up,
// for negligible CPU. (The default, no compression, leaves those blocks raw.)
//
//nolint:gochecknoglobals
var chCompression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}

// openConn opens a plain (non-routing) ClickHouse connection over all addresses.
// Used by DropData; the live read/write path uses the routing connection opened
// in Start.
func openConn(cfg config.ClickHouse) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings:    chSettings,
		Compression: chCompression,
	})
}

// allSchemaObjects is every object the ClickHouse stores own, built from each
// store's SchemaObjects so there is a single source of truth (no hardcoded list).
func allSchemaObjects(opts Options) []ddl.Object {
	objects := tsdb.SchemaObjects(opts.ClusterName)
	objects = append(objects, chmutable.SchemaObjects(opts.ClusterName)...)
	objects = append(objects, telemetrystore.SchemaObjects(opts.ClusterName)...)
	objects = append(objects, chstate.SchemaObjects(opts.ClusterName)...)

	return objects
}

// expectedTables lists every table a host must hold to be routed to (see
// clickhouse/routing). It always includes the mutable tables, even in read-only
// mode: a read-only node still needs them to serve mutable-label reads.
func expectedTables(opts Options) []string {
	objects := allSchemaObjects(opts)
	names := make([]string, len(objects))

	for i, o := range objects {
		names[i] = o.Name
	}

	return names
}

// schemaObjects is the set this backend creates: everything, or NOTHING in
// read-only mode (a read-only node assumes every table already exists — it still
// expects them for routing, see expectedTables, but never creates them).
func (b *Backend) schemaObjects() []ddl.Object {
	if b.options.ReadOnly {
		return nil
	}

	return allSchemaObjects(b.options)
}

// dropReplicaRunbook is the operator hint returned when a table cannot be created
// because its replica is still registered in Keeper (a lost/rebuilt replica).
func (b *Backend) dropReplicaRunbook() string {
	var sb strings.Builder

	sb.WriteString("This node looks like a lost/rebuilt replica. Re-attach it by running SYSTEM DROP REPLICA " +
		"from a HEALTHY node (CAREFUL: make sure another replica still holds the data, or you will lose it), " +
		"then POST this endpoint again:\n\n")
	sb.WriteString("LOST_REPLICA=\"replica01\"  # REPLACE with the lost replica's name\n")

	for _, o := range allSchemaObjects(b.options) {
		if !o.Replicated {
			continue
		}

		fmt.Fprintf(&sb, "clickhouse-client -d %s --query \"SYSTEM DROP REPLICA '$LOST_REPLICA' FROM TABLE %s;\"\n",
			b.options.Config.Database, o.Name)
	}

	return sb.String()
}

// Start opens the shared routing connection (one underlying connection per host,
// routed to healthy hosts) and runs its initial health probe. It fails only when
// no configured host is reachable; a host that is reachable but missing tables or
// resyncing is simply excluded from routing, not a startup error.
func (b *Backend) Start(ctx context.Context) error {
	b.l.Lock()
	defer b.l.Unlock()

	if b.routing != nil {
		return ErrAlreadyStarted
	}

	cfg := b.options.Config

	b.logger.Debug().
		Strs("addresses", cfg.Addresses).
		Str("database", cfg.Database).
		Str("clusterName", b.options.ClusterName).
		Msgf("Starting Clickhouse backend")

	// A test may ask for a clean slate: drop every table before the schema is
	// (re)created below, on its own short-lived connection (routing is not open
	// yet), so the drop happens on this very backend instead of a separate one.
	if b.options.DropDataOnStartup && !b.options.ReadOnly {
		if err := b.dropData(ctx); err != nil {
			return fmt.Errorf("clickhouse: drop data on startup: %w", err)
		}
	}

	conn, err := routing.New(ctx, routing.Options{
		Addresses: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings:             chSettings,
		Compression:          chCompression,
		MaxConcurrentQueries: cfg.MaxConcurrentQueries,
		ExpectedTables:       expectedTables(b.options),
		ClusterName:          b.options.ClusterName,
		Registerer:           b.options.MetricRegistry,
		Logger:               b.logger.With().Str("component", "clickhouse-routing").Logger(),
	})
	if err != nil {
		return fmt.Errorf("clickhouse: open connection: %w", err)
	}

	if !conn.AnyReachable() {
		_ = conn.Close()

		return fmt.Errorf("clickhouse: no reachable host among %v", cfg.Addresses)
	}

	b.routing = conn

	// Create the whole schema up front (idempotent), so a host can become "ready"
	// regardless of which stores are later instantiated — the health check expects
	// every table, and stores are built lazily.
	if err := ddl.EnsureTables(ctx, b.routing, b.logger, b.schemaObjects()); err != nil {
		_ = conn.Close()
		b.routing = nil

		return fmt.Errorf("clickhouse: create schema: %w", err)
	}

	return nil
}

// TSDB returns the (single, cached) ClickHouse TSDB, sharing the backend connection.
func (b *Backend) TSDB(ctx context.Context) (types.TSDB, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.routing == nil {
		return nil, ErrNotStarted
	}

	if b.tsdb != nil {
		return b.tsdb, nil
	}

	t, err := tsdb.New(
		ctx,
		b.routing,
		b.logger.With().Str("component", "tsdb").Logger(),
		tsdb.Options{
			ClusterName:       b.options.ClusterName,
			Redis:             b.options.Redis,
			Reg:               b.options.MetricRegistry,
			TenantLabel:       b.options.TenantLabel,
			DefaultTimeToLive: b.options.DefaultTimeToLive,
			WALFlushDelay:     b.options.Config.WALFlushDelay,
			WALSoftThreshold:  b.options.Config.WALSoftThreshold,
			ReadOnly:          b.options.ReadOnly,
			ImportOnly:        b.options.ImportOnly,
		},
	)
	if err != nil {
		return nil, err
	}

	b.tsdb = t

	return t, nil
}

// MutableProvider returns the mutable label provider (used by the API to read
// and write mutable labels).
func (b *Backend) MutableProvider(ctx context.Context) (*mutable.Provider, error) {
	b.l.Lock()
	defer b.l.Unlock()

	provider, _, err := b.getMutableStack(ctx)

	return provider, err
}

// MutableProcessor returns the mutable label processor (used by the API to
// translate mutable labels).
func (b *Backend) MutableProcessor(ctx context.Context) (*mutable.LabelProcessor, error) {
	b.l.Lock()
	defer b.l.Unlock()

	_, processor, err := b.getMutableStack(ctx)

	return processor, err
}

// getMutableStack builds (once) the mutable label store, provider and processor.
// Caller must hold b.l.
func (b *Backend) getMutableStack(ctx context.Context) (*mutable.Provider, *mutable.LabelProcessor, error) {
	if b.routing == nil {
		return nil, nil, ErrNotStarted
	}

	if b.mutableProvider != nil {
		return b.mutableProvider, b.mutableProcessor, nil
	}

	if b.mutableStore == nil {
		store, err := chmutable.NewStore(ctx, b.routing, chmutable.Options{
			ReadOnly:    b.options.ReadOnly,
			ClusterName: b.options.ClusterName,
			Logger:      b.logger.With().Str("component", "mutable-labels").Logger(),
		})
		if err != nil {
			return nil, nil, err
		}

		b.mutableStore = store
	}

	provider, processor := mutable.NewProviderAndProcessor(
		ctx,
		// The mutable package registers backend-agnostic metric names; tag them with
		// the backend so a source and a target (migrate) can coexist in one registry.
		prometheus.WrapRegistererWith(map[string]string{"backend": "clickhouse"}, b.options.MetricRegistry),
		b.options.Cluster,
		b.mutableStore,
		b.options.TenantLabel,
		b.logger.With().Str("component", "label_provider").Logger(),
	)

	b.mutableProvider = provider
	b.mutableProcessor = processor

	return provider, processor, nil
}

// TelemetryStore returns the ClickHouse telemetry store. It uses a STICKY
// connection (all queries pinned to one host) instead of the round-robin one:
// the optimistic lease needs read-your-writes, which only holds on a single
// replica (see routing.Conn.Sticky). The store runs background lease-refresh
// goroutines, so it is owned by the backend and closed by CloseAll.
func (b *Backend) TelemetryStore(ctx context.Context) (telemetry.Store, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.routing == nil {
		return nil, ErrNotStarted
	}

	if b.telemetryStore != nil {
		return b.telemetryStore, nil
	}

	store, err := telemetrystore.New(
		ctx, b.routing.Sticky(), b.options.ClusterName, b.options.ReadOnly, b.options.TelemetryTiming,
		b.logger.With().Str("component", "telemetry").Logger())
	if err != nil {
		return nil, err
	}

	b.telemetryStore = store

	return b.telemetryStore, nil
}

// StateStore returns the generic ClickHouse key/value state store. It uses a
// STICKY connection (pinned to one host) so a writer reads back its own writes —
// e.g. the migration checkpoint on restart; across replicas a just-written value
// may lag until replication. The first user is the Cassandra→ClickHouse
// migration checkpoint.
func (b *Backend) StateStore(context.Context) (types.State, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.routing == nil {
		return nil, ErrNotStarted
	}

	if b.stateStore == nil {
		b.stateStore = chstate.New(b.routing.Sticky(), b.options.ReadOnly)
	}

	return b.stateStore, nil
}

// dropData drops every ClickHouse table SquirrelDB owns (TSDB, mutable labels,
// telemetry). It refuses any database other than TestDatabase. The database
// itself is left intact (it is created by ClickHouse, not by us). It runs before
// Start opens routing, so it opens its own short-lived connection. It is only
// reached through the DropDataOnStartup test option.
func (b *Backend) dropData(ctx context.Context) error {
	if b.options.Config.Database != TestDatabase {
		return fmt.Errorf("refuse to drop tables in non-test database %q", b.options.Config.Database)
	}

	var conn driver.Conn

	if b.routing == nil {
		c, err := openConn(b.options.Config)
		if err != nil {
			return fmt.Errorf("clickhouse: open connection: %w", err)
		}

		defer c.Close()

		conn = c
	} else {
		conn = b.routing
	}

	// Drop in REVERSE of the creation order so the materialized view goes before
	// its source/target tables. DROP TABLE works on the view and the Null table
	// too. ON CLUSTER (when clustered) drops on every node, matching creation.
	//
	// SYNC forces a synchronous drop: with the Atomic database engine a plain DROP
	// is deferred (database_atomic_delay_before_drop_table_sec, 480s by default)
	// and the replica's Keeper path is freed only then — so an immediate re-CREATE
	// (our name-based path) would hit REPLICA_ALREADY_EXISTS. SYNC frees Keeper now.
	onCluster := ddl.OnCluster(b.options.ClusterName)
	objects := allSchemaObjects(b.options)

	for _, row := range slices.Backward(objects) {
		stmt := "DROP TABLE IF EXISTS " + row.Name + onCluster + " SYNC"
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clickhouse: %s: %w", stmt, err)
		}
	}

	return nil
}

// CloseAll stops every store using the connection (TSDB flushes its batch,
// telemetry stops its refresh loops) then closes the shared connection.
func (b *Backend) CloseAll(ctx context.Context) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.tsdb != nil {
		if err := b.tsdb.Close(ctx); err != nil {
			b.logger.Warn().Msgf("unable to stop TSDB: %v", err.Error())
		}

		b.tsdb = nil
	}

	b.mutableStore = nil
	b.mutableProvider = nil
	b.mutableProcessor = nil

	// Stop the telemetry refresh goroutines before closing the connection they
	// use.
	if b.telemetryStore != nil {
		_ = b.telemetryStore.Close()
		b.telemetryStore = nil
	}

	if b.routing != nil {
		_ = b.routing.Close()
		b.routing = nil
	}
}

// RegisterHTTPEndpoints registers the ClickHouse-specific debug endpoints. The
// caller (the API) prepends a per-backend prefix (e.g. /debug/clickhouse), so the
// paths here are relative and never collide with another backend's.
func (b *Backend) RegisterHTTPEndpoints(register func(method, path, description string, handler http.HandlerFunc)) {
	register(http.MethodGet, "/host",
		"Per-host health (reachable/ready) used for ClickHouse routing.", b.hostsHandler)
	register(http.MethodPost, "/create_tables",
		"Force CREATE ... IF NOT EXISTS ON CLUSTER for every table — re-attaches a rebuilt node "+
			"AFTER its stale replica was dropped (run SYSTEM DROP REPLICA first; see recovery runbook).",
		b.createTablesHandler)
}

// hostsHandler reports the per-host health view used for routing.
func (b *Backend) hostsHandler(w http.ResponseWriter, _ *http.Request) {
	b.l.Lock()
	routingConn := b.routing
	b.l.Unlock()

	if routingConn == nil {
		http.Error(w, "ClickHouse backend not started", http.StatusServiceUnavailable)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(routingConn.Hosts()); err != nil {
		b.logger.Warn().Err(err).Msg("write hosts response")
	}
}

// createTablesHandler force-(re)creates the schema (CREATE ... IF NOT EXISTS ON
// CLUSTER for every table). It re-attaches a rebuilt node after its stale replica
// was dropped — it does NOT drop replicas itself; if a replica is still
// registered in Keeper it stops and returns the SYSTEM DROP REPLICA runbook.
func (b *Backend) createTablesHandler(w http.ResponseWriter, r *http.Request) {
	if b.options.ReadOnly {
		fmt.Fprintln(w, "read-only mode: not creating tables (they are assumed to already exist)")

		return
	}

	if err := b.EnsureSchema(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	fmt.Fprintln(w, "ClickHouse tables created/verified")
}

// EnsureSchema force-(re)creates every ClickHouse table. Unlike the startup path
// (which skips existing tables), it runs CREATE ... IF NOT EXISTS ON CLUSTER for
// all of them, re-attaching a rebuilt node after its stale replica was dropped.
// A REPLICA_ALREADY_EXISTS is turned into an actionable SYSTEM DROP REPLICA hint.
func (b *Backend) EnsureSchema(ctx context.Context) error {
	b.l.Lock()
	conn := b.routing
	b.l.Unlock()

	if conn == nil {
		return ErrNotStarted
	}

	err := ddl.CreateTables(ctx, conn, b.logger, b.schemaObjects())

	if _, ok := errors.AsType[*ddl.ReplicaExistsError](err); ok {
		return fmt.Errorf("%w\n\n%s", err, b.dropReplicaRunbook())
	}

	return err
}

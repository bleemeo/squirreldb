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
	"errors"
	"fmt"
	"sync"
	"time"

	chmutable "github.com/bleemeo/squirreldb/clickhouse/mutable"
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
	ReadOnly          bool
	Cluster           types.Cluster
	TenantLabel       string
	BatchSize         time.Duration
	DefaultTimeToLive time.Duration
}

type Backend struct {
	options Options
	logger  zerolog.Logger

	l                sync.Mutex
	conn             driver.Conn
	mutableStore     mutable.Store
	mutableProvider  *mutable.Provider
	mutableProcessor *mutable.LabelProcessor
	telemetryStore   *telemetrystore.Store
	tsdb             *tsdb.ClickHouseTSDB
}

// New returns a new unstarted backend.
func New(_ context.Context, logger zerolog.Logger, options Options) (*Backend, error) {
	return &Backend{options: options, logger: logger}, nil
}

// openConn opens a ClickHouse connection from the configuration (no Ping).
func openConn(cfg config.ClickHouse) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
	})
}

// Start opens (and pings) the shared connection.
func (b *Backend) Start(ctx context.Context) error {
	b.l.Lock()
	defer b.l.Unlock()

	if b.conn != nil {
		return ErrAlreadyStarted
	}

	conn, err := openConn(b.options.Config)
	if err != nil {
		return fmt.Errorf("clickhouse: open connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()

		return fmt.Errorf("clickhouse: ping: %w", err)
	}

	b.conn = conn

	return nil
}

// TSDB returns the (single, cached) ClickHouse TSDB, sharing the backend connection.
func (b *Backend) TSDB(ctx context.Context) (types.TSDB, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.conn == nil {
		return nil, ErrNotStarted
	}

	if b.tsdb != nil {
		return b.tsdb, nil
	}

	t, err := tsdb.New(
		ctx,
		b.conn,
		b.logger.With().Str("component", "tsdb").Logger(),
		tsdb.Options{
			Redis:             b.options.Redis,
			Reg:               b.options.MetricRegistry,
			TenantLabel:       b.options.TenantLabel,
			BatchSize:         b.options.BatchSize,
			DefaultTimeToLive: b.options.DefaultTimeToLive,
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
	if b.conn == nil {
		return nil, nil, ErrNotStarted
	}

	if b.mutableProvider != nil {
		return b.mutableProvider, b.mutableProcessor, nil
	}

	if b.mutableStore == nil {
		store, err := chmutable.NewStore(ctx, b.conn, chmutable.Options{
			ReadOnly: b.options.ReadOnly,
			Logger:   b.logger.With().Str("component", "mutable-labels").Logger(),
		})
		if err != nil {
			return nil, nil, err
		}

		b.mutableStore = store
	}

	provider, processor := mutable.NewProviderAndProcessor(
		ctx,
		b.options.MetricRegistry,
		b.options.Cluster,
		b.mutableStore,
		b.options.TenantLabel,
		b.logger.With().Str("component", "label_provider").Logger(),
	)

	b.mutableProvider = provider
	b.mutableProcessor = processor

	return provider, processor, nil
}

// TelemetryStore returns the ClickHouse telemetry store, sharing the backend
// connection. The store runs background lease-refresh goroutines, so it is
// owned by the backend and closed by CloseAll (before the connection).
func (b *Backend) TelemetryStore(ctx context.Context) (telemetry.Store, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.conn == nil {
		return nil, ErrNotStarted
	}

	if b.telemetryStore != nil {
		return b.telemetryStore, nil
	}

	store, err := telemetrystore.New(ctx, b.conn, b.logger.With().Str("component", "telemetry").Logger())
	if err != nil {
		return nil, err
	}

	b.telemetryStore = store

	return b.telemetryStore, nil
}

// DropData drops every ClickHouse table SquirrelDB owns (TSDB, mutable labels,
// telemetry). It refuses unless the configured database is TestDatabase, or
// forceNonTestDatabase is set — the same guard as the Cassandra backend's
// keyspace drop. The database itself is left intact (it is created by
// ClickHouse, not by us). It can be called before Start (it opens its own
// short-lived connection when the backend is not started).
func (b *Backend) DropData(ctx context.Context, forceNonTestDatabase bool) error {
	if b.options.Config.Database != TestDatabase && !forceNonTestDatabase {
		return fmt.Errorf(
			"refuse to drop tables in database %q without forceNonTestDatabase", b.options.Config.Database)
	}

	conn := b.conn
	if conn == nil {
		c, err := openConn(b.options.Config)
		if err != nil {
			return fmt.Errorf("clickhouse: open connection: %w", err)
		}

		defer c.Close()

		conn = c
	}

	// mv_datapoint_data reads datapoint_input and writes tsdb_data, so drop it
	// first. IF EXISTS makes the order otherwise irrelevant and the call idempotent.
	statements := []string{
		"DROP VIEW IF EXISTS mv_datapoint_data",
		"DROP TABLE IF EXISTS datapoint_input",
		"DROP TABLE IF EXISTS tsdb_data",
		"DROP TABLE IF EXISTS tsdb_tags",
		"DROP TABLE IF EXISTS mutable_label_names",
		"DROP TABLE IF EXISTS mutable_label_values",
		"DROP TABLE IF EXISTS telemetry_cluster_id",
		"DROP TABLE IF EXISTS telemetry_node_ids",
	}

	for _, stmt := range statements {
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

	if b.conn != nil {
		_ = b.conn.Close()
		b.conn = nil
	}
}

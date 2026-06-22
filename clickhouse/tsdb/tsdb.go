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

// Package tsdb implements a SquirrelDB TSDB backend backed by ClickHouse.
//
// It is selected through the "clickhouse" value of the internal.tsdb option and
// is an alternative to the Cassandra backend.
//
// Writes go through the shared batcher (batch.Batch): points are accumulated by
// label hash and flushed periodically to ClickHouse via SQL (a Null-engine input
// table + materialized views). The batcher works by metric ID and drops the
// labels, so the labels are stored in the temporary store (keyed by the same
// hash) to be rebuilt at flush time, including after a takeover by another node.
// New series are eagerly registered in tsdb_tags so queries resolve them (and
// merge their not-yet-flushed points) before the next flush.
//
// Reads (Querier) resolve the matching series from tsdb_tags and stream their
// points from tsdb_data, merging in the points still buffered in the batcher.
package tsdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/batch"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy/temporarystore"
	"github.com/bleemeo/squirreldb/queryable/convert"
	redisTemporarystore "github.com/bleemeo/squirreldb/redis/temporarystore"
	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

// labelsTTLMargin is added on top of the batch size for the lifetime of the
// id -> labels mapping in the temporary store. It must cover the flush deadline
// (~batch size) plus the takeover window of a dead node, so the labels are
// still available when its points are flushed by another node.
const labelsTTLMargin = time.Hour

// Options carries the configuration needed to build a ClickHouseTSDB.
type Options struct {
	Config            config.ClickHouse
	Redis             config.Redis // If unset, an in-memory store is used for batching.
	Reg               prometheus.Registerer
	TenantLabel       string
	BatchSize         time.Duration
	DefaultTimeToLive time.Duration
}

// ClickHouseTSDB is a types.TSDB backed by ClickHouse.
type ClickHouseTSDB struct {
	opts           Options
	conn           driver.Conn
	temporaryStore batch.TemporaryStore
	batcher        *batch.Batch
	labelsTTL      time.Duration
	metrics        *metrics
	logger         zerolog.Logger

	// seenSeries caches the labels hashes registered in tsdb_tags, to avoid
	// re-inserting them on every write. Entries track last-seen (for eviction)
	// and next-register (for periodic refresh); swept after seenSeriesTTL.
	seenSeries map[uint64]seenEntry
	seenMu     sync.Mutex

	wg     sync.WaitGroup
	cancel context.CancelFunc

	// queries tracks in-flight queriers (each holds the shared conn); Close waits
	// for them before closing the conn. closeMu guards closed so no querier is
	// registered once Close has started (which would race queries.Add/Wait).
	queries sync.WaitGroup
	closeMu sync.Mutex
	closed  bool
}

// TestDatabase is the ClickHouse database used by tests. DropTables refuses to
// drop tables in any other database, to guard against wiping production data.
const TestDatabase = "squirreldb_test"

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

// DropTables drops the TSDB tables (and the input materialized view). It refuses
// unless the configured database is TestDatabase, or forceNonTestDatabase is set
// — the same guard as the Cassandra backend's keyspace drop. The database itself
// is left intact (it is created by ClickHouse, not by us).
func DropTables(ctx context.Context, cfg config.ClickHouse, forceNonTestDatabase bool) error {
	if cfg.Database != TestDatabase && !forceNonTestDatabase {
		return fmt.Errorf("refuse to drop tables in database %q without forceNonTestDatabase", cfg.Database)
	}

	conn, err := openConn(cfg)
	if err != nil {
		return fmt.Errorf("clickhouse: open connection: %w", err)
	}

	defer conn.Close()

	// mv_datapoint_data reads datapoint_input and writes tsdb_data, so drop it
	// first. IF EXISTS makes the order otherwise irrelevant and the call idempotent.
	statements := []string{
		"DROP VIEW IF EXISTS mv_datapoint_data",
		"DROP TABLE IF EXISTS datapoint_input",
		"DROP TABLE IF EXISTS tsdb_data",
		"DROP TABLE IF EXISTS tsdb_tags",
	}

	for _, stmt := range statements {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clickhouse: %s: %w", stmt, err)
		}
	}

	return nil
}

// New connects to ClickHouse, creates the schema if needed, sets up the write
// path (batcher + SQL writer) and returns a ready ClickHouseTSDB.
func New(ctx context.Context, logger zerolog.Logger, opts Options) (*ClickHouseTSDB, error) {
	conn, err := openConn(opts.Config)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}

	c := &ClickHouseTSDB{
		opts:       opts,
		conn:       conn,
		labelsTTL:  opts.BatchSize + labelsTTLMargin,
		metrics:    newMetrics(opts.Reg),
		logger:     logger,
		seenSeries: make(map[uint64]seenEntry),
	}

	if err := c.createSchema(ctx); err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("clickhouse: create schema: %w", err)
	}

	c.temporaryStore, err = newTemporaryStore(ctx, opts, logger)
	if err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("clickhouse: %w", err)
	}

	c.batcher = batch.New(
		opts.Reg,
		opts.BatchSize,
		c.temporaryStore,
		logger.With().Str("component", "batch").Logger(),
	)

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.cancel = cancel

	c.wg.Go(func() {
		c.batcher.Run(runCtx, &sqlWriter{c: c})
	})

	// Evict stale entries from the eager-registration cache.
	c.wg.Go(func() {
		c.runSeenSweeper(runCtx)
	})

	return c, nil
}

// newTemporaryStore returns a Redis-backed store when Redis is configured, or an
// in-memory store otherwise. The store is owned by the batcher (closed by it).
//
// When Redis is configured it MUST succeed: we do not fall back to the in-memory
// store on a Redis error. The in-memory store is node-local, so a silent fallback
// would make a node's buffered labels and points invisible to the node that takes
// over on failure — silent data loss on failover. Fail loudly instead (the
// Cassandra backend has the same contract).
func newTemporaryStore(ctx context.Context, opts Options, logger zerolog.Logger) (batch.TemporaryStore, error) {
	storeLogger := logger.With().Str("component", "temporary_store").Logger()

	if len(opts.Redis.Addresses) > 0 && opts.Redis.Addresses[0] != "" {
		store, err := redisTemporarystore.New(
			ctx,
			opts.Reg,
			redisTemporarystore.Options{
				RedisOptions: opts.Redis,
				Keyspace:     opts.Redis.Keyspace,
			},
			storeLogger,
		)
		if err != nil {
			return nil, fmt.Errorf("redis temporary store: %w", err)
		}

		return store, nil
	}

	return temporarystore.New(opts.Reg, storeLogger), nil //nolint:contextcheck
}

func (c *ClickHouseTSDB) AppenderWithOptions(
	ctx context.Context,
	opts types.AppenderOptions,
) storage.Appender {
	return &appender{
		c:                 c,
		ttl:               opts.WriteTTL,
		ctx:               ctx,
		pendingTimeSeries: make(map[uint64]types.Timeseries),
	}
}

func (c *ClickHouseTSDB) QuerierWithOptions(
	_ types.QuerierOptions,
	mint int64,
	maxt int64,
) (storage.Querier, error) {
	return c.newSQLQuerier(mint, maxt)
}

func (c *ClickHouseTSDB) ChunkQuerierWithOptions(
	_ types.QuerierOptions,
	mint int64,
	maxt int64,
) (storage.ChunkQuerier, error) {
	q, err := c.newSQLQuerier(mint, maxt)
	if err != nil {
		return nil, err
	}

	return convert.NewQuerierToChunkQuerier(q), nil
}

// newSQLQuerier builds a querier and registers it as in-flight so Close waits
// for it to release the shared connection. The matching queries.Done runs in
// sqlQuerier.Close (called by Prometheus once the querier is no longer used).
// It is refused once Close has started, so the connection is never used after it.
func (c *ClickHouseTSDB) newSQLQuerier(mint, maxt int64) (*sqlQuerier, error) {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closed {
		return nil, errClosed
	}

	c.queries.Add(1)

	return &sqlQuerier{
		labelQuerier: c.newLabelQuerier(mint, maxt),
		batcher:      c.batcher,
		metrics:      c.metrics,
		onClose:      c.queries.Done,
	}, nil
}

func (c *ClickHouseTSDB) newLabelQuerier(mint, maxt int64) labelQuerier {
	return labelQuerier{
		conn:        c.conn,
		tenantLabel: c.opts.TenantLabel,
		mint:        mint,
		maxt:        maxt,
	}
}

// Flush flushes points still buffered in the batcher to ClickHouse.
func (c *ClickHouseTSDB) Flush(ctx context.Context) error {
	return c.batcher.Flush(ctx)
}

func (c *ClickHouseTSDB) Close(ctx context.Context) error {
	// Stop registering new queriers, so queries.Wait below cannot race a late Add.
	c.closeMu.Lock()
	c.closed = true
	c.closeMu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	// The batcher does a final flush and closes the temporary store on shutdown.
	c.wg.Wait()

	// Wait for in-flight queries to release the shared connection before closing
	// it, bounded by the shutdown context so a stuck query can't block forever.
	if !c.waitInflightQueries(ctx) {
		c.logger.Warn().Msg("Closing ClickHouse connection while queries are still in flight")
	}

	return c.conn.Close()
}

// waitInflightQueries blocks until every in-flight querier has been closed, or
// the context is done. It returns true if all queries finished, false on timeout.
func (c *ClickHouseTSDB) waitInflightQueries(ctx context.Context) bool {
	done := make(chan struct{})

	go func() {
		c.queries.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

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

	// seenSeries caches the labels hashes already registered in tsdb_tags (with
	// their last-seen time), to avoid re-inserting them on every write. Entries
	// are refreshed on write and swept after seenSeriesTTL.
	seenSeries map[uint64]time.Time
	seenMu     sync.Mutex

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// New connects to ClickHouse, creates the schema if needed, sets up the write
// path (batcher + SQL writer) and returns a ready ClickHouseTSDB.
func New(ctx context.Context, logger zerolog.Logger, opts Options) (*ClickHouseTSDB, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: opts.Config.Addresses,
		Auth: clickhouse.Auth{
			Database: opts.Config.Database,
			Username: opts.Config.Username,
			Password: opts.Config.Password,
		},
	})
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
		seenSeries: make(map[uint64]time.Time),
	}

	if err := c.createSchema(ctx); err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("clickhouse: create schema: %w", err)
	}

	c.temporaryStore = newTemporaryStore(ctx, opts, logger)
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
func newTemporaryStore(ctx context.Context, opts Options, logger zerolog.Logger) batch.TemporaryStore {
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
		if err == nil {
			return store
		}

		logger.Err(err).Msg("Failed to use Redis temporary store, falling back to in-memory store")
	}

	return temporarystore.New(opts.Reg, storeLogger) //nolint:contextcheck
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
	return c.newSQLQuerier(mint, maxt), nil
}

func (c *ClickHouseTSDB) ChunkQuerierWithOptions(
	_ types.QuerierOptions,
	mint int64,
	maxt int64,
) (storage.ChunkQuerier, error) {
	return convert.NewQuerierToChunkQuerier(c.newSQLQuerier(mint, maxt)), nil
}

func (c *ClickHouseTSDB) newSQLQuerier(mint, maxt int64) *sqlQuerier {
	return &sqlQuerier{
		labelQuerier: c.newLabelQuerier(),
		batcher:      c.batcher,
		metrics:      c.metrics,
		mint:         mint,
		maxt:         maxt,
	}
}

func (c *ClickHouseTSDB) newLabelQuerier() labelQuerier {
	return labelQuerier{
		conn:        c.conn,
		tenantLabel: c.opts.TenantLabel,
	}
}

// Flush flushes points still buffered in the batcher to ClickHouse.
func (c *ClickHouseTSDB) Flush(ctx context.Context) error {
	return c.batcher.Flush(ctx)
}

func (c *ClickHouseTSDB) Close(context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}

	// The batcher does a final flush and closes the temporary store on shutdown.
	c.wg.Wait()

	return c.conn.Close()
}

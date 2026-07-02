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
// It is selected through the "clickhouse" value of the "backend" option and
// is an alternative to the Cassandra backend.
//
// Writes go through a shared Redis WAL (write-ahead log): each Commit appends its
// series (labels + points) to a single Redis list and returns — durable as soon as
// it is in (HA) Redis. One node in the cluster (the drainer, elected via a Redis
// lease) periodically writes the head of the WAL to ClickHouse in a few large
// arrays-per-series inserts (registering the series in tsdb_tags and inserting the
// points into datapoint_input, whose materialized view fans them into tsdb_data)
// then trims it. This is a deliberate fit for ClickHouse (few large global inserts,
// no per-metric ownership/takeover) at the cost of read-your-writes: a just-written
// point becomes queryable within ~the flush delay.
//
// Reads (Querier) resolve the matching series from tsdb_tags and stream their
// points from tsdb_data; there is no read-merge of not-yet-drained points.
package tsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

const (
	// defaultWALFlushDelay is how often the drainer writes the WAL to ClickHouse
	// when it is below the soft threshold (config wal_flush_delay overrides it).
	defaultWALFlushDelay = 5 * time.Second
	// defaultWALSoftThreshold is the WAL length, in POINTS, above which the drainer
	// flushes early instead of waiting for the delay (config wal_soft_threshold
	// overrides it). One WAL entry is one point, so the list length IS the point
	// count — an exact, free bound (LLEN) with no side counter. With the factors
	// below the producer starts slowing at 3× and blocks at 10× this. Each buffered
	// point carries its labels (~150-200 B), so 200k points ≈ 35 MiB and the 10×
	// block ≈ 2M points ≈ 350 MiB of Redis. Tuned against real workloads / Redis
	// capacity.
	defaultWALSoftThreshold = 200_000 // points

	// walCriticalFactor and walStopFactor derive the producer-backpressure
	// thresholds from the soft threshold: at critical (× soft) a producer starts
	// sleeping before appending, at stop (× soft) it blocks until the WAL drains.
	// Both are well above soft so a healthy drainer (keeping the WAL near soft)
	// never triggers backpressure.
	walCriticalFactor = 3
	walStopFactor     = 10

	// walMaxSleep caps the backpressure sleep (reached at the stop threshold).
	walMaxSleep = time.Second
	// walBlockPollInterval is how often a blocked producer re-checks the WAL length.
	walBlockPollInterval = 100 * time.Millisecond
	// walLeaseTTL is the drainer lease lifetime; it is refreshed every
	// drainPollInterval, so a dead leader is replaced within ~this delay.
	walLeaseTTL = 20 * time.Second
	// walInsertMaxPoints caps the points in one ClickHouse insert (the drainer
	// loops over several inserts to drain a large WAL).
	walInsertMaxPoints = 500_000
)

// Options carries the configuration needed to build a ClickHouseTSDB.
type Options struct {
	// ClusterName, when set, switches the schema to ClickHouse cluster mode
	// (ON CLUSTER + ReplicatedMergeTree with an explicit, name-based Keeper
	// path; the tables must not be RENAMEd). Empty means a single node.
	ClusterName string
	// Redis holds the write-ahead log shared by every node (durable, HA). When
	// unset, the WAL falls back to an in-process, single-node store with reduced
	// durability (see newWALClient / memWALClient) — like the Cassandra backend.
	Redis             config.Redis
	Reg               prometheus.Registerer
	TenantLabel       string
	DefaultTimeToLive time.Duration
	// WALFlushDelay / WALSoftThreshold override the drain cadence and the early-
	// flush / backpressure thresholds (WALSoftThreshold is in POINTS); 0 uses the
	// defaults above.
	WALFlushDelay    time.Duration
	WALSoftThreshold int
}

// ClickHouseTSDB is a types.TSDB backed by ClickHouse.
type ClickHouseTSDB struct {
	opts    Options
	conn    driver.Conn
	walConn walClient // owns the WAL/lease connection (closed here); Redis or in-memory
	wal     *walStore
	drainer *walDrainer // periodic drain loop; also flushes the WAL on Close
	metrics *metrics
	logger  zerolog.Logger

	// walCritical/walStop are the backpressure thresholds (points); lastWALLen is
	// the WAL length (points) seen at the last append, used to decide the NEXT
	// append's backpressure without an extra Redis round-trip.
	walCritical int
	walStop     int
	lastWALLen  atomic.Int64

	// seenSeries caches the labels hashes registered in tsdb_tags, to avoid
	// re-inserting them on every drain. Entries track last-seen (for eviction)
	// and next-register (for periodic refresh); swept after seenSeriesTTL.
	seenSeries map[uint64]seenEntry
	seenMu     sync.Mutex

	wg     sync.WaitGroup
	cancel context.CancelFunc

	// queries tracks in-flight queriers (each holds the shared conn); Close waits
	// for them before returning. closeMu guards closed so no querier is registered
	// once Close has started (which would race queries.Add/Wait).
	queries sync.WaitGroup
	closeMu sync.Mutex
	closed  bool
}

// New sets up the write path (WAL + drainer) and returns a ready ClickHouseTSDB.
// The WAL is Redis-backed when Redis is configured (durable, HA) and in-process
// otherwise (see newWALClient). The ClickHouse connection is owned by the caller
// (clickhouse/backend) and closed by it; the WAL connection is owned here.
func New(ctx context.Context, conn driver.Conn, logger zerolog.Logger, opts Options) (*ClickHouseTSDB, error) {
	flushDelay := opts.WALFlushDelay
	if flushDelay <= 0 {
		flushDelay = defaultWALFlushDelay
	}

	soft := opts.WALSoftThreshold
	if soft <= 0 {
		soft = defaultWALSoftThreshold
	}

	walConn := newWALClient(opts.Redis, logger)
	wal := &walStore{
		client: walConn,
		key:    opts.Redis.Keyspace + "clickhouse_wal",
	}

	c := &ClickHouseTSDB{
		opts:        opts,
		conn:        conn,
		walConn:     walConn,
		wal:         wal,
		metrics:     newMetrics(opts.Reg),
		logger:      logger,
		walCritical: soft * walCriticalFactor,
		walStop:     soft * walStopFactor,
		seenSeries:  make(map[uint64]seenEntry),
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.cancel = cancel

	c.drainer = &walDrainer{
		wal:        wal,
		leaseKey:   opts.Redis.Keyspace + "clickhouse_wal_lease",
		owner:      uuid.NewString(),
		leaseTTL:   walLeaseTTL,
		flushDelay: flushDelay,
		soft:       soft,
		maxPoints:  walInsertMaxPoints,
		write:      c.drainWrite,
		metrics:    c.metrics,
		logger:     logger.With().Str("component", "wal-drainer").Logger(),
	}

	c.wg.Go(func() {
		c.drainer.run(runCtx)
	})

	// Evict stale entries from the registration cache.
	c.wg.Go(func() {
		c.runSeenSweeper(runCtx)
	})

	return c, nil
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

// newSQLQuerier builds a querier and registers it as in-flight so Close waits for
// it to release the shared connection. The matching queries.Done runs in
// sqlQuerier.Close. It is refused once Close has started, so the connection is
// never used after it.
func (c *ClickHouseTSDB) newSQLQuerier(mint, maxt int64) (*sqlQuerier, error) {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closed {
		return nil, errClosed
	}

	c.queries.Add(1)

	return &sqlQuerier{
		labelQuerier: c.newLabelQuerier(mint, maxt),
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

// applyBackpressure slows or blocks the caller before it appends to the WAL, based
// on the length seen at the previous append (see backpressureDelay). Applying it
// BEFORE the append means a context timeout during the wait leaves nothing written
// (no duplicate on the client's retry). It returns ctx.Err() if the wait is
// cancelled.
func (c *ClickHouseTSDB) applyBackpressure(ctx context.Context) error {
	delay, block := backpressureDelay(c.lastWALLen.Load(), c.walCritical, c.walStop, walMaxSleep)

	if block {
		c.metrics.WALBackpressureTotal.WithLabelValues("block").Inc()
		c.logger.Warn().Int("stop_points", c.walStop).
			Msg("ClickHouse WAL reached the stop threshold; blocking writes until it drains")

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(walBlockPollInterval):
			}

			n, err := c.wal.Len(ctx)
			if err != nil {
				return err
			}

			c.lastWALLen.Store(n)

			if n < int64(c.walStop) {
				return nil
			}
		}
	}

	if delay > 0 {
		c.metrics.WALBackpressureTotal.WithLabelValues("slow").Inc()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return nil
}

// Flush is a no-op: a write is buffered in the WAL as soon as Commit returns and
// drained to ClickHouse asynchronously by the drainer (a clean shutdown drains what
// remains, see Close). It does not force a synchronous drain here because that would
// race the running periodic drainer on the WAL head.
func (c *ClickHouseTSDB) Flush(_ context.Context) error {
	return nil
}

func (c *ClickHouseTSDB) Close(ctx context.Context) error {
	// Stop registering new queriers, so queries.Wait below cannot race a late Add.
	c.closeMu.Lock()
	c.closed = true
	c.closeMu.Unlock()

	// Wait for in-flight queries to finish FIRST, while the periodic drainer is still
	// running. Any work still in flight can keep feeding the WAL, and the running
	// drainer keeps absorbing it, so we don't stop draining until the write side is
	// quiet. Bounded by the shutdown context so a stuck query can't block forever.
	if !c.waitInflightQueries(ctx) {
		c.logger.Warn().Msg("Stopping ClickHouse TSDB while queries are still in flight")
	}

	// Now that nothing is producing points, stop the periodic drainer and the
	// sweeper.
	if c.cancel != nil {
		c.cancel()
	}

	c.wg.Wait()

	// Flush whatever is still buffered to ClickHouse, so a clean shutdown leaves the
	// data only in ClickHouse. This is essential for the in-memory WAL (its buffer
	// would otherwise be lost with the process) and a harmless backlog-reducer for
	// Redis. It also releases the drainer lease so a takeover is immediate. Bounded
	// by the shutdown context, and safe to run now: external writes are already
	// stopped and the ClickHouse connection is closed by the backend (its owner)
	// only after this returns.
	if c.drainer != nil {
		c.drainer.finalDrain(ctx)
	}

	// The WAL connection is ours.
	c.walConn.Close()

	return nil
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

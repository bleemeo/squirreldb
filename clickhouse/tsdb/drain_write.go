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

package tsdb

import (
	"context"
	"fmt"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

// seriesBatch is all points of one series within a drain batch, regrouped from the
// per-point WAL entries so the ClickHouse insert is one arrays-per-series row (not
// one row per point). ttl is the largest seen for the series (never under-estimate
// max_expiration/row TTL, which would expire data or its index row early). key is
// the canonical labels encoding (see seriesKey), computed once here and carried
// through registration so the seen-cache passes don't recompute it.
type seriesBatch struct {
	labels labels.Labels
	key    string
	ttl    int64
	points []types.MetricPoint
}

// seriesKey is the canonical map key for a series: the labels encoding
// (labels.Bytes), NOT labels.Hash(). The WAL is shared across tenants, so an
// attacker-forced xxHash collision (xxHash is invertible) would otherwise merge a
// victim tenant's series into the attacker's. A string key lets Go's seeded map
// hashing bucket the series, and byte equality keeps distinct series distinct.
// Bytes, not String: String is not injective for adversarial UTF-8 label names.
func seriesKey(l labels.Labels) string {
	return string(l.Bytes(nil))
}

// drainWrite persists a batch of WAL entries to ClickHouse. Each entry is one point;
// it first regroups them by series, then registers the distinct series in tsdb_tags
// (new/due ones only) and inserts the points into datapoint_input (one
// arrays-per-series row; the MV fans them into tsdb_data). It is the walDrainer's
// write function.
func (c *ClickHouseTSDB) drainWrite(ctx context.Context, entries []walEntry) error {
	return c.writeBatch(ctx, entries, c.registerSeries)
}

// writeBatch regroups the per-point entries by series, then hands them to
// writeSeries. The live drain path uses this; the bulk import builds its
// seriesBatch list directly (one chunk = one series, already arrays-per-series)
// and calls writeSeries, skipping the regroup copy.
func (c *ClickHouseTSDB) writeBatch(
	ctx context.Context,
	entries []walEntry,
	register func(context.Context, []registration) error,
) error {
	if len(entries) == 0 {
		return nil
	}

	return c.writeSeries(ctx, groupBySeries(entries), register, 0)
}

// writeSeries registers the given series via the supplied function
// (registerSeries for the live drain — new/due only; or writeAllRegistrations
// for the bulk import — every series, every time), then inserts their points
// into datapoint_input (one arrays-per-series row; the MV fans them into
// tsdb_data).
//
// flushEveryPoints, when > 0, streams the insert in intermediate blocks of about
// that many points instead of one block for the whole call (see the flush inside
// the loop). 0 keeps a single block — used by the live drain, whose batches are
// already small.
func (c *ClickHouseTSDB) writeSeries(
	ctx context.Context,
	series []*seriesBatch,
	register func(context.Context, []registration) error,
	flushEveryPoints int,
) error {
	if len(series) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		c.metrics.FlushSeconds.Observe(time.Since(start).Seconds())
	}()

	if err := register(ctx, registrationsFor(series)); err != nil {
		return fmt.Errorf("register series: %w", err)
	}

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO datapoint_input")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	defer batch.Close()

	globalDays := c.ttlDays()

	// One hour-truncated stamp for the whole batch, used as the read-side
	// last-write-wins tiebreaker (see tsdbDataDDL). Stamped here, not via now() in
	// the MV, which would re-evaluate per inserted block.
	insertHour := time.Now().Truncate(time.Hour)

	var (
		rows             int
		pointsSinceFlush int
	)

	for _, s := range series {
		cols := seriesToInput(s.labels, c.opts.TenantLabel, s.ttl, globalDays)

		timestamps := make([]time.Time, len(s.points))
		values := make([]float64, len(s.points))

		for j, p := range s.points {
			timestamps[j] = time.UnixMilli(p.Timestamp)
			values[j] = p.Value
		}

		if err := batch.Append(
			cols.metricName, cols.tenant, cols.tags, timestamps, values, cols.rowTTLDays, insertHour,
		); err != nil {
			_ = batch.Abort()

			c.metrics.FlushErrorsTotal.Inc()

			return fmt.Errorf("append row: %w", err)
		}

		rows += len(s.points)
		pointsSinceFlush += len(s.points)

		// Release our copy of this series' points now that clickhouse-go has copied
		// them into its per-column buffers. clickhouse-go materialises the whole
		// block's columns before Send, so without this the entire source batch would
		// stay live alongside that full columnar copy for the length of the insert.
		// Dropping the reference here lets the source be reclaimed as the columns
		// fill. Safe: registrationsFor(series) above already consumed the points, and
		// nothing below reads them. On the import path this is the caller's batch
		// (see ImportRawData, which hands us ownership); on the drain path they are
		// groupBySeries' throwaway slices.
		s.points = nil

		// Stream the insert in bounded blocks: once enough points have accumulated,
		// send the current block and reset it (block.Reset keeps the column buffers'
		// capacity, so they are reused, not reallocated). This caps clickhouse-go's
		// columnar copy at ~flushEveryPoints instead of the whole batch. All flushed
		// blocks stay in the SAME insert, so the server-side materialized-view squash
		// still coalesces them — parts must be checked, but should stay low.
		if flushEveryPoints > 0 && pointsSinceFlush >= flushEveryPoints {
			if err := batch.Flush(); err != nil {
				c.metrics.FlushErrorsTotal.Inc()

				return fmt.Errorf("flush block: %w", err)
			}

			pointsSinceFlush = 0
		}
	}

	c.metrics.FlushRowsPerBatch.Observe(float64(rows))
	c.metrics.FlushMetricsPerBatch.Observe(float64(len(series)))

	if rows == 0 {
		return batch.Abort()
	}

	if err := batch.Send(); err != nil {
		c.metrics.FlushErrorsTotal.Inc()

		return fmt.Errorf("send batch to clickhouse: %w", err)
	}

	c.metrics.FlushPointsTotal.Add(float64(rows))

	return nil
}

// groupBySeries regroups the per-point WAL entries of a drain batch into one
// seriesBatch per distinct series (its points concatenated, largest TTL kept), so
// the insert stays arrays-per-series regardless of how the points were appended.
func groupBySeries(entries []walEntry) []*seriesBatch {
	// Keyed by the canonical labels encoding (see seriesKey), not labels.Hash().
	bySeries := make(map[string]*seriesBatch)

	for _, e := range entries {
		if len(e.Points) == 0 {
			continue
		}

		key := seriesKey(e.Labels)

		s, ok := bySeries[key]
		if !ok {
			s = &seriesBatch{labels: e.Labels, key: key, ttl: e.TTL}
			bySeries[key] = s
		}

		s.ttl = max(s.ttl, e.TTL)
		s.points = append(s.points, e.Points...)
	}

	out := make([]*seriesBatch, 0, len(bySeries))
	for _, s := range bySeries {
		out = append(out, s)
	}

	return out
}

// registrationsFor derives the tsdb_tags registration input from the regrouped
// series (its point-time range and TTL).
func registrationsFor(series []*seriesBatch) []registration {
	regs := make([]registration, 0, len(series))

	for _, s := range series {
		minTS, maxTS := pointsRange(s.points)
		regs = append(regs, registration{labels: s.labels, key: s.key, ttl: s.ttl, minTS: minTS, maxTS: maxTS})
	}

	return regs
}

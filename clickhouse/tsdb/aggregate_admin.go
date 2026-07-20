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
	"io"
	"net/http"
	"time"
)

// aggBucketExpr maps a raw `timestamp` column to its right-edge 15-min bucket. It
// MUST match the bucket expression in mvDatapointAgg15mDDL (kept in sync by hand,
// like aggBucket and the DDL INTERVAL).
const aggBucketExpr = "toDateTime(toStartOfInterval(timestamp - toIntervalMillisecond(1)," +
	" INTERVAL 15 MINUTE) + toIntervalMinute(15))"

// aggPartition is tsdb_data's PARTITION BY span (7-day, epoch-aligned in UTC).
// Rebuild/diff windows align to it so each query stays within one partition,
// keeping the rows it scans and groups (and thus the memory) bounded per query.
const aggPartition = 7 * 24 * time.Hour

const defaultRebuildChunk = aggPartition

// floorBucket / ceilBucket round a millisecond timestamp to the epoch-based bucket
// grid. Rebuilds work on whole buckets (a bucket can only be recomputed from its
// full interval), so the range is snapped to this grid: from down, to up — by at
// most one bucket. Correct for negative (pre-epoch) values, though data is
// realistically post-epoch.
func floorBucket(ms int64) int64 {
	b := aggBucket.Milliseconds()

	r := ms % b
	if r < 0 {
		r += b
	}

	return ms - r
}

func ceilBucket(ms int64) int64 {
	if f := floorBucket(ms); f != ms {
		return f + aggBucket.Milliseconds()
	}

	return ms
}

// floorPartition / ceilPartition align a millisecond timestamp to the epoch-based
// 7-day partition grid (matching tsdb_data's PARTITION BY). Correct for negative
// (pre-epoch) values too, though data is realistically post-epoch.
func floorPartition(ms int64) int64 {
	p := aggPartition.Milliseconds()

	r := ms % p
	if r < 0 {
		r += p
	}

	return ms - r
}

func ceilPartition(ms int64) int64 {
	if f := floorPartition(ms); f != ms {
		return f + aggPartition.Milliseconds()
	}

	return ms
}

// alignedChunks splits [from, to] into half-open (startMs, endMs] windows of at
// most chunk each (default defaultRebuildChunk). The bounds snap to the bucket
// grid — from down, to up — so the buckets holding from and to are covered whole.
// No window ever crosses a partition boundary (processing one partition per query
// is cheaper), so windows also align on those boundaries — which means the first
// and last windows are often smaller than chunk.
func alignedChunks(from, to time.Time, chunk time.Duration) [][2]int64 {
	if chunk <= 0 {
		chunk = defaultRebuildChunk
	}

	if r := chunk % aggBucket; r != 0 {
		chunk += aggBucket - r
	}

	fromMs := clampStoreTime(floorBucket(from.UnixMilli()))
	toMs := clampStoreTime(ceilBucket(to.UnixMilli()))
	chunkMs := chunk.Milliseconds()

	if fromMs >= toMs {
		return nil
	}

	boundaries := make([]int64, 0, int((toMs-fromMs)/chunkMs))
	for start := fromMs; start < toMs; {
		boundaries = append(boundaries, start)

		nextPartition := ceilPartition(start + 1) // +1: we want *next* partition
		nextStart := min(start+chunkMs, nextPartition)

		start = nextStart
	}

	boundaries = append(boundaries, toMs)

	windows := make([][2]int64, 0, len(boundaries)-1)

	for i := 1; i < len(boundaries); i++ {
		windows = append(windows, [2]int64{boundaries[i-1], boundaries[i]})
	}

	return windows
}

// RebuildAggregate recomputes tsdb_data_agg_15m over [from, to] from the raw
// points, in windows of at most chunk (0 = the default of one week). Rebuilds work
// on whole buckets, so from and to are extended to the enclosing bucket boundaries
// (by up to 15 minutes). `to` must stay behind the live aggregation to avoid
// double-processing the in-progress bucket — with the boundary alignment, that
// means `to` should be older than now - 15 minutes.
//
// It is idempotent and is the single routine behind three needs: backfilling a base
// migrated before the MV existed, correcting a range after a WAL re-drain inflated
// sum/count, and rebuilding after the bucket size changed. It returns the number of
// windows processed.
//
// When drop is true, the pre-aggregated rows are deleted before the insert. The
// range should hold no pre-existing aggregate, so set drop=false only when you can
// otherwise guarantee it (e.g. the table was manually truncated).
//
// When dedup is false it aggregates the raw points directly into buckets, exactly
// like the live MV — one group per (series, bucket), the right choice for a backfill
// or a bucket-size change. When dedup is true it first collapses re-drained
// duplicate (id, timestamp) rows to last-write-wins, producing the canonical
// aggregate that fixes a re-drain drift — but that extra per-timestamp grouping
// raises the memory cost, so reserve it for a bounded range where a diff flagged a
// re-drain.
func (c *ClickHouseTSDB) RebuildAggregate(
	ctx context.Context, writer io.Writer, from, to time.Time, chunk time.Duration, drop, dedup bool,
) (int, error) {
	windows := alignedChunks(from, to, chunk)

	if len(windows) > 0 {
		windowsFrom := time.UnixMilli(windows[0][0])
		windowsTo := time.UnixMilli(windows[len(windows)-1][1])

		fmt.Fprintf(writer, "Will pre-aggregate for [%s, %s], in %d chunks\n",
			windowsFrom.Format(time.RFC3339), windowsTo.Format(time.RFC3339), len(windows))
	} else {
		fmt.Fprintf(writer, "Will pre-aggregate 0 chunks\n")
	}

	if respf, ok := writer.(http.Flusher); ok {
		respf.Flush()
	}

	for i, w := range windows {
		t0 := time.Now()

		if err := c.rebuildChunk(ctx, w[0], w[1], drop, dedup); err != nil {
			return i, fmt.Errorf("clickhouse: rebuild aggregate [%s, %s]: %w",
				time.UnixMilli(w[0]).UTC().Format(time.RFC3339), time.UnixMilli(w[1]).UTC().Format(time.RFC3339), err)
		}

		chunkFrom := time.UnixMilli(w[0])
		chunkTo := time.UnixMilli(w[1])
		chunkDuration := time.Since(t0).Truncate(time.Millisecond)

		fmt.Fprintf(writer, "pre-aggregate chunk for [%s, %s] in %s\n",
			chunkFrom.Format(time.RFC3339), chunkTo.Format(time.RFC3339), chunkDuration.String())

		if respf, ok := writer.(http.Flusher); ok {
			respf.Flush()
		}
	}

	return len(windows), nil
}

// rebuildChunk rebuilds one aligned (fromMs, toMs] window: optionally delete the
// stale aggregate rows, then insert the freshly recomputed ones.
func (c *ClickHouseTSDB) rebuildChunk(ctx context.Context, fromMs, toMs int64, drop, dedup bool) error {
	if drop {
		// mutations_sync = 2 waits for the delete to apply on every replica, so the
		// insert below never races the removal of the rows it is about to replace.
		del := "ALTER TABLE tsdb_data_agg_15m DELETE" +
			" WHERE bucket > fromUnixTimestamp64Milli(?) AND bucket <= fromUnixTimestamp64Milli(?)" +
			" SETTINGS mutations_sync = 2"

		if err := c.conn.Exec(ctx, del, fromMs, toMs); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	ins := rebuildInsertSQL(dedup)

	if err := c.conn.Exec(ctx, ins, fromMs, toMs); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// rebuildInsertSQL returns the INSERT ... SELECT that recomputes the aggregate for
// a (fromMs, toMs] window (two ? bound to those millisecond bounds).
func rebuildInsertSQL(dedup bool) string {
	if !dedup {
		// Aggregate the raw points straight into buckets, like mvDatapointAgg15mDDL.
		// One group per (series, bucket): cheap and scalable.
		return "INSERT INTO tsdb_data_agg_15m " +
			"SELECT id, tenant, " + aggBucketExpr + " AS bucket, " +
			"min(value), max(value), sum(value), count(), max(row_ttl_days) " +
			"FROM tsdb_data " +
			"WHERE timestamp > fromUnixTimestamp64Milli(?) AND timestamp <= fromUnixTimestamp64Milli(?) " +
			"AND NOT isNaN(value) " +
			"GROUP BY tenant, id, bucket"
	}

	// Inner query: collapse re-drained duplicates to last-write-wins per (tenant,
	// id, timestamp), matching the read path (argMax by insert_hour). Outer query:
	// aggregate the deduplicated points into buckets. One group per (series,
	// timestamp) in the inner: ~90× more groups than the bucketed form above.
	//
	// The inner aggregates are aliased v / ttl (NOT value / row_ttl_days): reusing
	// the raw column names would shadow them and ClickHouse then resolves the column
	// in the inner WHERE to the aggregate alias, rejecting the query.
	return "INSERT INTO tsdb_data_agg_15m " +
		"SELECT id, tenant, " + aggBucketExpr + " AS bucket, " +
		"min(v), max(v), sum(v), count(), max(ttl) " +
		"FROM (" +
		"SELECT id, tenant, timestamp, argMax(value, insert_hour) AS v, max(row_ttl_days) AS ttl " +
		"FROM tsdb_data " +
		"WHERE timestamp > fromUnixTimestamp64Milli(?) AND timestamp <= fromUnixTimestamp64Milli(?) " +
		"AND NOT isNaN(value) " +
		"GROUP BY tenant, id, timestamp" +
		") " +
		"GROUP BY tenant, id, bucket"
}

// AggDiffSummary reports, over a bucket range, how far the live aggregate has
// drifted from a fresh recomputation of the raw points — the signature of a WAL
// re-drain double-counting sum/count. DiscrepantBuckets is the number of
// (series, bucket) pairs whose stored count differs from the deduplicated raw
// count; the two totals let the operator gauge the size of the drift.
type AggDiffSummary struct {
	DiscrepantBuckets uint64 `json:"discrepant_buckets"`
	AggCountTotal     uint64 `json:"agg_count_total"`
	RawCountTotal     uint64 `json:"raw_count_total"`
}

// DiffAggregate compares, over [from, to], the stored aggregate count against a
// fresh deduplicated count of the raw points, processing the range in aligned
// windows of chunk each (0 = the default) so a wide range does not trip the memory
// limit. A non-zero DiscrepantBuckets means the live MV over- or under-counted
// (e.g. a WAL re-drain); RebuildAggregate over the same range fixes it. Read-only.
func (c *ClickHouseTSDB) DiffAggregate(
	ctx context.Context, writer io.Writer, from, to time.Time, chunk time.Duration,
) (AggDiffSummary, error) {
	var total AggDiffSummary

	for _, w := range alignedChunks(from, to, chunk) {
		part, err := c.diffChunk(ctx, w[0], w[1])
		if err != nil {
			return AggDiffSummary{}, fmt.Errorf("clickhouse: diff aggregate [%s, %s]: %w",
				time.UnixMilli(w[0]).UTC().Format(time.RFC3339), time.UnixMilli(w[1]).UTC().Format(time.RFC3339), err)
		}

		total.DiscrepantBuckets += part.DiscrepantBuckets
		total.AggCountTotal += part.AggCountTotal
		total.RawCountTotal += part.RawCountTotal

		chunkFrom := time.UnixMilli(w[0])
		chunkTo := time.UnixMilli(w[1])

		fmt.Fprintf(
			writer,
			"aggregate chunk diff for [%s, %s]: DiscrepantBuckets=%d, AggCountTotal=%d vs RawCountTotal=%d\n",
			chunkFrom.Format(time.RFC3339),
			chunkTo.Format(time.RFC3339),
			part.DiscrepantBuckets,
			part.AggCountTotal,
			part.RawCountTotal,
		)

		if respf, ok := writer.(http.Flusher); ok {
			respf.Flush()
		}
	}

	return total, nil
}

// diffChunk computes the drift summary for one aligned (fromMs, toMs] window by
// comparing, per (series, bucket), the stored count against a fresh count of the
// raw points. The per-bucket comparison (not a total) avoids false negatives from
// over/under-counts cancelling out.
//
// raw_count uses uniq() (not uniqExact): a 15-min bucket never holds enough
// distinct timestamps for its HyperLogLog to be inexact, while uniqExact keeps a
// full set per group and blows the memory. tenant leads each GROUP BY (it is
// functionally determined by id) so the join on (id, bucket) is unaffected.
func (c *ClickHouseTSDB) diffChunk(ctx context.Context, fromMs, toMs int64) (AggDiffSummary, error) {
	query := "SELECT countIf(agg_count != raw_count) AS discrepant, " +
		"sum(agg_count) AS agg_total, sum(raw_count) AS raw_total " +
		"FROM (" +
		"SELECT id, bucket, sum(count) AS agg_count " +
		"FROM tsdb_data_agg_15m " +
		"WHERE bucket > fromUnixTimestamp64Milli(?) AND bucket <= fromUnixTimestamp64Milli(?) " +
		"GROUP BY tenant, id, bucket" +
		") a " +
		"FULL OUTER JOIN (" +
		"SELECT id, " + aggBucketExpr + " AS bucket, uniq(timestamp) AS raw_count " +
		"FROM tsdb_data " +
		"WHERE timestamp > fromUnixTimestamp64Milli(?) AND timestamp <= fromUnixTimestamp64Milli(?) " +
		"AND NOT isNaN(value) " +
		"GROUP BY tenant, id, bucket" +
		") r USING (id, bucket)"

	var s AggDiffSummary

	row := c.conn.QueryRow(ctx, query, fromMs, toMs, fromMs, toMs)
	if err := row.Scan(&s.DiscrepantBuckets, &s.AggCountTotal, &s.RawCountTotal); err != nil {
		return AggDiffSummary{}, err
	}

	return s, nil
}

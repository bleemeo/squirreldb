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
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/queryable/limitqueryable"
	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/rs/zerolog"
)

// sqlQuerier serves Select entirely through SQL: it resolves the matching series
// and their labels from tsdb_tags, then streams their points from tsdb_data.
// Recently-written points are visible once the WAL drainer has flushed them
// (~the flush delay); there is no read-merge of un-flushed points.
// LabelValues/LabelNames come from the embedded labelQuerier.
type sqlQuerier struct {
	labelQuerier

	metrics *metrics
	// maxSeries caps how many series a single Select may resolve before it fails
	// with limitqueryable.ErrTooManySeries; 0 means unlimited. It bounds both the
	// resolve memory and the size of the id set pointsQuery inlines (see Select).
	maxSeries uint32
	// maxPoints caps how many points a single read may return before ClickHouse
	// aborts it (max_result_rows, see pointReadContext); 0 means unlimited. It stops
	// a wide raw read (e.g. a small [range] over a huge span routed to raw) from
	// materializing server-side or buffering client-side before limitqueryable —
	// which only counts once the caller iterates — would notice.
	maxPoints uint64
	// onClose releases this querier's in-flight registration (queries.Done); see
	// ClickHouseTSDB.newSQLQuerier. Called once, from Close.
	onClose func()
	// forceRaw pins the read to raw points regardless of the step-based routing (see
	// aggSelection): the ForceRaw request header.
	forceRaw     bool
	requestDebug types.DebugLevel
	logger       zerolog.Logger
}

func (q *sqlQuerier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	// whereWithTime (not buildWhere): resolveSeries must count only series active in
	// [mint, maxt], like the label endpoints. Without it a high-churn tenant's whole
	// history of matching series is resolved and trips maxSeries on a recent-window
	// query, even when almost none have points in range. The same where feeds
	// pointsQuery's subquery fallback, keeping its id set identical to what resolveSeries found.
	where, args := q.whereWithTime(matchers)

	labelsByID, err := q.resolveSeries(ctx, where, args)
	if err != nil {
		// A limit overflow is a client-facing error in its own right (same one
		// limitqueryable raises); don't bury it under a "resolve series" prefix.
		if errors.Is(err, limitqueryable.ErrTooManySeries) {
			return storage.ErrSeriesSet(err)
		}

		return storage.ErrSeriesSet(fmt.Errorf("clickhouse: resolve series: %w", err))
	}

	q.metrics.ReadSeriesTotal.Add(float64(len(labelsByID)))

	if len(labelsByID) == 0 {
		return storage.EmptySeriesSet()
	}

	ids := make([]uuid.UUID, 0, len(labelsByID))
	for id := range labelsByID {
		ids = append(ids, id)
	}

	var (
		query     string
		queryArgs []any
		source    = "raw"
	)

	switch plan := q.aggSelection(hints); plan.mode { //nolint:exhaustive
	case aggModeRange:
		query, queryArgs = q.aggPointsQuery(ids, plan.expr, where, args)
		source = "aggregated"
	case aggModeInstantSnap:
		query, queryArgs = q.aggSnapQuery(ids, hints, where, args)
		source = "aggregated"
	default:
		query, queryArgs = q.pointsQuery(ids, where, args)
	}

	q.metrics.ReadSourceTotal.WithLabelValues(source).Inc()

	if q.requestDebug >= types.DebugLevelSimple {
		fn := ""
		step := int64(0)

		if hints != nil {
			fn, step = hints.Func, hints.Step
		}

		q.logger.Info().Msgf(
			"clickhouse Select: series=%d source=%s func=%q step=%d forceRaw=%v",
			len(labelsByID), source, fn, step, q.forceRaw)
	}

	rows, err := q.conn.Query(q.pointReadContext(ctx), query, queryArgs...)
	if err != nil {
		if mapped, ok := asTooManyPoints(err); ok {
			return storage.ErrSeriesSet(mapped)
		}

		return storage.ErrSeriesSet(fmt.Errorf("clickhouse: read points: %w", err))
	}

	set := &sqlSeriesSet{rows: rows, labelsByID: labelsByID}

	// Drain the cursor into memory before returning, which closes it — and so
	// releases the routing read slot it holds — right here. Were the streaming set
	// returned instead (slot held until the querier closes), a querier that opens
	// several selector cursors would hold one read slot per open cursor at once, and
	// concurrent multi-selector PromQL queries could each take a subset of the read
	// budget then block forever waiting for the rest (a hold-and-wait deadlock,
	// surfacing as a query timeout). PromQL drains every SeriesSet before evaluating,
	// so this buffers nothing it would not already. The cursor yields series in id
	// order; sort in memory only when the caller requires label order (the rare path:
	// PromQL always passes sortSeries=false).
	return materialize(set, sortSeries)
}

// materialize fully reads set into memory and returns an in-memory set, sorted by
// labels when the caller requires it. Draining here closes set's cursor (see
// readRow), releasing the routing read slot before materialize returns.
func materialize(set storage.SeriesSet, sortSeries bool) storage.SeriesSet {
	var series []storage.Series

	for set.Next() {
		series = append(series, set.At())
	}

	if err := set.Err(); err != nil {
		return storage.ErrSeriesSet(err)
	}

	if sortSeries {
		sort.Slice(series, func(i, j int) bool {
			return labels.Compare(series[i].Labels(), series[j].Labels()) < 0
		})
	}

	return &sliceSeriesSet{series: series, index: -1}
}

func (q *sqlQuerier) Close() error {
	// Select drains and closes every cursor before returning (see materialize), so
	// there is no open cursor to release here — only the in-flight registration.
	if q.onClose != nil {
		q.onClose()
		q.onClose = nil
	}

	return nil
}

// resolveSeries returns the labels of every series matching the where clause,
// keyed by their ClickHouse id. This reads only metadata (no points).
//
// When maxSeries is set it stops and returns limitqueryable.ErrTooManySeries as
// soon as the distinct-id count exceeds it, bounding both the map's memory and
// the id set pointsQuery will inline. It counts distinct ids (the map key), so
// duplicate tsdb_tags rows (a re-registered series, pre-merge) don't trip it.
func (q *sqlQuerier) resolveSeries(ctx context.Context, where string, args []any) (map[uuid.UUID]labels.Labels, error) {
	start := time.Now()
	defer func() {
		q.metrics.ReadSeconds.WithLabelValues("resolve").Observe(time.Since(start).Seconds())
	}()

	query := "SELECT id, metric_name, tenant, tags FROM tsdb_tags WHERE " + where

	rows, err := q.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	labelsByID := make(map[uuid.UUID]labels.Labels)

	for rows.Next() {
		var (
			id         uuid.UUID
			metricName string
			tenant     string
			tags       map[string]string
		)

		if err := rows.Scan(&id, &metricName, &tenant, &tags); err != nil {
			return nil, err
		}

		labelsByID[id] = q.buildLabels(metricName, tenant, tags)

		if q.maxSeries != 0 && len(labelsByID) > int(q.maxSeries) {
			return nil, limitqueryable.ErrTooManySeries
		}
	}

	return labelsByID, rows.Err()
}

// clampStoreTime clamps a millisecond timestamp to the range representable by
// the DateTime64(3) timestamp columns (see storeMinTimeMillis/storeMaxTimeMillis).
// Both the points filter (pointsQuery) and the activity filter (whereWithTime)
// bind their clamped bounds to fromUnixTimestamp64Milli, which would overflow
// and fail the query on an out-of-range value; clamping the bound rather than
// converting the column to int64 keeps the timestamp partition/sort key usable.
func clampStoreTime(ms int64) int64 {
	if ms < storeMinTimeMillis {
		return storeMinTimeMillis
	}

	if ms > storeMaxTimeMillis {
		return storeMaxTimeMillis
	}

	return ms
}

// pointsInlineThreshold is the largest id set pointsQuery inlines as a literal
// `id IN (?)` list. clickhouse-go renders the slice into the query text, so a
// larger set would risk exceeding ClickHouse's max_query_size (256 KiB default,
// ~6800 UUIDs); above the threshold pointsQuery falls back to a server-side
// subquery instead. Deployments that set promql.max_evaluated_series below this
// (resolveSeries caps the id set to it) never reach the fallback.
const pointsInlineThreshold = 5000

// pointsQuery builds the tsdb_data query and its arguments. Rows are ordered by
// id so the streaming SeriesSet can group points per series; series order does
// not matter (the caller sorts when it requires label order, see materialize).
//
// For up to pointsInlineThreshold ids it binds `id IN (?)` with the ids
// resolveSeries already resolved, so the matchers (including regex match()) are
// evaluated against tsdb_tags once per Select, not twice. Beyond that it falls
// back to `id IN (SELECT id FROM tsdb_tags WHERE <where>)`, re-evaluating the
// matchers on the (small) index table but keeping the query size bounded; where
// already carries the tenant scope (buildWhere), so the subquery reproduces
// exactly the ids resolveSeries found.
//
// A tenant-scoped query also filters tsdb_data by `tenant = ?` (the trusted
// request tenant). tsdb_data is keyed by id, so matching id alone would return
// BOTH tenants' points for a colliding id; the explicit tenant predicate confines
// any id collision to the querying tenant and lets ClickHouse prune on the
// (tenant, id, timestamp) primary key. Skipped when unscoped (q.tenant == ""),
// which reads across tenants as before.
func (q *sqlQuerier) pointsQuery(ids []uuid.UUID, where string, whereArgs []any) (string, []any) {
	queryArgs := make([]any, 0, len(whereArgs)+4)

	var tenantCond string

	if q.tenant != "" {
		tenantCond = "tenant = ? AND "

		queryArgs = append(queryArgs, q.tenant)
	}

	var idCond string

	if len(ids) <= pointsInlineThreshold {
		idCond = "id IN (?)"

		queryArgs = append(queryArgs, ids)
	} else {
		idCond = "id IN (SELECT id FROM tsdb_tags WHERE " + where + ")"

		queryArgs = append(queryArgs, whereArgs...)
	}

	// ORDER BY ends with insert_hour DESC so that duplicate (id, timestamp) rows
	// come newest-first; dedupSortedPoints then keeps the first of each run, i.e.
	// the last-written value (last-write-wins). insert_hour is not selected, only
	// used to order.
	query := "SELECT id, timestamp, value FROM tsdb_data" +
		" WHERE " + tenantCond + idCond +
		" AND timestamp >= fromUnixTimestamp64Milli(?) AND timestamp <= fromUnixTimestamp64Milli(?)" +
		" ORDER BY id, timestamp, insert_hour DESC"

	queryArgs = append(queryArgs, clampStoreTime(q.mint), clampStoreTime(q.maxt))

	return query, queryArgs
}

// aggValueExpr maps a PromQL function to the SQL expression that reconstructs the
// downsampled value from the min/max/sum/count columns, evaluated under GROUP BY
// (id, bucket) so it re-merges the parts the AggregatingMergeTree has not merged
// yet. It is an allowlist: only range-vector functions that min/max/sum/count can
// reproduce return ok=true. ok is false for everything else — the caller reads raw.
//
// Note hints.Func is the nearest enclosing function OR aggregation operator, so it
// carries both "sum_over_time" (a range aggregation over time — downsamplable) and
// "sum" (an instant operator aggregating across series, which wants each series'
// value at the eval instant, NOT a time aggregate). Only the *_over_time / counter
// forms belong here; the bare operators fall through to raw.
func aggValueExpr(fn string) (string, bool) {
	switch fn {
	// Range-vector aggregations over time: PromQL applies fn to every point in the
	// [range] window, so evaluating it over the bucketed series reproduces the raw
	// result — exactly for min/max/sum, approximately for avg (buckets weighted equally).
	case "min_over_time":
		return "min(min)", true
	case "max_over_time":
		return "max(max)", true
	case "sum_over_time":
		return "sum(sum)", true
	case "avg_over_time":
		return "sum(sum) / sum(count)", true
	case "last_over_time":
		// The bucket's own last sample: exact when the [range] ends on a bucket edge,
		// else the last sample of the last bucket that ended inside the range.
		return "argMinMerge(last)", true
	case "stddev_over_time", "stdvar_over_time":
		// Approximation: max() is assumed good enough for a spread over a downsampled
		// series (min/max/sum/count cannot reconstruct one).
		return "max(max)", true
	case "first_over_time":
		// No symmetrical column to last: min(min) is the approximation kept.
		return "min(min)", true
	case "rate", "irate", "increase", "delta", "idelta", "deriv":
		// Counter/trend family: the bucket max, and NOT last — they only differ on a
		// bucket holding a reset, and max is the one that keeps the rate right. Max
		// carries the pre-reset peak, which is what PromQL's reset correction consumes to
		// recover the climb that happened inside that bucket; last carries the post-reset
		// value, and that climb is then lost from the increase. The "rate with resets"
		// case in tests/squirreldb-promql-aggregate is the guard.
		return "max(max)", true
	case "present_over_time", "absent_over_time":
		// Depend only on whether the window holds ANY point, not on values or their
		// count; the downsampled series keeps one point per non-empty bucket, so
		// presence is preserved. (A sub-bucket [range] stays on raw via the range-width
		// guard in aggSelection.) The value is irrelevant here; max(max) is fine.
		return "max(max)", true

	case "count_over_time":
		// Counts points, not values: the downsampled series has ~one point per bucket
		// instead of the raw samples, so the count is off by the samples-per-bucket factor.
		return "", false
	case "quantile_over_time", "mad_over_time":
		// Need the per-point distribution, which min/max/sum/count don't preserve.
		return "", false
	case "resets", "changes":
		// Need per-point order, which the bucketed series loses.
		return "", false
	default:
		// A bare selector ("") and the instant aggregation operators (sum/min/max/...
		// across series at one instant) don't decide their routing here — aggSelection
		// sends an instant vector to the snapping path.
		// Any other unrecognised function reads "average".
		return "sum(sum) / sum(count)", true
	}
}

// aggMode is the point source aggSelection routes a Select to.
type aggMode int

const (
	aggModeRaw         aggMode = iota // read raw points from tsdb_data
	aggModeRange                      // read tsdb_data_agg_15m, one point per bucket (aggPointsQuery)
	aggModeInstantSnap                // read tsdb_data_agg_15m, one point per eval step (aggSnapQuery)
)

// aggPlan is aggSelection's decision: the mode and, for aggModeRange, the value
// expression aggValueExpr chose for hints.Func.
type aggPlan struct {
	mode aggMode
	expr string
}

// aggSelection decides which point source this Select reads. ForceRaw wins;
// otherwise, by the shape of the read:
//
//   - a range-vector function (min/max/sum/avg_over_time, counter family) routes to
//     the plain aggregate when aggValueExpr can reconstruct it and the [range] spans
//     at least two buckets. The decision is on the [range] width, NOT the step: a
//     range QUERY (Step > 0) additionally keeps a fine step on raw (don't downsample a
//     zoomed-in view), but an instant QUERY (Step == 0) evaluates a single point over
//     the whole [range], so it always uses the aggregate — this is the big instant
//     query, e.g. avg_over_time(m[740h]), that the aggregate exists for;
//   - an instant vector (Range == 0: a bare selector, or an instant aggregation
//     operator like sum()/max()) routes to the snapped aggregate — one representative
//     point per eval step, stamped on the step so PromQL's lookback, shorter than the
//     bucket spacing, lands on it (aggModeInstantSnap, see aggSnapQuery) — but only on
//     a coarse step; a fine step, or an instant query (Step == 0, a single read served
//     cheaply from raw's small lookback window), stays raw;
//   - everything else reads raw.
func (q *sqlQuerier) aggSelection(hints *storage.SelectHints) aggPlan {
	if q.forceRaw || hints == nil {
		return aggPlan{mode: aggModeRaw}
	}

	if hints.Range > 0 {
		rangeExpr, rangeOK := aggValueExpr(hints.Func)
		// A [range] narrower than two buckets would see zero or one downsampled point.
		if !rangeOK || hints.Range < 2*aggBucket.Milliseconds() {
			return aggPlan{mode: aggModeRaw}
		}

		// A range query zoomed in below the resolution keeps raw fidelity; an instant
		// query (Step == 0) always uses the aggregate.
		if hints.Step > 0 && hints.Step < aggMinStep.Milliseconds() {
			return aggPlan{mode: aggModeRaw}
		}

		return aggPlan{mode: aggModeRange, expr: rangeExpr}
	}

	// Instant vector. Snapping needs a coarse step grid; Step == 0 (instant query)
	// reads a tiny lookback window from raw instead.
	if hints.Step < aggMinStep.Milliseconds() {
		return aggPlan{mode: aggModeRaw}
	}

	return aggPlan{mode: aggModeInstantSnap}
}

// aggPointsQuery builds the query over tsdb_data_agg_15m. It mirrors pointsQuery
// (same tenant/id scoping and inline-vs-subquery fallback) but selects the bucket
// timestamp and the reconstructed value, grouping by (id, bucket) to finalize the
// partial aggregates. No dedup is needed on read: the GROUP BY collapses duplicate
// parts (including WAL re-drains) into one row per bucket.
func (q *sqlQuerier) aggPointsQuery(ids []uuid.UUID, valueExpr, where string, whereArgs []any) (string, []any) {
	queryArgs := make([]any, 0, len(whereArgs)+4)

	var tenantCond string

	if q.tenant != "" {
		tenantCond = "tenant = ? AND "

		queryArgs = append(queryArgs, q.tenant)
	}

	var idCond string

	if len(ids) <= pointsInlineThreshold {
		idCond = "id IN (?)"

		queryArgs = append(queryArgs, ids)
	} else {
		idCond = "id IN (SELECT id FROM tsdb_tags WHERE " + where + ")"

		queryArgs = append(queryArgs, whereArgs...)
	}

	// valueExpr is a fixed expression chosen by aggValueExpr (never user input).
	query := "SELECT id, bucket, " + valueExpr + " AS value FROM tsdb_data_agg_15m" +
		" WHERE " + tenantCond + idCond +
		" AND bucket >= fromUnixTimestamp64Milli(?) AND bucket <= fromUnixTimestamp64Milli(?)" +
		" GROUP BY id, bucket" +
		" ORDER BY id, bucket"

	queryArgs = append(queryArgs, clampStoreTime(q.mint), clampStoreTime(q.maxt))

	return query, queryArgs
}

// aggSnapQuery builds the instant-vector query over tsdb_data_agg_15m. Where
// aggPointsQuery returns one point per bucket labelled on the bucket's right edge,
// this returns one point per candidate PromQL eval instant, valued from the bucket
// covering that instant and stamped ON it:
//
//   - the covering bucket is toStartOfInterval(ts, 15min) — the last bucket whose
//     right edge is at/before ts, so within aggBucket of it (the join matches that
//     one bucket, which enforces the aggBucket staleness cap: a ts whose bucket has
//     no data yields no row, i.e. a gap, so a series idle for more than one bucket
//     drops out);
//   - the value is that bucket's last sample (argMinMerge(last), see
//     tsdbDataAgg15mDDL), which is what PromQL would itself return for an instant
//     vector over the 15-min-downsampled series: exactly the raw value whenever the
//     instant falls on a bucket edge, at most one bucket stale otherwise;
//   - stamping on the eval grid is what lets PromQL's lookback, shorter than the
//     15-min bucket spacing, land on a point (the reason a bare selector routed to
//     the bucket-labelled aggregate would drop half its steps).
//
// The instants are not recoverable from the hints, so the grid unions the two
// families PromQL evaluates on — at most two points per step:
//
//   - Start + i*Step. For a range query hints.Start is the first eval instant less
//     (lookbackDelta - 1) — Prometheus widens it for the instant-vector lookback —
//     so each point lands just inside its instant's lookback window, whatever
//     lookbackDelta the engine is configured with;
//   - the multiples of Step, from the first one at/after Start. That is the grid a
//     subquery evaluates on, and hints.Start there is also shifted by the subquery
//     range, so a Start-anchored point can fall a whole Step from the instant —
//     outside the lookback, dropping every step of the query.
//
// A point no instant uses is harmless: each carries the bucket covering itself, and
// PromQL takes the latest point at/before its instant, i.e. the freshest of the two.
// Aligning up (never below Start) keeps the bucket scan bound below unchanged.
//
// GROUP BY re-merges the AggregatingMergeTree parts, as in aggPointsQuery, and
// collapses the two grids where they coincide. Step is guaranteed positive by
// aggSelection (aggModeInstantSnap requires Step > 0).
func (q *sqlQuerier) aggSnapQuery(
	ids []uuid.UUID,
	hints *storage.SelectHints,
	where string,
	whereArgs []any,
) (string, []any) {
	queryArgs := make([]any, 0, len(whereArgs)+10)

	// The two grids over [Start, End] stepping by Step; End+1 includes an aligned End
	// (range() is exclusive on its upper bound).
	queryArgs = append(queryArgs,
		hints.Start, hints.End+1, hints.Step,
		alignUp(hints.Start, hints.Step), hints.End+1, hints.Step,
	)

	var tenantCond string

	if q.tenant != "" {
		tenantCond = "a.tenant = ? AND "

		queryArgs = append(queryArgs, q.tenant)
	}

	var idCond string

	if len(ids) <= pointsInlineThreshold {
		idCond = "a.id IN (?)"

		queryArgs = append(queryArgs, ids)
	} else {
		idCond = "a.id IN (SELECT id FROM tsdb_tags WHERE " + where + ")"

		queryArgs = append(queryArgs, whereArgs...)
	}

	// Coarse bucket filter (partition pruning); the join key does the exact matching.
	// The lower bound is one bucket before Start, since a grid point near Start maps to
	// a bucket up to aggBucket earlier.
	//
	// The grid subquery is the RIGHT (build) side of the join so ClickHouse hashes the
	// small step grid, not the large tsdb_data_agg_15m slice. The range() placeholders
	// still precede the WHERE ones textually, so the positional arg order is unchanged.
	query := "SELECT a.id AS id, grid.ts AS ts, argMinMerge(a.last) AS value" +
		" FROM tsdb_data_agg_15m AS a" +
		" INNER JOIN (SELECT ts, toDateTime(toStartOfInterval(ts, INTERVAL 15 MINUTE)) AS bucket FROM" +
		" (SELECT fromUnixTimestamp64Milli(arrayJoin(arrayDistinct(arrayConcat(" +
		"range(toUInt64(?), toUInt64(?), toUInt64(?)), range(toUInt64(?), toUInt64(?), toUInt64(?))" +
		")))) AS ts)) AS grid" +
		" ON a.bucket = grid.bucket" +
		" WHERE " + tenantCond + idCond +
		" AND a.bucket >= fromUnixTimestamp64Milli(?) AND a.bucket <= fromUnixTimestamp64Milli(?)" +
		" GROUP BY a.id, grid.ts" +
		" ORDER BY a.id, ts"

	queryArgs = append(queryArgs, clampStoreTime(hints.Start-aggBucket.Milliseconds()), clampStoreTime(hints.End))

	return query, queryArgs
}

// alignUp returns the smallest multiple of step at/after v; step must be positive.
func alignUp(v, step int64) int64 {
	rem := v % step
	if rem == 0 {
		return v
	}

	// Go truncates toward zero, so a negative v is already at/after its multiple.
	if rem < 0 {
		return v - rem
	}

	return v - rem + step
}

// clickhouse error codes raised when a result/read row limit is exceeded with the
// throw overflow mode (max_result_rows -> TOO_MANY_ROWS_OR_BYTES, max_rows_to_read
// -> TOO_MANY_ROWS). Either means the read would evaluate more points than allowed.
const (
	codeTooManyRows        = 158
	codeTooManyRowsOrBytes = 396
)

// pointReadContext caps a point read at maxPoints result rows, so ClickHouse aborts
// it (throwing) instead of streaming an unbounded raw read; 0 leaves ctx untouched.
func (q *sqlQuerier) pointReadContext(ctx context.Context) context.Context {
	if q.maxPoints == 0 {
		return ctx
	}

	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_result_rows":      q.maxPoints,
		"result_overflow_mode": "throw",
	}))
}

// asTooManyPoints maps a ClickHouse result-limit overflow to
// limitqueryable.ErrTooManyPoints (the same client-facing error the Go-side counter
// raises), so the API reports it uniformly; ok is false for any other error.
func asTooManyPoints(err error) (error, bool) {
	var ex *proto.Exception
	if errors.As(err, &ex) && (ex.Code == codeTooManyRows || ex.Code == codeTooManyRowsOrBytes) {
		return fmt.Errorf("%w (%s)", limitqueryable.ErrTooManyPoints, ex.Message), true
	}

	return err, false
}

// buildLabels reconstructs a series' label set from its tsdb_tags columns.
func (lq *labelQuerier) buildLabels(metricName, tenant string, tags map[string]string) labels.Labels {
	b := labels.NewScratchBuilder(len(tags) + 2)
	b.Add(labels.MetricName, metricName)

	if tenant != "" {
		b.Add(lq.tenantLabel, tenant)
	}

	for name, value := range tags {
		b.Add(name, value)
	}

	b.Sort()

	return b.Labels()
}

// dedupSortedPoints collapses consecutive points that share a timestamp, keeping
// the first, in a slice already sorted by timestamp. It compacts in place (no
// allocation) and is a no-op when there is nothing to drop.
//
// tsdb_data is a plain MergeTree with no deduplication, so re-flushing a point
// (takeover, batcher last-chance re-write) or a client re-writing a corrected
// value can leave several rows for the same (id, timestamp). The read cursor
// orders by (id, timestamp, insert_hour DESC), so such rows are adjacent and the
// most recently written one comes first; left in place they would make PromQL
// abort with "duplicate sample for timestamp". We keep the first of each run
// (last-write-wins) and drop the rest, on each series' points as the read cursor
// groups them (see sqlSeriesSet.Next).
func dedupSortedPoints(points []types.MetricPoint) []types.MetricPoint {
	if len(points) < 2 {
		return points
	}

	w := 1

	for r := 1; r < len(points); r++ {
		if points[r].Timestamp == points[w-1].Timestamp {
			continue
		}

		points[w] = points[r]
		w++
	}

	return points[:w]
}

// sqlSeriesSet is a streaming storage.SeriesSet over an (id, timestamp)-ordered
// tsdb_data cursor: it groups the consecutive rows of each id into one series.
type sqlSeriesSet struct {
	rows       driver.Rows
	labelsByID map[uuid.UUID]labels.Labels

	started   bool
	hasLook   bool
	closed    bool
	lookahead sqlPoint

	current sqlSeries
	err     error
}

// close releases the cursor. It is idempotent: a fully-drained cursor is already
// closed by readRow, and Close (via the querier) may close an abandoned one.
func (s *sqlSeriesSet) close() {
	if !s.closed && s.rows != nil {
		_ = s.rows.Close()
		s.closed = true
	}
}

type sqlPoint struct {
	id    uuid.UUID
	point types.MetricPoint
}

// Next groups the cursor's rows for one id into the current series. The cursor is
// ordered by (id, timestamp), so a series' rows are contiguous and already sorted.
func (s *sqlSeriesSet) Next() bool {
	if s.err != nil {
		return false
	}

	if !s.started {
		s.started = true

		p, ok := s.readRow()
		if !ok {
			return false
		}

		s.lookahead = p
		s.hasLook = true
	}

	for s.hasLook {
		id := s.lookahead.id
		points := []types.MetricPoint{s.lookahead.point}
		s.hasLook = false

		for {
			p, ok := s.readRow()
			if !ok {
				break
			}

			if p.id != id {
				s.lookahead = p
				s.hasLook = true

				break
			}

			points = append(points, p.point)
		}

		if s.err != nil {
			return false
		}

		lbls, ok := s.labelsByID[id]
		if !ok {
			// Series not in the resolved set (shouldn't happen): skip it.
			continue
		}

		// tsdb_data is a plain MergeTree and the WAL is at-least-once (a re-drained
		// batch can leave several rows for the same (id, timestamp)); drop the
		// duplicates so PromQL doesn't abort with "duplicate sample".
		s.current = sqlSeries{labels: lbls, points: dedupSortedPoints(points)}

		return true
	}

	return false
}

func (s *sqlSeriesSet) readRow() (sqlPoint, bool) {
	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			// A result-limit overflow surfaces here (the read is streamed); map it to
			// the shared too-many-points error, otherwise keep the driver error.
			s.err, _ = asTooManyPoints(err)
		}

		s.close()

		return sqlPoint{}, false
	}

	var (
		id    uuid.UUID
		ts    time.Time
		value float64
	)

	if err := s.rows.Scan(&id, &ts, &value); err != nil {
		s.err = err

		s.close()

		return sqlPoint{}, false
	}

	return sqlPoint{id: id, point: types.MetricPoint{Timestamp: ts.UnixMilli(), Value: value}}, true
}

func (s *sqlSeriesSet) At() storage.Series { return s.current }
func (s *sqlSeriesSet) Err() error         { return s.err }

func (s *sqlSeriesSet) Warnings() annotations.Annotations { return nil }

// sliceSeriesSet is an in-memory storage.SeriesSet over a pre-built slice, used
// to return series sorted by labels (see materialize).
type sliceSeriesSet struct {
	series []storage.Series
	index  int
}

func (s *sliceSeriesSet) Next() bool {
	s.index++

	return s.index < len(s.series)
}

func (s *sliceSeriesSet) At() storage.Series                { return s.series[s.index] }
func (s *sliceSeriesSet) Err() error                        { return nil }
func (s *sliceSeriesSet) Warnings() annotations.Annotations { return nil }

// sqlSeries is one series with its buffered points.
type sqlSeries struct {
	labels labels.Labels
	points []types.MetricPoint
}

func (s sqlSeries) Labels() labels.Labels { return s.labels }

func (s sqlSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return convert.NewSampleIterator(s.points)
}

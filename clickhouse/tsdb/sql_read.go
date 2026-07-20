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
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/queryable/limitqueryable"
	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/model/histogram"
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
	// forcePreAggregated / forceRaw override the step-based routing (see
	// aggSelection): the ForcePreAggregated / ForceRaw request headers.
	forcePreAggregated bool
	forceRaw           bool
	requestDebug       types.DebugLevel
	logger             zerolog.Logger
	// sets are the streaming sets produced by Select (a querier may be queried
	// several times, each set consumed independently); Close releases their
	// cursors. setsMu guards the slice against concurrent Select calls.
	setsMu sync.Mutex
	sets   []*sqlSeriesSet
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

	if aggExpr, ok := q.aggSelection(hints); ok {
		query, queryArgs = q.aggPointsQuery(ids, aggExpr, where, args)
		source = "aggregated"
	} else {
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
			"clickhouse Select: series=%d source=%s func=%q step=%d forceRaw=%v forcePreAgg=%v",
			len(labelsByID), source, fn, step, q.forceRaw, q.forcePreAggregated)
	}

	rows, err := q.conn.Query(q.pointReadContext(ctx), query, queryArgs...)
	if err != nil {
		if mapped, ok := asTooManyPoints(err); ok {
			return storage.ErrSeriesSet(mapped)
		}

		return storage.ErrSeriesSet(fmt.Errorf("clickhouse: read points: %w", err))
	}

	set := &sqlSeriesSet{rows: rows, labelsByID: labelsByID}

	// Track the set so Close releases its cursor even if the caller abandons the
	// iteration early (a fully-drained cursor closes itself, see readRow). Earlier
	// sets are left untouched — a querier may be Select-ed several times and each
	// set is consumed independently.
	q.trackSet(set)

	// The cursor (and thus the streaming set) yields series in id order. When the
	// caller requires label order, drain and sort in memory. This is the rare
	// path: PromQL always calls Select with sortSeries=false.
	if sortSeries {
		return sortedByLabels(set)
	}

	return set
}

// sortedByLabels fully reads a SeriesSet and returns it sorted by labels.
func sortedByLabels(set storage.SeriesSet) storage.SeriesSet {
	var series []storage.Series

	for set.Next() {
		series = append(series, set.At())
	}

	if err := set.Err(); err != nil {
		return storage.ErrSeriesSet(err)
	}

	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].Labels(), series[j].Labels()) < 0
	})

	return &sliceSeriesSet{series: series, index: -1}
}

func (q *sqlQuerier) Close() error {
	// Release every cursor before signalling done, so the connection is free of
	// this querier's work by the time Close (the TSDB one) stops waiting.
	q.setsMu.Lock()
	sets := q.sets
	q.sets = nil
	q.setsMu.Unlock()

	for _, set := range sets {
		set.close()
	}

	if q.onClose != nil {
		q.onClose()
		q.onClose = nil
	}

	return nil
}

// trackSet records a streaming set so Close releases its cursor.
func (q *sqlQuerier) trackSet(set *sqlSeriesSet) {
	q.setsMu.Lock()
	q.sets = append(q.sets, set)
	q.setsMu.Unlock()
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
// not matter (the caller sorts when it requires label order, see sortedByLabels).
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
// yet. ok is false for functions those four columns cannot serve soundly — the
// caller then reads raw.
func aggValueExpr(fn string) (string, bool) {
	switch fn {
	case "min", "min_over_time":
		return "min(min)", true
	case "max", "max_over_time":
		return "max(max)", true
	case "sum", "sum_over_time":
		return "sum(sum)", true
	case "count", "count_over_time":
		return "toFloat64(sum(count))", true
	case "avg", "avg_over_time":
		return "sum(sum) / sum(count)", true
	case "rate", "irate", "increase", "delta", "idelta", "deriv":
		// Counter/trend family: use the bucket max as the representative value (like
		// the Cassandra backend). On a monotonic counter it equals the last cumulative
		// value; PromQL still detects resets on the downsampled series.
		return "max(max)", true
	case "last_over_time", "stddev_over_time", "stdvar_over_time":
		// Approximation: as above, on a monotonic counter max is equals the last value (so exact),
		// and for non-counter, max() is assumed to be good enough.
		return "max(max)", true
	case "first_over_time":
		// Symmetrical to last_over_time
		return "min(min)", true
	case "resets", "changes":
		// Not soundly served by min/max/sum/count (resets/changes need per-point order): read raw.
		return "", false
	default:
		return "sum(sum) / sum(count)", true // average, like the Cassandra default
	}
}

// aggSelection decides whether this Select reads the pre-aggregated table and, if
// so, returns the value expression for hints.Func. ForceRaw wins; ForcePreAggregated
// then forces the aggregate for any aggregatable function. Otherwise the aggregate
// is used only when both hold: the step reaches aggMinStep (resolution — don't
// over-downsample), and the range-vector window spans at least two buckets
// (fidelity — a [range] narrower than the bucket can't be served from the
// downsampled series). A non-aggregatable function always reads raw.
func (q *sqlQuerier) aggSelection(hints *storage.SelectHints) (string, bool) {
	if q.forceRaw {
		return "", false
	}

	fn := ""
	if hints != nil {
		fn = hints.Func
	}

	expr, ok := aggValueExpr(fn)
	if !ok {
		return "", false
	}

	if q.forcePreAggregated {
		return expr, true
	}

	if hints == nil || hints.Step < aggMinStep.Milliseconds() {
		return "", false
	}

	// Range is the [range] selector width in ms (0 for an instant vector, which
	// only needs one point per step). A narrower window than two buckets would see
	// zero or one downsampled point, so keep it on raw.
	if hints.Range != 0 && hints.Range < 2*aggBucket.Milliseconds() {
		return "", false
	}

	return expr, true
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
// to return series sorted by labels (see sortedByLabels).
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
	return &samplesIterator{points: s.points, offset: -1}
}

// samplesIterator is a chunkenc.Iterator over a slice of float samples.
type samplesIterator struct {
	points []types.MetricPoint
	offset int
}

func (it *samplesIterator) Next() chunkenc.ValueType {
	if it.offset+1 >= len(it.points) {
		return chunkenc.ValNone
	}

	it.offset++

	return chunkenc.ValFloat
}

func (it *samplesIterator) Seek(t int64) chunkenc.ValueType { //nolint:govet
	if it.offset < 0 {
		it.offset = 0
	}

	for ; it.offset < len(it.points); it.offset++ {
		if it.points[it.offset].Timestamp >= t {
			return chunkenc.ValFloat
		}
	}

	return chunkenc.ValNone
}

func (it *samplesIterator) At() (int64, float64) {
	return it.points[it.offset].Timestamp, it.points[it.offset].Value
}

func (it *samplesIterator) AtT() int64 {
	return it.points[it.offset].Timestamp
}

func (it *samplesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

func (it *samplesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (it *samplesIterator) Err() error { return nil }

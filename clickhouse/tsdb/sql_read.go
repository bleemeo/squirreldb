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
	"sort"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

// sqlQuerier serves Select entirely through SQL: it resolves the matching series
// and their labels from tsdb_tags, then streams their points from tsdb_data.
// Recently-written points are visible once the WAL drainer has flushed them
// (~the flush delay); there is no read-merge of un-flushed points.
// LabelValues/LabelNames come from the embedded labelQuerier.
type sqlQuerier struct {
	labelQuerier

	metrics *metrics
	// onClose releases this querier's in-flight registration (queries.Done); see
	// ClickHouseTSDB.newSQLQuerier. Called once, from Close.
	onClose func()
	// sets are the streaming sets produced by Select (a querier may be queried
	// several times, each set consumed independently); Close releases their
	// cursors. setsMu guards the slice against concurrent Select calls.
	setsMu sync.Mutex
	sets   []*sqlSeriesSet
}

func (q *sqlQuerier) Select(
	ctx context.Context,
	sortSeries bool,
	_ *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	where, args := q.buildWhere(matchers)

	labelsByID, err := q.resolveSeries(ctx, where, args)
	if err != nil {
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

	query, queryArgs := q.pointsQuery(ids)

	rows, err := q.conn.Query(ctx, query, queryArgs...)
	if err != nil {
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

// pointsQuery builds the tsdb_data query and its arguments. Rows are ordered by
// id so the streaming SeriesSet can group points per series; series order does
// not matter (the caller sorts when it requires label order, see sortedByLabels).
//
// It binds `id IN (?)` with the ids resolveSeries already resolved, rather than
// re-running the matcher WHERE as a `id IN (SELECT ... FROM tsdb_tags WHERE ...)`
// subquery: the matchers (including regex match()) are thus evaluated against
// tsdb_tags once per Select, not twice.
//
// A tenant-scoped query also filters tsdb_data by `tenant = ?` (the trusted
// request tenant). tsdb_data is keyed by id, so matching id alone would return
// BOTH tenants' points for a colliding id; the explicit tenant predicate confines
// any id collision to the querying tenant and lets ClickHouse prune on the
// (tenant, id, timestamp) primary key. Skipped when unscoped (q.tenant == ""),
// which reads across tenants as before.
func (q *sqlQuerier) pointsQuery(ids []uuid.UUID) (string, []any) {
	queryArgs := make([]any, 0, 4)

	var tenantCond string

	if q.tenant != "" {
		tenantCond = "tenant = ? AND "

		queryArgs = append(queryArgs, q.tenant)
	}

	// ORDER BY ends with inserted_at DESC so that duplicate (id, timestamp) rows
	// come newest-first; dedupSortedPoints then keeps the first of each run, i.e.
	// the last-written value (last-write-wins). inserted_at is not selected, only
	// used to order.
	query := "SELECT id, timestamp, value FROM tsdb_data" +
		" WHERE " + tenantCond + "id IN (?)" +
		" AND timestamp >= fromUnixTimestamp64Milli(?) AND timestamp <= fromUnixTimestamp64Milli(?)" +
		" ORDER BY id, timestamp, inserted_at DESC"

	queryArgs = append(queryArgs, ids, clampStoreTime(q.mint), clampStoreTime(q.maxt))

	return query, queryArgs
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
// orders by (id, timestamp, inserted_at DESC), so such rows are adjacent and the
// most recently written one comes first; left in place they would make PromQL
// abort with "duplicate sample for timestamp". We keep the first of each run
// (last-write-wins) and drop the rest, on the flushed slice we just built.
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
			s.err = err
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

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
	"time"

	"github.com/bleemeo/squirreldb/batch"
	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

// sqlQuerier serves Select entirely through SQL (mode "sql"): it resolves the
// matching series and their labels from tsdb_tags, then streams their points
// from tsdb_data, merging in the points still buffered in the batcher (not yet
// flushed) so recent writes are visible. LabelValues/LabelNames come from the
// embedded labelQuerier.
type sqlQuerier struct {
	labelQuerier

	batcher *batch.Batch
	metrics *metrics
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

	// Points still buffered in the batcher (not yet flushed to tsdb_data).
	pending, err := q.readPending(ctx, labelsByID)
	if err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("clickhouse: read pending points: %w", err))
	}

	query, queryArgs := q.pointsQuery(where, args)

	rows, err := q.conn.Query(ctx, query, queryArgs...)
	if err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("clickhouse: read points: %w", err))
	}

	set := &sqlSeriesSet{rows: rows, labelsByID: labelsByID, pending: pending}

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
	return nil
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

// readPending reads, for every matching series, the points still buffered in the
// batcher within [mint, maxt]. The batcher is keyed by the labels hash, so we
// derive it from the reconstructed labels. Returns a map keyed by ClickHouse id.
func (q *sqlQuerier) readPending(
	ctx context.Context,
	labelsByID map[uuid.UUID]labels.Labels,
) (map[uuid.UUID][]types.MetricPoint, error) {
	if q.batcher == nil || len(labelsByID) == 0 {
		return nil, nil //nolint:nilnil
	}

	start := time.Now()
	defer func() {
		q.metrics.ReadSeconds.WithLabelValues("pending").Observe(time.Since(start).Seconds())
	}()

	chids := make([]uuid.UUID, 0, len(labelsByID))
	hashIDs := make([]types.MetricID, 0, len(labelsByID))

	for chid, lbls := range labelsByID {
		chids = append(chids, chid)
		hashIDs = append(hashIDs, types.MetricID(lbls.Hash())) //nolint:gosec // hash used as the batcher key
	}

	data, err := q.batcher.ReadTemporaryMultiple(ctx, hashIDs, q.mint, q.maxt)
	if err != nil {
		return nil, err
	}

	pending := make(map[uuid.UUID][]types.MetricPoint)

	var pendingPoints int

	for i, md := range data {
		if len(md.Points) > 0 {
			pending[chids[i]] = md.Points
			pendingPoints += len(md.Points)
		}
	}

	q.metrics.ReadPendingPointsTotal.Add(float64(pendingPoints))

	return pending, nil
}

// pointsQuery builds the tsdb_data query and its arguments. Rows are ordered by
// id so the streaming SeriesSet can group points per series; series order does
// not matter (the caller sorts when it requires label order, see sortedByLabels).
func (q *sqlQuerier) pointsQuery(where string, args []any) (string, []any) {
	query := "SELECT id, timestamp, value FROM tsdb_data" +
		" WHERE id IN (SELECT id FROM tsdb_tags WHERE " + where + ")" +
		" AND timestamp >= fromUnixTimestamp64Milli(?) AND timestamp <= fromUnixTimestamp64Milli(?)" +
		" ORDER BY id, timestamp"

	queryArgs := make([]any, 0, len(args)+2)
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, q.mint, q.maxt)

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

// mergePoints merges two timestamp-sorted slices, dropping duplicate timestamps
// (a point flushed to tsdb_data and still in the batcher); the first slice wins
// on a tie.
func mergePoints(flushed, pending []types.MetricPoint) []types.MetricPoint {
	if len(pending) == 0 {
		return flushed
	}

	if len(flushed) == 0 {
		return pending
	}

	out := make([]types.MetricPoint, 0, len(flushed)+len(pending))

	var (
		lastTS   int64
		haveLast bool
	)

	add := func(p types.MetricPoint) {
		if haveLast && p.Timestamp == lastTS {
			return
		}

		out = append(out, p)
		lastTS = p.Timestamp
		haveLast = true
	}

	i, j := 0, 0
	for i < len(flushed) && j < len(pending) {
		if flushed[i].Timestamp <= pending[j].Timestamp {
			add(flushed[i])
			i++
		} else {
			add(pending[j])
			j++
		}
	}

	for ; i < len(flushed); i++ {
		add(flushed[i])
	}

	for ; j < len(pending); j++ {
		add(pending[j])
	}

	return out
}

// sqlSeriesSet is a streaming storage.SeriesSet over an (id, timestamp)-ordered
// tsdb_data cursor, merging each series with its batcher pending points. It has
// two phases: first the series that have flushed points (driven by the cursor),
// then the series that only have pending points (batcher-only, in the query
// window).
type sqlSeriesSet struct {
	rows       driver.Rows
	labelsByID map[uuid.UUID]labels.Labels
	pending    map[uuid.UUID][]types.MetricPoint // consumed in phase 1, leftovers emitted in phase 2

	started    bool
	lookahead  sqlPoint
	hasLook    bool
	inPhase2   bool
	leftover   []uuid.UUID
	leftoverAt int

	current sqlSeries
	err     error
}

type sqlPoint struct {
	id    uuid.UUID
	point types.MetricPoint
}

func (s *sqlSeriesSet) Next() bool {
	if s.err != nil {
		return false
	}

	if !s.inPhase2 {
		if s.nextFlushed() {
			return true
		}

		if s.err != nil {
			return false
		}

		// Cursor exhausted: move to the batcher-only series.
		s.inPhase2 = true
		s.leftover = make([]uuid.UUID, 0, len(s.pending))

		for id := range s.pending {
			s.leftover = append(s.leftover, id)
		}
	}

	return s.nextPendingOnly()
}

// nextFlushed yields the next series that has flushed points (from the cursor),
// merging in its pending points.
func (s *sqlSeriesSet) nextFlushed() bool {
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
		flushed := []types.MetricPoint{s.lookahead.point}
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

			flushed = append(flushed, p.point)
		}

		if s.err != nil {
			return false
		}

		lbls, ok := s.labelsByID[id]
		if !ok {
			// Series not in the resolved set (shouldn't happen): skip it.
			continue
		}

		points := mergePoints(flushed, s.pending[id])
		delete(s.pending, id)

		s.current = sqlSeries{labels: lbls, points: points}

		return true
	}

	return false
}

// nextPendingOnly yields the next series that only has batcher points.
func (s *sqlSeriesSet) nextPendingOnly() bool {
	for s.leftoverAt < len(s.leftover) {
		id := s.leftover[s.leftoverAt]
		s.leftoverAt++

		points := s.pending[id]
		if len(points) == 0 {
			continue
		}

		s.current = sqlSeries{labels: s.labelsByID[id], points: points}

		return true
	}

	return false
}

func (s *sqlSeriesSet) readRow() (sqlPoint, bool) {
	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			s.err = err
		}

		_ = s.rows.Close()

		return sqlPoint{}, false
	}

	var (
		id    uuid.UUID
		ts    time.Time
		value float64
	)

	if err := s.rows.Scan(&id, &ts, &value); err != nil {
		s.err = err

		_ = s.rows.Close()

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

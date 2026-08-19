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

package dummy

import (
	"context"
	"maps"
	"slices"
	"sort"
	"sync"

	"github.com/bleemeo/squirreldb/appendable/fixedappender"
	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

// DiscardTSDB will write metrics to /dev/null.
type DiscardTSDB struct{}

func (DiscardTSDB) Close(context.Context) error {
	return nil
}

func (db DiscardTSDB) QuerierWithOptions(_ types.QuerierOptions, mint, maxt int64) (storage.Querier, error) {
	return db.Querier(mint, maxt)
}

func (db DiscardTSDB) ChunkQuerierWithOptions(_ types.QuerierOptions, mint, maxt int64) (storage.ChunkQuerier, error) {
	return db.ChunkQuerier(mint, maxt)
}

func (DiscardTSDB) AppenderWithOptions(ctx context.Context, _ types.AppenderOptions) storage.Appender {
	return fixedappender.NewNoopAppender()
}

func (DiscardTSDB) Querier(mint, maxt int64) (storage.Querier, error) {
	return storage.NoopQuerier(), nil
}

func (DiscardTSDB) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return convert.NewQuerierToChunkQuerier(storage.NoopQuerier()), nil
}

func (DiscardTSDB) Flush(context.Context) error {
	return nil
}

func (DiscardTSDB) Run(ctx context.Context) error {
	return nil
}

// MemoryTSDB store all value in memory. Only useful in unittest.
type MemoryTSDB struct {
	mutex sync.Mutex
	data  map[uint64]Data
}

type Data struct {
	Labels labels.Labels
	TTL    int64
	Points []types.MetricPoint
}

func (db *MemoryTSDB) Queryable(ctx context.Context) (types.QueryableWithOptions, error) {
	return db, nil
}

func (db *MemoryTSDB) Close(context.Context) error {
	return nil
}

func (db *MemoryTSDB) AppenderWithOptions(ctx context.Context, options types.AppenderOptions) storage.Appender {
	return &appender{
		pendingTimeSeries: make(map[uint64]types.Timeseries),
		db:                db,
		ttl:               options.WriteTTL,
	}
}

func (db *MemoryTSDB) QuerierWithOptions(_ types.QuerierOptions, mint, maxt int64) (storage.Querier, error) {
	return db.Querier(mint, maxt)
}

func (db *MemoryTSDB) ChunkQuerierWithOptions(_ types.QuerierOptions, mint, maxt int64) (storage.ChunkQuerier, error) {
	return db.ChunkQuerier(mint, maxt)
}

func (db *MemoryTSDB) Querier(mint, maxt int64) (storage.Querier, error) {
	return &querier{
		db:   db,
		mint: mint,
		maxt: maxt,
	}, nil
}

func (db *MemoryTSDB) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := db.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}

	return convert.NewQuerierToChunkQuerier(q), nil
}

func (db *MemoryTSDB) AddData(data []types.Timeseries, ttl int64) {
	metrics := make(map[uint64]types.Timeseries, len(data))
	for _, row := range data {
		id := row.Labels.Hash()
		metrics[id] = row
	}

	db.write(metrics, ttl)
}

func (db *MemoryTSDB) Data() []types.Timeseries {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	result := make([]types.Timeseries, 0, len(db.data))

	for _, metric := range db.data {
		result = append(result, types.Timeseries{
			Points: metric.Points,
			Labels: metric.Labels,
		})
	}

	return result
}

func (db *MemoryTSDB) CountMetrics() int {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	return len(db.data)
}

type appender struct {
	db  *MemoryTSDB
	ttl int64

	pendingTimeSeries map[uint64]types.Timeseries
}

func (app *appender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	labelsHash := l.Hash()

	ts, ok := app.pendingTimeSeries[labelsHash]
	if !ok {
		app.pendingTimeSeries[labelsHash] = types.Timeseries{
			Labels: l,
			Points: []types.MetricPoint{{
				Timestamp: t,
				Value:     v,
			}},
		}
	} else {
		ts.Points = append(ts.Points, types.MetricPoint{
			Timestamp: t,
			Value:     v,
		})
		app.pendingTimeSeries[labelsHash] = ts
	}

	return 0, nil
}

func (app *appender) Commit() error {
	defer func() {
		app.pendingTimeSeries = make(map[uint64]types.Timeseries)
	}()

	app.db.write(app.pendingTimeSeries, app.ttl)

	return nil
}

func (app *appender) Rollback() error {
	app.pendingTimeSeries = make(map[uint64]types.Timeseries)

	return nil
}

func (app *appender) SetOptions(opts *storage.AppendOptions) {
}

func (app *appender) AppendExemplar(
	ref storage.SeriesRef,
	l labels.Labels,
	e exemplar.Exemplar,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *appender) AppendHistogram(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *appender) AppendHistogramCTZeroSample(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	ct int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *appender) UpdateMetadata(
	ref storage.SeriesRef,
	l labels.Labels,
	m metadata.Metadata,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *appender) AppendCTZeroSample(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	ct int64,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

type querier struct {
	db   *MemoryTSDB
	mint int64
	maxt int64
}

func (q *querier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	metricIDs := q.db.filterMetricIDs(matchers, sortSeries)

	return &seriesSet{
		db:   q.db,
		list: metricIDs,
		minT: q.mint,
		maxT: q.maxt,
	}
}

func (q *querier) LabelValues(
	ctx context.Context,
	name string,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	metricIDs := q.db.filterMetricIDs(matchers, false)
	metricIDs = q.db.metricsWithData(metricIDs, q.mint, q.maxt)

	labelsList := q.db.getLabels(metricIDs)

	result := make(map[string]any, 0)

	for _, lbl := range labelsList {
		v := lbl.Get(name)
		if v != "" {
			result[v] = nil
		}
	}

	resultSlice := slices.Collect(maps.Keys(result))
	slices.Sort(resultSlice)

	return resultSlice, nil, nil
}

func (q *querier) LabelNames(
	ctx context.Context,
	_ *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	metricIDs := q.db.filterMetricIDs(matchers, false)
	metricIDs = q.db.metricsWithData(metricIDs, q.mint, q.maxt)

	labelsList := q.db.getLabels(metricIDs)

	result := make(map[string]any, 0)

	for _, lbl := range labelsList {
		lbl.Range(func(l labels.Label) {
			result[l.Name] = nil
		})
	}

	resultSlice := slices.Collect(maps.Keys(result))
	slices.Sort(resultSlice)

	return resultSlice, nil, nil
}

func (q *querier) Close() error {
	return nil
}

type seriesSet struct {
	db      *MemoryTSDB
	minT    int64
	maxT    int64
	list    []uint64
	current series
}

func (i *seriesSet) Next() bool {
	for len(i.list) > 0 {
		id := i.list[0]
		i.list = i.list[1:]

		points, lbl := i.db.getPointsAndLabels(id, i.minT, i.maxT)
		if len(points) > 0 {
			i.current = series{
				points: points,
				labels: lbl,
			}

			return true
		}
	}

	return false
}

func (i *seriesSet) At() storage.Series {
	return i.current
}

func (i *seriesSet) Err() error {
	return nil
}

func (i *seriesSet) Warnings() annotations.Annotations {
	return nil
}

type series struct {
	labels labels.Labels
	points []types.MetricPoint
}

func (s series) Labels() labels.Labels {
	return s.labels
}

func (s series) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return convert.NewSampleIterator(s.points)
}

func (db *MemoryTSDB) write(pendingTimeSeries map[uint64]types.Timeseries, ttl int64) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.data == nil {
		db.data = make(map[uint64]Data)
	}

	for k, v := range pendingTimeSeries {
		d, ok := db.data[k]
		if !ok {
			d.Labels = v.Labels.Copy()
		}

		d.TTL = max(d.TTL, ttl)

		d.Points = append(d.Points, v.Points...)
		db.data[k] = d
	}
}

func (db *MemoryTSDB) filterMetricIDs(matchers []*labels.Matcher, sorted bool) []uint64 {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	result := make([]uint64, 0)

	for id, row := range db.data {
		if matcherMatches(matchers, row.Labels) {
			result = append(result, id)
		}
	}

	if sorted {
		sort.Slice(result, func(i, j int) bool {
			aLabels := db.data[result[i]].Labels
			bLabels := db.data[result[j]].Labels

			return labels.Compare(aLabels, bLabels) < 0
		})
	}

	return result
}

func (db *MemoryTSDB) metricsWithData(metricIDs []uint64, minT int64, maxT int64) []uint64 {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	i := 0

	for _, id := range metricIDs {
		hadData := false

		for _, pt := range db.data[id].Points {
			if pt.Timestamp >= minT && pt.Timestamp <= maxT {
				hadData = true

				break
			}
		}

		if hadData {
			metricIDs[i] = id
			i++
		}
	}

	return metricIDs[:i]
}

func (db *MemoryTSDB) getLabels(metricIDs []uint64) []labels.Labels {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	result := make([]labels.Labels, len(metricIDs))
	for i, id := range metricIDs {
		result[i] = db.data[id].Labels
	}

	return result
}

func (db *MemoryTSDB) getPointsAndLabels(id uint64, minT int64, maxT int64) ([]types.MetricPoint, labels.Labels) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	points := filterPoints(db.data[id].Points, minT, maxT)

	return points, db.data[id].Labels
}

func matcherMatches(matchers []*labels.Matcher, lbls labels.Labels) bool {
	for _, m := range matchers {
		value := lbls.Get(m.Name)
		if !m.Matches(value) {
			return false
		}
	}

	return true
}

func filterPoints(points []types.MetricPoint, mint int64, maxt int64) []types.MetricPoint {
	// Avoid allocation if all points matches
	needFilter := false

	for _, p := range points {
		if p.Timestamp < mint || p.Timestamp > maxt {
			needFilter = true

			break
		}
	}

	if !needFilter {
		return points
	}

	result := make([]types.MetricPoint, 0, len(points))

	for _, p := range points {
		if p.Timestamp < mint || p.Timestamp > maxt {
			continue
		}

		result = append(result, p)
	}

	return result
}

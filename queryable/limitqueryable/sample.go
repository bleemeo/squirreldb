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

package limitqueryable

import (
	"context"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

type querier struct {
	parent *Queryable
	inner  storage.Querier
}

func (q querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet { //nolint:lll
	if q.parent.opts.MaxEvaluatedSeries != 0 && q.parent.returnedSeries.Load() >= q.parent.opts.MaxEvaluatedSeries {
		return storage.ErrSeriesSet(ErrTooManySeries)
	}

	if q.parent.opts.ForcedMatchers != nil {
		matchers = append(matchers, q.parent.opts.ForcedMatchers...)
	}

	inner := q.inner.Select(ctx, sortSeries, hints, matchers...)

	return seriesSet{
		inner:  inner,
		parent: q.parent,
	}
}

func (q querier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) { //nolint:lll
	if q.parent.opts.ForcedMatchers != nil {
		matchers = append(matchers, q.parent.opts.ForcedMatchers...)
	}

	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q querier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) { //nolint:lll
	if q.parent.opts.ForcedMatchers != nil {
		matchers = append(matchers, q.parent.opts.ForcedMatchers...)
	}

	return q.inner.LabelNames(ctx, hints, matchers...)
}

func (q querier) Close() error {
	return q.inner.Close()
}

type seriesSet struct {
	inner  storage.SeriesSet
	parent *Queryable
}

func (s seriesSet) Next() bool {
	result := s.inner.Next()
	if !result {
		return false
	}

	count := s.parent.returnedSeries.Add(1)

	return s.parent.opts.MaxEvaluatedSeries == 0 || count <= s.parent.opts.MaxEvaluatedSeries
}

func (s seriesSet) At() storage.Series {
	inner := s.inner.At()

	return series{
		inner:  inner,
		parent: s.parent,
	}
}

func (s seriesSet) Err() error {
	if s.parent.opts.MaxEvaluatedSeries != 0 && s.parent.returnedSeries.Load() > s.parent.opts.MaxEvaluatedSeries {
		return ErrTooManySeries
	}

	return s.inner.Err()
}

func (s seriesSet) Warnings() annotations.Annotations {
	return s.inner.Warnings()
}

type series struct {
	inner  storage.Series
	parent *Queryable
}

func (s series) Labels() labels.Labels {
	return s.inner.Labels()
}

func (s series) Iterator(reuse chunkenc.Iterator) chunkenc.Iterator {
	oldIter, ok := reuse.(sampleIterator)

	if ok {
		reuse = oldIter.inner
	} else {
		reuse = nil
	}

	inner := s.inner.Iterator(reuse)

	return sampleIterator{
		inner:  inner,
		parent: s.parent,
	}
}

type sampleIterator struct {
	inner  chunkenc.Iterator
	parent *Queryable
}

func (i sampleIterator) Next() chunkenc.ValueType {
	result := i.inner.Next()
	if result == chunkenc.ValNone {
		return result
	}

	count := i.parent.returnedPoints.Add(1)
	if i.parent.opts.MaxEvaluatedPoints != 0 && count > i.parent.opts.MaxEvaluatedPoints {
		return chunkenc.ValNone
	}

	return result
}

func (i sampleIterator) Seek(t int64) chunkenc.ValueType { //nolint:govet
	result := i.inner.Seek(t)
	if result == chunkenc.ValNone {
		return result
	}

	// Here we assume the cost of Seek is 1 which might be wrong because when advancing
	// the backing iterator might reads more points. But it's the best approximation we have.
	count := i.parent.returnedPoints.Add(1)
	if i.parent.opts.MaxEvaluatedPoints != 0 && count > i.parent.opts.MaxEvaluatedPoints {
		return chunkenc.ValNone
	}

	return result
}

func (i sampleIterator) At() (int64, float64) {
	return i.inner.At()
}

func (i sampleIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return i.inner.AtHistogram(h)
}

func (i sampleIterator) AtFloatHistogram(h *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return i.inner.AtFloatHistogram(h)
}

func (i sampleIterator) AtT() int64 {
	return i.inner.AtT()
}

func (i sampleIterator) Err() error {
	if i.parent.opts.MaxEvaluatedPoints != 0 && i.parent.returnedPoints.Load() > i.parent.opts.MaxEvaluatedPoints {
		return ErrTooManyPoints
	}

	return i.inner.Err()
}

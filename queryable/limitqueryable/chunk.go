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
	"errors"
	"fmt"

	"github.com/bleemeo/squirreldb/queryable/fixedqueryable"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

var ErrAssertion = errors.New("assertion error")

type chunkQuerier struct {
	parent *Queryable
	inner  storage.ChunkQuerier
}

func (q chunkQuerier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.ChunkSeriesSet {
	if q.parent.opts.MaxEvaluatedSeries != 0 && q.parent.returnedSeries.Load() >= q.parent.opts.MaxEvaluatedSeries {
		return fixedqueryable.NewErrorChunkSeriesSet(ErrTooManySeries)
	}

	if q.parent.opts.ForcedMatchers != nil {
		matchers = append(matchers, q.parent.opts.ForcedMatchers...)
	}

	inner := q.inner.Select(ctx, sortSeries, hints, matchers...)

	return chunkSeriesSet{
		inner:  inner,
		parent: q.parent,
	}
}

func (q chunkQuerier) LabelValues(
	ctx context.Context,
	name string,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q chunkQuerier) LabelNames(
	ctx context.Context,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	return q.inner.LabelNames(ctx, hints, matchers...)
}

func (q chunkQuerier) Close() error {
	return q.inner.Close()
}

type chunkSeriesSet struct {
	inner  storage.ChunkSeriesSet
	parent *Queryable
}

func (s chunkSeriesSet) Next() bool {
	result := s.inner.Next()
	if !result {
		return false
	}

	count := s.parent.returnedSeries.Add(1)

	return s.parent.opts.MaxEvaluatedSeries == 0 || count <= s.parent.opts.MaxEvaluatedSeries
}

func (s chunkSeriesSet) At() storage.ChunkSeries {
	inner := s.inner.At()

	return chunkSeries{
		inner:  inner,
		parent: s.parent,
	}
}

func (s chunkSeriesSet) Err() error {
	if s.parent.opts.MaxEvaluatedSeries != 0 && s.parent.returnedSeries.Load() > s.parent.opts.MaxEvaluatedSeries {
		return ErrTooManySeries
	}

	return s.inner.Err()
}

func (s chunkSeriesSet) Warnings() annotations.Annotations {
	return s.inner.Warnings()
}

type chunkSeries struct {
	inner  storage.ChunkSeries
	parent *Queryable
}

func (s chunkSeries) Labels() labels.Labels {
	return s.inner.Labels()
}

func (s chunkSeries) Iterator(reuse chunks.Iterator) chunks.Iterator {
	oldIter, ok := reuse.(*chunkIterator)

	if ok {
		reuse = oldIter.inner
	} else {
		reuse = nil
	}

	inner := s.inner.Iterator(reuse)

	return &chunkIterator{
		inner:  inner,
		parent: s.parent,
	}
}

type chunkIterator struct {
	inner  chunks.Iterator
	parent *Queryable
	err    error
}

func (i *chunkIterator) Next() bool {
	if i.err != nil {
		return false
	}

	result := i.inner.Next()
	if !result {
		return false
	}

	at := i.inner.At()

	if at.Chunk != nil {
		count := i.parent.returnedPoints.Add(uint64(at.Chunk.NumSamples())) //nolint:gosec
		if i.parent.opts.MaxEvaluatedPoints != 0 && count > i.parent.opts.MaxEvaluatedPoints {
			return false
		}
	} else {
		i.err = fmt.Errorf("%w: Meta.Chunk is assumed to never be nil after Next()", ErrAssertion)

		return false
	}

	return result
}

func (i *chunkIterator) At() chunks.Meta {
	return i.inner.At()
}

func (i *chunkIterator) Err() error {
	if i.err != nil {
		return i.err
	}

	if i.parent.opts.MaxEvaluatedPoints != 0 && i.parent.returnedPoints.Load() > i.parent.opts.MaxEvaluatedPoints {
		return ErrTooManyPoints
	}

	return i.inner.Err()
}

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

package source

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// alignUp returns the smallest multiple of step greater than or equal to t.
func alignUp(t int64, step int64) int64 {
	rem := t % step
	if rem == 0 {
		return t
	}

	if rem < 0 {
		// Go's remainder has the sign of the dividend: t - rem is above t.
		return t - rem
	}

	if t > math.MaxInt64-step {
		return math.MaxInt64
	}

	return t - rem + step
}

// iterator is a chunkenc.Iterator over the points of a Series between mint and
// maxt (both included). Timestamps are multiples of the series step.
type iterator struct {
	ctx        context.Context //nolint:containedctx // values are computed lazily, in the context of the Select.
	series     Series
	maxt       int64
	first      int64
	current    int64
	started    bool
	pointsRead *atomic.Uint64
}

func newIterator(ctx context.Context, series Series, mint, maxt int64, pointsRead *atomic.Uint64) *iterator {
	return &iterator{
		ctx:        ctx,
		series:     series,
		maxt:       maxt,
		first:      alignUp(mint, series.Step),
		pointsRead: pointsRead,
	}
}

func (it *iterator) valueType() chunkenc.ValueType {
	if it.started && it.current <= it.maxt {
		return chunkenc.ValFloat
	}

	return chunkenc.ValNone
}

func (it *iterator) Next() chunkenc.ValueType {
	switch {
	case !it.started:
		it.started = true
		it.current = it.first
	case it.current <= it.maxt:
		if it.current > math.MaxInt64-it.series.Step {
			it.current = math.MaxInt64
		} else {
			it.current += it.series.Step
		}
	}

	return it.valueType()
}

// Seek advances the iterator to the first point at or after t. It never moves
// backward.
func (it *iterator) Seek(t int64) chunkenc.ValueType { //nolint:govet // stdmethods: chunkenc.Iterator's Seek signature.
	if it.started && (it.current >= t || it.current > it.maxt) {
		return it.valueType()
	}

	it.current = max(alignUp(t, it.series.Step), it.first)
	it.started = true

	return it.valueType()
}

func (it *iterator) At() (int64, float64) {
	if it.pointsRead != nil {
		it.pointsRead.Add(1)
	}

	return it.current, it.series.Value(it.ctx, it.current)
}

func (it *iterator) AtT() int64 {
	return it.current
}

func (it *iterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return it.current, nil
}

func (it *iterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return it.current, nil
}

func (it *iterator) Err() error {
	return nil
}

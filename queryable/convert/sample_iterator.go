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

package convert

import (
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// SampleIterator is a chunkenc.Iterator over a slice of float samples, shared by
// every backend's storage.Series implementation. Points are assumed sorted by
// timestamp in ascending order. Histograms are not supported.
type SampleIterator struct {
	points []types.MetricPoint
	offset int
}

// NewSampleIterator returns an iterator positioned before the first sample, so
// the first Next advances to points[0].
func NewSampleIterator(points []types.MetricPoint) *SampleIterator {
	return &SampleIterator{points: points, offset: -1}
}

// Next advances the iterator by one and returns the type of the value at the new
// position (or ValNone if the iterator is exhausted).
func (it *SampleIterator) Next() chunkenc.ValueType {
	if it.offset+1 >= len(it.points) {
		return chunkenc.ValNone
	}

	it.offset++

	return chunkenc.ValFloat
}

// Seek advances the iterator forward to the first sample with a timestamp equal
// or greater than t. If the current sample already satisfies this, Seek has no
// effect. Returns ValNone once the iterator is exhausted.
func (it *SampleIterator) Seek(t int64) chunkenc.ValueType { //nolint:govet
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

// At returns the current timestamp/value pair. Before the iterator has advanced,
// the behaviour is unspecified.
func (it *SampleIterator) At() (int64, float64) {
	return it.points[it.offset].Timestamp, it.points[it.offset].Value
}

// AtT returns the current timestamp. Before the iterator has advanced, the
// behaviour is unspecified.
func (it *SampleIterator) AtT() int64 {
	return it.points[it.offset].Timestamp
}

func (it *SampleIterator) AtST() int64 {
	return 0
}

// AtHistogram is not implemented (float samples only).
func (it *SampleIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram is not implemented (float samples only).
func (it *SampleIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// Err returns the current error. It should be used only after the iterator is
// exhausted, that is Next or Seek returned ValNone.
func (it *SampleIterator) Err() error { return nil }

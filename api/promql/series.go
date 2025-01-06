// Copyright 2015-2025 Bleemeo
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

package promql

import (
	"fmt"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

type seriesIter struct {
	list      types.MetricDataSet
	index     types.Index
	err       error
	id2Labels map[types.MetricID]labels.Labels
	current   series
}

func (i *seriesIter) Next() bool {
	if i.list == nil {
		return false
	}

	if i.err != nil {
		return false
	}

	if !i.list.Next() {
		return false
	}

	i.current = series{
		data: i.list.At(),
	}

	var ok bool

	if i.current.labels, ok = i.id2Labels[i.current.data.ID]; !ok {
		i.err = fmt.Errorf("metric ID %d don't have labels", i.current.data.ID)

		return false
	}

	return true
}

func (i *seriesIter) At() storage.Series {
	return i.current
}

func (i *seriesIter) Err() error {
	if i.err != nil {
		return i.err
	}

	if i.list == nil {
		return nil
	}

	return i.list.Err()
}

func (i *seriesIter) Warnings() annotations.Annotations {
	return nil
}

type series struct {
	labels []labels.Label
	data   types.MetricData
}

func (s series) Labels() labels.Labels {
	return s.labels
}

// Iterator returns an iterator of the data of the series.
// The iterator passed as argument is for re-use, if not nil.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
func (s series) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return &seriesSample{
		data:   s.data,
		offset: -1,
	}
}

type seriesSample struct {
	data   types.MetricData
	offset int
}

// Next advances the iterator by one and returns the type of the value
// at the new position (or ValNone if the iterator is exhausted).
func (s *seriesSample) Next() chunkenc.ValueType {
	if s.offset+1 >= len(s.data.Points) {
		return chunkenc.ValNone
	}

	s.offset++

	return chunkenc.ValFloat
}

// Seek advances the iterator forward to the first sample with a
// timestamp equal or greater than t. If the current sample found by a
// previous `Next` or `Seek` operation already has this property, Seek
// has no effect. If a sample has been found, Seek returns the type of
// its value. Otherwise, it returns ValNone, after with the iterator is
// exhausted.
func (s *seriesSample) Seek(t int64) chunkenc.ValueType {
	for ; s.offset < len(s.data.Points); s.offset++ {
		if s.data.Points[s.offset].Timestamp >= t {
			return chunkenc.ValFloat
		}
	}

	s.offset = len(s.data.Points) - 1

	return chunkenc.ValNone
}

// At returns the current timestamp/value pair.
// Before the iterator has advanced At behaviour is unspecified.
func (s *seriesSample) At() (int64, float64) {
	return s.data.Points[s.offset].Timestamp, s.data.Points[s.offset].Value
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behaviour is unspecified.
func (s *seriesSample) AtT() int64 {
	return s.data.Points[s.offset].Timestamp
}

// AtHistogram returns the current timestamp/value pair if the value is
// a histogram with integer counts.
// Not implemented.
func (s *seriesSample) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts.
// Not implemented.
func (s *seriesSample) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// Err returns the current error. It should be used only after iterator is
// exhausted, that is `Next` or `Seek` returns false.
func (s *seriesSample) Err() error {
	return nil
}

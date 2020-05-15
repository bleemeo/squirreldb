package promql

import (
	"squirreldb/types"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type seriesIter struct {
	list      types.MetricDataSet
	index     types.Index
	id2Labels map[types.MetricID][]labels.Label
	current   series
	err       error
}

func (i *seriesIter) Next() bool {
	if !i.list.Next() {
		return false
	}

	i.current = series{
		data: i.list.At(),
	}

	var ok bool

	if i.current.labels, ok = i.id2Labels[i.current.data.ID]; !ok {
		dataLabels, err := i.index.LookupLabels(i.current.data.ID)
		if err != nil {
			i.err = err
			return false
		}

		i.current.labels = make([]labels.Label, len(dataLabels))

		for j, l := range dataLabels {
			i.current.labels[j] = labels.Label{
				Name:  l.Name,
				Value: l.Value,
			}
		}
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

	return i.list.Err()
}

type series struct {
	data   types.MetricData
	labels []labels.Label
}

func (s series) Labels() labels.Labels {
	return s.labels
}

func (s series) Iterator() chunkenc.Iterator {
	return &seriesSample{
		data:   s.data,
		offset: -1,
	}
}

type seriesSample struct {
	data   types.MetricData
	offset int
}

// Next advances the iterator by one.
func (s *seriesSample) Next() bool {
	if s.offset+1 >= len(s.data.Points) {
		return false
	}

	s.offset++

	return true
}

// Seek advances the iterator forward to the first sample with the timestamp equal or greater than t.
// If current sample found by previous `Next` or `Seek` operation already has this property, Seek has no effect.
// Seek returns true, if such sample exists, false otherwise.
// Iterator is exhausted when the Seek returns false.
func (s *seriesSample) Seek(t int64) bool {
	for ; s.offset < len(s.data.Points); s.offset++ {
		if s.data.Points[s.offset].Timestamp >= t {
			return true
		}
	}

	s.offset = len(s.data.Points) - 1

	return false
}

// At returns the current timestamp/value pair.
// Before the iterator has advanced At behaviour is unspecified.
func (s *seriesSample) At() (int64, float64) {
	return s.data.Points[s.offset].Timestamp, s.data.Points[s.offset].Value
}

// Err returns the current error. It should be used only after iterator is
// exhausted, that is `Next` or `Seek` returns false.
func (s *seriesSample) Err() error {
	return nil
}

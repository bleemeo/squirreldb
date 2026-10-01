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

// Package file is the sandbox source replaying recorded samples: each series
// of the metric lists is associated with recorded samples, which are repeated
// in an endless loop aligned on the Unix epoch.
package file

import (
	"context"
	"errors"
	"fmt"

	"github.com/bleemeo/squirreldb/sandbox/source"

	"github.com/prometheus/prometheus/model/labels"
)

var (
	ErrEmptyConfig        = errors.New("empty configuration. At least one metric is required")
	ErrInvalidStepSeconds = errors.New("step seconds should be a strictly positive integer")
	ErrMissingSample      = errors.New("missing samples")
	ErrNoSampleValue      = errors.New("samples must contain at least one value")
)

// WithoutLabels returns a new labels.Labels from ls that contains labels not matching names.
// 'names' have to be sorted in ascending order.
// It's a copy from Prometheus code but which don't drop __name__.
func WithoutLabels(ls labels.Labels, names ...string) labels.Labels {
	if len(names) == 0 {
		return ls
	}

	b := labels.NewBuilder(labels.EmptyLabels())
	j := 0

	ls.Range(func(l labels.Label) {
		for j < len(names) && names[j] < l.Name {
			j++
		}

		if j < len(names) && l.Name == names[j] {
			return
		}

		b.Set(l.Name, l.Value)
	})

	return b.Labels()
}

// Store is the content of the file source: the series and the samples they use.
type Store struct {
	Metrics []Metric
	Samples []Samples
}

// Metric is a series of the metric lists.
type Metric struct {
	Labels labels.Labels
	// MatchingLabels are the labels that must be equal between the metric and its
	// samples. It contains __name__ and is sorted.
	MatchingLabels []string
	Samples        Samples
}

// Samples are recorded samples, one every StepSeconds.
type Samples struct {
	Labels      labels.Labels
	StepSeconds int
	Samples     []float64
}

// Hash return a hash value that should yield the same value for two related metrics.
// For example node_cpu_seconds_total{cpu="7",mode="irq"} and node_cpu_seconds_total{cpu="7",mode="steal"} are related.
func (m Metric) Hash() uint64 {
	lbls := WithoutLabels(m.Labels, m.MatchingLabels...)

	return lbls.Hash()
}

// ValueAt returns the value of the metric at timestamp ts (in milliseconds).
//
// The samples are reproduced in a cycle forever and the sample picked for a
// given timestamp is always the same. Two related metrics (see Hash) must look
// alike without being identical, so they don't use the same sample at the same
// time: the sample picked at the epoch (ts == 0) is based on Hash, then the
// cycle goes on with one sample per step.
func (m Metric) ValueAt(ts int64) float64 {
	return m.valueAt(m.Hash()%uint64(len(m.Samples.Samples)), ts)
}

func (m Metric) valueAt(epochSampleIndex uint64, ts int64) float64 {
	count := int64(len(m.Samples.Samples))
	pointsSinceEpoch := floorDiv(ts, int64(m.Samples.StepSeconds)*1000)

	//nolint:gosec // epochSampleIndex < count, which fits an int64.
	idx := (int64(epochSampleIndex) + pointsSinceEpoch%count + count) % count

	return m.Samples.Samples[idx]
}

// floorDiv returns a / b rounded toward negative infinity (b > 0).
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b < 0 {
		q--
	}

	return q
}

// Series returns the store metrics as sandbox series.
func (s *Store) Series() []source.Series {
	result := make([]source.Series, len(s.Metrics))

	for i, m := range s.Metrics {
		epochSampleIndex := m.Hash() % uint64(len(m.Samples.Samples))

		result[i] = source.Series{
			Labels: m.Labels,
			Step:   int64(m.Samples.StepSeconds) * 1000,
			Value: func(_ context.Context, ts int64) float64 {
				return m.valueAt(epochSampleIndex, ts)
			},
		}
	}

	return result
}

// matchSamples set the samples for every metrics. It will delete any metrics
// that don't have samples (so after this call, all s.Metrics will have samples).
// It returns a warning for each deleted metric.
func (s *Store) matchSamples() []error {
	var warnings []error

	keptMetrics := make([]Metric, 0, len(s.Metrics))

	for _, m := range s.Metrics {
		var allSamples []Samples

		for _, sample := range s.Samples {
			match := true

			for _, name := range m.MatchingLabels {
				if m.Labels.Get(name) != sample.Labels.Get(name) {
					match = false

					break
				}
			}

			if match {
				allSamples = append(allSamples, sample)
			}
		}

		if len(allSamples) == 0 {
			warnings = append(warnings, fmt.Errorf("metric %s: %w", m.Labels.String(), ErrMissingSample))
		} else {
			m.Samples = allSamples[m.Hash()%uint64(len(allSamples))]
			keptMetrics = append(keptMetrics, m)
		}
	}

	s.Metrics = keptMetrics

	return warnings
}

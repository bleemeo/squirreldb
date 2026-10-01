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

package file

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/bleemeo/squirreldb/sandbox/dataset"
	"github.com/bleemeo/squirreldb/sandbox/source"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// memoryOpen returns an OpenFunc serving files from memory.
func memoryOpen(files map[string]string) OpenFunc {
	return func(name string) (io.ReadCloser, error) {
		content, ok := files[name]
		if !ok {
			return nil, os.ErrNotExist
		}

		return io.NopCloser(strings.NewReader(content)), nil
	}
}

func testStore(t *testing.T) Store {
	t.Helper()

	files := map[string]string{
		"metrics.txt": `# comment
cpu_used{instance="server1"}
cpu_used{instance="server2"}
node_cpu_seconds_total{cpu="0",mode="idle"}
node_cpu_seconds_total{cpu="0",mode="user"}
missing_samples{instance="server1"}
`,
		"samples.json": `{"labels":{"__name__":"cpu_used"},"step_seconds":10,"samples":["1","2","3","4"]}
{"labels":{"__name__":"node_cpu_seconds_total","mode":"idle"},"step_seconds":10,"samples":["10","20","30"]}
{"labels":{"__name__":"node_cpu_seconds_total","mode":"user"},"step_seconds":10,"samples":["100","200","300"]}
`,
	}

	store, warnings, err := Load(dataset.Files{
		Metrics: []dataset.MetricsFile{{File: "metrics.txt", MatchingLabels: []string{"mode"}}},
		Samples: []dataset.SamplesFile{{File: "samples.json"}},
	}, memoryOpen(files))
	if err != nil {
		t.Fatal(err)
	}

	if len(warnings) != 1 || !errors.Is(warnings[0], ErrMissingSample) {
		t.Errorf("warnings = %v, want one ErrMissingSample", warnings)
	}

	if len(store.Metrics) != 4 {
		t.Fatalf("got %d metrics, want 4 (missing_samples must be dropped)", len(store.Metrics))
	}

	return store
}

func TestMatchSamples(t *testing.T) {
	t.Parallel()

	store := testStore(t)

	for _, m := range store.Metrics {
		if m.Labels.Get("mode") != m.Samples.Labels.Get("mode") {
			t.Errorf("metric %s uses samples %s: matching label mode differs", m.Labels, m.Samples.Labels)
		}
	}
}

// TestValueDeterminism checks that a value only depends on the timestamp: the
// samples are replayed in a loop aligned on the Unix epoch, whatever the query
// range.
func TestValueDeterminism(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	series := source.Merge(store.Series())
	queryable := source.Queryable{Series: series}

	read := func(mint, maxt int64) map[string]map[int64]float64 {
		q, err := queryable.Querier(mint, maxt)
		if err != nil {
			t.Fatal(err)
		}

		defer q.Close()

		result := make(map[string]map[int64]float64)
		set := q.Select(t.Context(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))

		for set.Next() {
			points := make(map[int64]float64)
			it := set.At().Iterator(nil)

			for it.Next() == chunkenc.ValFloat {
				ts, v := it.At()

				if ts%10000 != 0 {
					t.Errorf("timestamp %d is not aligned on the 10s step", ts)
				}

				points[ts] = v
			}

			result[set.At().Labels().String()] = points
		}

		return result
	}

	const t0 = int64(1700000000000)

	full := read(t0, t0+120000)
	shifted := read(t0+33000, t0+77000)

	for name, points := range shifted {
		// +40s, +50s, +60s and +70s.
		if len(points) != 4 {
			t.Errorf("%s: got %d points between +33s and +77s, want 4", name, len(points))
		}

		for ts, v := range points {
			if full[name][ts] != v {
				t.Errorf("%s at %d: got %v, want %v (same as with another range)", name, ts, v, full[name][ts])
			}
		}
	}

	// The samples are a cycle: one sample per step, starting from an offset based on the labels.
	for _, m := range store.Metrics {
		count := int64(len(m.Samples.Samples))
		step := int64(m.Samples.StepSeconds) * 1000

		for _, ts := range []int64{0, t0, -3 * step} {
			if m.ValueAt(ts) != m.ValueAt(ts+count*step) {
				t.Errorf("%s: value at %d differs from the value one cycle later", m.Labels, ts)
			}

			next := m.ValueAt(ts + step)
			idx := -1

			for i, v := range m.Samples.Samples {
				if v == m.ValueAt(ts) {
					idx = i
				}
			}

			if want := m.Samples.Samples[(idx+1)%int(count)]; next != want {
				t.Errorf("%s: value at %d is %v, want the next sample %v", m.Labels, ts+step, next, want)
			}
		}
	}

	// The sample used at the epoch depends on the labels hash.
	for _, m := range store.Metrics {
		want := m.Samples.Samples[m.Hash()%uint64(len(m.Samples.Samples))]
		if got := m.ValueAt(0); got != want {
			t.Errorf("%s: value at epoch = %v, want %v", m.Labels, got, want)
		}
	}
}

func TestLoadEmbeddedDataset(t *testing.T) {
	t.Parallel()

	ds, err := dataset.Load("")
	if err != nil {
		t.Fatal(err)
	}

	store, warnings, err := Load(ds.Config.Files, ds.Open)
	if err != nil {
		t.Fatal(err)
	}

	for _, w := range warnings {
		t.Logf("warning: %v", w)
	}

	found := false

	for _, m := range store.Metrics {
		if m.Labels.Get(labels.MetricName) == "agent_status" {
			found = true
		}
	}

	if !found {
		t.Error("agent_status not found in the embedded dataset")
	}
}

func TestLoadErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		samples string
		wantErr error
	}{
		{
			name:    "invalid step",
			samples: `{"labels":{"__name__":"cpu_used"},"step_seconds":0,"samples":["1"]}`,
			wantErr: ErrInvalidStepSeconds,
		},
		{
			name:    "no sample",
			samples: `{"labels":{"__name__":"cpu_used"},"step_seconds":10,"samples":[]}`,
			wantErr: ErrNoSampleValue,
		},
		{
			name:    "no matching sample",
			samples: `{"labels":{"__name__":"other"},"step_seconds":10,"samples":["1"]}`,
			wantErr: ErrEmptyConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			files := map[string]string{
				"metrics.txt":  `cpu_used{instance="server1"}`,
				"samples.json": tt.samples,
			}

			_, _, err := Load(dataset.Files{
				Metrics: []dataset.MetricsFile{{File: "metrics.txt"}},
				Samples: []dataset.SamplesFile{{File: "samples.json"}},
			}, memoryOpen(files))
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Load() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

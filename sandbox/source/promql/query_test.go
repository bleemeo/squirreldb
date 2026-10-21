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

package promql

import (
	"context"
	"testing"

	"github.com/bleemeo/squirreldb/sandbox/dataset"
	"github.com/bleemeo/squirreldb/sandbox/source"

	"github.com/prometheus/prometheus/model/labels"
)

// TestQueryOnOtherSeries checks that an expression may use the series of the
// queryable it is evaluated against, and that an expression returning more
// than one sample yields 0.
func TestQueryOnOtherSeries(t *testing.T) {
	t.Parallel()

	lgr := testLogger(t)

	constant := func(lbls labels.Labels, value float64) source.Series {
		return source.Series{
			Labels: lbls,
			Step:   10000,
			Value:  func(context.Context, int64) float64 { return value },
		}
	}

	fileSeries := source.Merge([]source.Series{
		constant(labels.FromStrings("__name__", "up", "instance", "a"), 2),
		constant(labels.FromStrings("__name__", "up", "instance", "b"), 5),
	})

	metrics := []Metric{
		testMetric(t, `total_up`, 10, "sum(up) * 3"),
		testMetric(t, `bad`, 10, "up"),
	}

	series := Series(metrics, NewEngine(lgr), source.Queryable{Series: fileSeries}, lgr)

	if got := series[0].Value(t.Context(), 60000); got != 21 {
		t.Errorf("total_up = %v, want 21", got)
	}

	if got := series[1].Value(t.Context(), 60000); got != 0 {
		t.Errorf("bad = %v, want 0", got)
	}
}

func TestLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     dataset.Promql
		wantErr bool
	}{
		{
			name: "valid",
			cfg:  dataset.Promql{{Name: `expiry{instance="a"}`, StepSeconds: 10, Query: "time() + 5"}},
		},
		{
			name:    "invalid step",
			cfg:     dataset.Promql{{Name: `expiry`, StepSeconds: 0, Query: "time()"}},
			wantErr: true,
		},
		{
			name:    "invalid name",
			cfg:     dataset.Promql{{Name: `expiry{`, StepSeconds: 10, Query: "time()"}},
			wantErr: true,
		},
		{
			name:    "invalid query",
			cfg:     dataset.Promql{{Name: `expiry`, StepSeconds: 10, Query: "time() +"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := Load(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

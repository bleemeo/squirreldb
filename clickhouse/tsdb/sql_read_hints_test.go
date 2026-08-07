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

package tsdb

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// hintsRecorder is a Queryable that answers nothing and records the SelectHints it was
// called with. The hints are built before evaluation, so an empty result is enough.
type hintsRecorder struct{ seen *[]storage.SelectHints }

func (q hintsRecorder) Querier(int64, int64) (storage.Querier, error) { return q, nil }

func (q hintsRecorder) Select(
	_ context.Context, _ bool, hints *storage.SelectHints, _ ...*labels.Matcher,
) storage.SeriesSet {
	*q.seen = append(*q.seen, *hints)

	return storage.EmptySeriesSet()
}

func (q hintsRecorder) LabelValues(
	context.Context, string, *storage.LabelHints, ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q hintsRecorder) LabelNames(
	context.Context, *storage.LabelHints, ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q hintsRecorder) Close() error { return nil }

// selectHints is what aggSelection gets to decide on, plus the mode it decides.
type selectHints struct {
	fn    string
	rng   time.Duration
	step  time.Duration
	rmode aggMode
}

func modeName(m aggMode) string {
	switch m {
	case aggModeRaw:
		return "raw"
	case aggModeRange:
		return "range"
	case aggModeInstantSnap:
		return "snap"
	default:
		return "?"
	}
}

// TestSelectHintsRouting pins how a query's SHAPE reaches aggSelection, because the
// routing can only read what Prometheus chose to put in the hints, and that mapping is
// neither documented nor guessable:
//
//   - a subquery's range never appears. `m[30d:60s]` gives Range = 0, like a bare
//     selector: the read cannot know it feeds a 30-day window;
//   - Step is the INNERMOST subquery's step when there is one, not the query's step —
//     which is what routes the subquery cases;
//   - Func stops at the first enclosing call and is severed by a binary operator, so
//     `increase(clamp_max(m, 1)[...])` reports "clamp_max" and `(m > 0)[...]` reports
//     nothing at all. A function name is therefore NO protection: aggSelection only
//     consults Func on a real range vector (Range > 0).
//
// The Bleemeo status-page queries are in here because they are the ones the tolerance
// for a wrong answer is lowest on (see the probe_* cases in
// tests/squirreldb-promql-aggregate): they must keep landing on raw.
func TestSelectHintsRouting(t *testing.T) {
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:               1e6,
		Timeout:                  time.Minute,
		LookbackDelta:            5 * time.Minute,
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
	})

	end := time.Date(2026, 8, 5, 14, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		query string
		step  time.Duration
		want  []selectHints
	}{
		{
			name:  "status page incident count",
			query: `round(increase(clamp_max(probe_status{instance="a"}, 1)[30d:60s]))`,
			step:  24 * time.Hour,
			want:  []selectHints{{fn: "clamp_max", rng: 0, step: time.Minute, rmode: aggModeRaw}},
		},
		{
			name: "status page uptime",
			query: `((1 - count_over_time((probe_status{instance="a"} > 0)[30d:60s])` +
				` / count_over_time(probe_status{instance="a"}[30d:60s])) * 100)`,
			step: 24 * time.Hour,
			want: []selectHints{
				// The comparison severs the path, so the function is lost.
				{fn: "", rng: 0, step: time.Minute, rmode: aggModeRaw},
				// Here it survives — and is ignored all the same, Range being 0.
				{fn: "count_over_time", rng: 0, step: time.Minute, rmode: aggModeRaw},
			},
		},
		{
			name:  "status page incident timeline",
			query: `max_over_time(probe_status{instance="a"}[300s])`,
			step:  time.Minute,
			want:  []selectHints{{fn: "max_over_time", rng: 5 * time.Minute, step: time.Minute, rmode: aggModeRaw}},
		},
		{
			// A range vector IS where the function decides: count_over_time counts points,
			// which the bucketed series cannot reproduce, so aggValueExpr sends it to raw.
			name:  "count_over_time on a range vector",
			query: `count_over_time(cpu_used[2h])`,
			step:  time.Hour,
			want:  []selectHints{{fn: "count_over_time", rng: 2 * time.Hour, step: time.Hour, rmode: aggModeRaw}},
		},
		{
			name:  "avg_over_time on a range vector",
			query: `avg_over_time(cpu_used[2h])`,
			step:  time.Hour,
			want:  []selectHints{{fn: "avg_over_time", rng: 2 * time.Hour, step: time.Hour, rmode: aggModeRange}},
		},
		{
			name:  "bare selector on a coarse step",
			query: `cpu_used`,
			step:  time.Hour,
			want:  []selectHints{{fn: "", rng: 0, step: time.Hour, rmode: aggModeInstantSnap}},
		},
		{
			// Indistinguishable from the bare selector above, and snapped like it: the same
			// count_over_time that forces raw on a range vector does not here.
			name:  "subquery whose step reaches the threshold",
			query: `count_over_time((cpu_used > 50)[6h:1h])`,
			step:  time.Hour,
			want:  []selectHints{{fn: "", rng: 0, step: time.Hour, rmode: aggModeInstantSnap}},
		},
		{
			name:  "subquery whose step stays below it",
			query: `count_over_time((cpu_used > 50)[6h:15m])`,
			step:  time.Hour,
			want:  []selectHints{{fn: "", rng: 0, step: 15 * time.Minute, rmode: aggModeRaw}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var seen []storage.SelectHints

			query, err := engine.NewRangeQuery(
				context.Background(), hintsRecorder{&seen}, nil, tc.query, end.Add(-48*time.Hour), end, tc.step)
			if err != nil {
				t.Fatalf("NewRangeQuery: %v", err)
			}

			if res := query.Exec(context.Background()); res.Err != nil {
				t.Fatalf("Exec: %v", res.Err)
			}

			if len(seen) != len(tc.want) {
				t.Fatalf("got %d Select calls, want %d", len(seen), len(tc.want))
			}

			for i, hints := range seen {
				got := selectHints{
					fn:    hints.Func,
					rng:   time.Duration(hints.Range) * time.Millisecond,
					step:  time.Duration(hints.Step) * time.Millisecond,
					rmode: (&sqlQuerier{}).aggSelection(&hints).mode,
				}

				if got != tc.want[i] {
					t.Errorf("Select %d: got Func=%q Range=%v Step=%v -> %s, want Func=%q Range=%v Step=%v -> %s",
						i, got.fn, got.rng, got.step, modeName(got.rmode),
						tc.want[i].fn, tc.want[i].rng, tc.want[i].step, modeName(tc.want[i].rmode))
				}
			}
		})
	}
}

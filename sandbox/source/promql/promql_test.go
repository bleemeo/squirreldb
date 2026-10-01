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
	"fmt"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/sandbox/source"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/rs/zerolog"
)

type logPipe struct {
	t *testing.T
}

func (l logPipe) Write(p []byte) (n int, err error) {
	l.t.Helper()

	l.t.Log(string(p))

	return len(p), nil
}

func testLogger(t *testing.T) zerolog.Logger {
	t.Helper()

	logOutput := logPipe{t}

	consoleWriter := zerolog.ConsoleWriter{
		Out: logOutput,
	}

	return zerolog.New(consoleWriter)
}

func testMetric(t *testing.T, name string, stepSeconds int, query string) Metric {
	t.Helper()

	lbls, err := parser.ParseMetric(name)
	if err != nil {
		t.Fatalf("Failed to parse metric %q: %s", name, err)
	}

	return Metric{
		Labels:      lbls,
		StepSeconds: stepSeconds,
		Query:       query,
	}
}

func TestTimestampAlignment(t *testing.T) {
	t.Parallel()

	lgr := testLogger(t)

	const stepSeconds = 10

	const deltaBetweenTSAndValue = 5 * time.Second

	metrics := []Metric{
		testMetric(t, `probe_domain_expiry{instance="sandbox.bleemeo.com:8015"}`,
			stepSeconds, "time() + 5"),
	}

	series := Series(metrics, NewEngine(lgr), source.Queryable{}, lgr)
	queryable := source.Queryable{Series: series}

	for _, minT := range []int64{7, 10, 15} {
		matcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "probe_domain_expiry")

		t.Run(fmt.Sprintf("With_Seek_%ds", minT), func(t *testing.T) {
			t.Parallel()

			q, err := queryable.Querier(minT*1000, 100*1000)
			if err != nil {
				t.Fatal(err)
			}

			defer q.Close()

			seriesSet := q.Select(t.Context(), false, nil, matcher)

			if !seriesSet.Next() {
				t.Fatal("Failed to find the series set")
			}

			seriesIterator := seriesSet.At().Iterator(nil)

			seek := func(ts int64) {
				if seriesIterator.Seek(ts*1000) != chunkenc.ValFloat {
					t.Fatalf("Failed to seek to %ds", ts/1000)
				}
			}
			next := func() {
				if seriesIterator.Next() != chunkenc.ValFloat {
					t.Fatal("Failed to find next value")
				}
			}

			assertValue := func(value float64, ts int64) {
				expectedValue := float64(ts/1000) + deltaBetweenTSAndValue.Seconds()
				if value != expectedValue {
					t.Fatalf("Unexpected value: want %.2f, got %.2f (mint=%d)", expectedValue, value, minT)
				}
			}

			if minT <= 10 {
				seek(10)
				next()

				ts, value := seriesIterator.At()

				if ts != 20*1000 {
					t.Fatalf("Unexpected timestamp: got %ds, want 20s (mint=%d)", ts/1000, minT)
				}

				assertValue(value, ts)

				ts = seriesIterator.AtT()
				if ts != 20*1000 {
					t.Fatalf("Unexpected timestamp: got %ds, want 20s (mint=%d)", ts/1000, minT)
				}
			} else {
				seek(20)
			}

			next()

			ts, value := seriesIterator.At()

			if ts != 30*1000 {
				t.Fatalf("Unexpected timestamp: got %ds, want 30s (mint=%d)", ts/1000, minT)
			}

			assertValue(value, ts)

			ts = seriesIterator.AtT()
			if ts != 30*1000 {
				t.Fatalf("Unexpected timestamp: got %ds, want 30s (mint=%d)", ts/1000, minT)
			}

			seek(45)
			next()

			ts, value = seriesIterator.At()

			if ts != 60*1000 {
				t.Fatalf("Unexpected timestamp: got %ds, want 60s (mint=%d)", ts/1000, minT)
			}

			assertValue(value, ts)

			ts = seriesIterator.AtT()
			if ts != 60*1000 {
				t.Fatalf("Unexpected timestamp: got %ds, want 60s (mint=%d)", ts/1000, minT)
			}
		})

		t.Run(fmt.Sprintf("Without_Seek_%ds", minT), func(t *testing.T) {
			t.Parallel()

			q, err := queryable.Querier(minT*1000, 100*1000)
			if err != nil {
				t.Fatal(err)
			}

			defer q.Close()

			seriesSet := q.Select(t.Context(), false, nil, matcher)

			if !seriesSet.Next() {
				t.Fatal("Failed to find the series set")
			}

			seriesIterator := seriesSet.At().Iterator(nil)

			next := func() {
				if seriesIterator.Next() != chunkenc.ValFloat {
					t.Fatal("Failed to find next value")
				}
			}

			assertValue := func(value float64, ts int64) {
				expectedValue := float64(ts/1000) + deltaBetweenTSAndValue.Seconds()
				if value != expectedValue {
					t.Fatalf("Unexpected value: want %.2f, got %.2f (mint=%d)", expectedValue, value, minT)
				}
			}

			if minT <= 10 {
				next()

				ts, value := seriesIterator.At()

				if ts != 10*1000 {
					t.Fatalf("Unexpected timestamp: got %ds, want 10s (mint=%d)", ts/1000, minT)
				}

				assertValue(value, ts)

				ts = seriesIterator.AtT()
				if ts != 10*1000 {
					t.Fatalf("Unexpected timestamp: got %ds, want 10s (mint=%d)", ts/1000, minT)
				}
			}

			next()

			ts, value := seriesIterator.At()

			if ts != 20*1000 {
				t.Fatalf("Unexpected timestamp: got %ds, want 20s (mint=%d)", ts/1000, minT)
			}

			assertValue(value, ts)

			ts = seriesIterator.AtT()
			if ts != 20*1000 {
				t.Fatalf("Unexpected timestamp: got %ds, want 20s (mint=%d)", ts/1000, minT)
			}
		})
	}
}

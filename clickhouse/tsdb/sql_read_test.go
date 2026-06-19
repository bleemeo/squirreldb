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
	"testing"

	"github.com/bleemeo/squirreldb/types"

	"github.com/google/go-cmp/cmp"
)

func pts(tsValues ...int64) []types.MetricPoint {
	out := make([]types.MetricPoint, 0, len(tsValues)/2)
	for i := 0; i+1 < len(tsValues); i += 2 {
		out = append(out, types.MetricPoint{Timestamp: tsValues[i], Value: float64(tsValues[i+1])})
	}

	return out
}

func TestMergePoints(t *testing.T) {
	cases := []struct {
		name             string
		flushed, pending []types.MetricPoint
		want             []types.MetricPoint
	}{
		{"both empty", nil, nil, nil},
		{"flushed only", pts(1, 10, 2, 20), nil, pts(1, 10, 2, 20)},
		{"pending only", nil, pts(3, 30), pts(3, 30)},
		{
			name:    "interleaved",
			flushed: pts(1, 10, 4, 40),
			pending: pts(2, 20, 3, 30),
			want:    pts(1, 10, 2, 20, 3, 30, 4, 40),
		},
		{
			// Pending (recent) continues after flushed; typical batcher case.
			name:    "pending after flushed",
			flushed: pts(1, 10, 2, 20),
			pending: pts(3, 30, 4, 40),
			want:    pts(1, 10, 2, 20, 3, 30, 4, 40),
		},
		{
			// A point present in both (flush in progress): deduplicated, flushed wins.
			name:    "duplicate timestamp deduped",
			flushed: pts(1, 10, 2, 20),
			pending: pts(2, 99, 3, 30),
			want:    pts(1, 10, 2, 20, 3, 30),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mergePoints(tc.flushed, tc.pending)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("mergePoints mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func genPoints(startTS, count, step int64) []types.MetricPoint {
	out := make([]types.MetricPoint, count)
	for i := range count {
		out[i] = types.MetricPoint{Timestamp: startTS + i*step, Value: float64(i)}
	}

	return out
}

// TestMergePointsCappedDiffersOnOutOfOrder documents why we cannot use the
// Cassandra cap-and-concat in ClickHouse: when a flushed point is newer than the
// earliest pending one (backdated/out-of-order), cap-and-concat drops it whereas
// mergePoints keeps it.
func TestMergePointsCappedDiffersOnOutOfOrder(t *testing.T) {
	flushed := pts(1, 1, 2, 2, 5, 5) // a flushed point at ts=5
	pending := pts(3, 30, 4, 40)     // earliest pending ts=3

	merged := mergePoints(flushed, pending)

	// mergePoints keeps the flushed ts=5; cap-and-concat drops it.
	if len(merged) != 5 {
		t.Errorf("mergePoints lost a point: got %d, want 5", len(merged))
	}
}

// BenchmarkMergePointsCommonCase benchmarks our merge-sort+dedup on a realistic shape:
// a 52 minutes in flushed range plus a small (8 minutes) recent
// pending tail, disjoint.
func BenchmarkMergePointsCommonCase(b *testing.B) {
	const step = 10_000 // 10s in ms

	flushed := genPoints(0, 52*60/10, step)              // 52 minutes of flushed points
	pending := genPoints((52*60/10)*step, 8*60/10, step) // 8min recent tail
	expected := len(flushed) + len(pending)

	if got := len(mergePoints(flushed, pending)); got != expected {
		b.Fatalf("setup: mergePoints = %d, want %d", got, expected)
	}

	b.Run("mergeSortDedup", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_ = mergePoints(flushed, pending)
		}
	})
}

// BenchmarkMergePointsBackfillCase benchmarks our merge-sort+dedup on a backfilling case:
// a 30 minutes in flushed range plus a same-size (30 minutes) of older datan in pending, disjoint.
func BenchmarkMergePointsBackfillCase(b *testing.B) {
	const step = 10_000 // 10s in ms

	flushed := genPoints((30*60/10)*step, 30*60/10, step)
	pending := genPoints(0, 30*60/10, step)
	expected := len(flushed) + len(pending)

	if got := len(mergePoints(flushed, pending)); got != expected {
		b.Fatalf("setup: mergePoints = %d, want %d", got, expected)
	}

	b.Run("mergeSortDedup", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_ = mergePoints(flushed, pending)
		}
	})
}

// BenchmarkMergePointsInterleaved benchmarks our merge-sort+dedup on a interleave case:
// flushed & pending contains both 60 minutes, but both one points every 2 x step.
func BenchmarkMergePointsInterleaved(b *testing.B) {
	const step = 10_000 // 10s in ms

	flushed := genPoints(0, 60*60/20, 2*step)
	pending := genPoints(step, 60*60/20, 2*step)
	expected := len(flushed) + len(pending)

	if got := len(mergePoints(flushed, pending)); got != expected {
		b.Fatalf("setup: mergePoints = %d, want %d", got, expected)
	}

	b.Run("mergeSortDedup", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_ = mergePoints(flushed, pending)
		}
	})
}

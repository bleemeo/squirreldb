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
	"math"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/google/go-cmp/cmp"
)

func TestClampStoreTime(t *testing.T) {
	normal := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC).UnixMilli()
	// The queryable bounds the window to [Unix(0), year 3000]; year 3000 is past
	// the DateTime64 max and must be clamped down rather than overflow.
	year3000 := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

	cases := []struct {
		name string
		in   int64
		want int64
	}{
		{"in range", normal, normal},
		{"at min", storeMinTimeMillis, storeMinTimeMillis},
		{"at max", storeMaxTimeMillis, storeMaxTimeMillis},
		{"below min clamps up", storeMinTimeMillis - 1, storeMinTimeMillis},
		{"above max clamps down", storeMaxTimeMillis + 1, storeMaxTimeMillis},
		{"queryable year 3000 sentinel", year3000, storeMaxTimeMillis},
		{"min int64", math.MinInt64, storeMinTimeMillis},
		{"max int64", math.MaxInt64, storeMaxTimeMillis},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampStoreTime(tc.in); got != tc.want {
				t.Errorf("clampStoreTime(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func pts(tsValues ...int64) []types.MetricPoint {
	out := make([]types.MetricPoint, 0, len(tsValues)/2)
	for i := 0; i+1 < len(tsValues); i += 2 {
		out = append(out, types.MetricPoint{Timestamp: tsValues[i], Value: float64(tsValues[i+1])})
	}

	return out
}

func TestDedupSortedPoints(t *testing.T) {
	cases := []struct {
		name string
		in   []types.MetricPoint
		want []types.MetricPoint
	}{
		{"empty", nil, nil},
		{"single", pts(1, 10), pts(1, 10)},
		{"no duplicate", pts(1, 10, 2, 20, 3, 30), pts(1, 10, 2, 20, 3, 30)},
		{
			// A re-flushed point appears twice for the same timestamp; the first
			// (here value 10) wins and the duplicate is dropped.
			name: "adjacent duplicate keeps first",
			in:   pts(1, 10, 1, 99, 2, 20),
			want: pts(1, 10, 2, 20),
		},
		{
			name: "duplicate at end",
			in:   pts(1, 10, 2, 20, 2, 21),
			want: pts(1, 10, 2, 20),
		},
		{
			name: "triple duplicate",
			in:   pts(5, 50, 5, 51, 5, 52),
			want: pts(5, 50),
		},
		{
			name: "all duplicate then distinct",
			in:   pts(1, 10, 1, 11, 1, 12, 2, 20, 2, 21),
			want: pts(1, 10, 2, 20),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dedupSortedPoints(tc.in)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("dedupSortedPoints mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

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

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

package backend

import (
	"reflect"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/types"
)

func TestReconstructWriteTTL(t *testing.T) {
	t.Parallel()

	now := time.Unix(1000, 0) // 1000s since epoch

	cases := []struct {
		name       string
		ttlSeconds int64
		points     []types.MetricPoint
		want       time.Duration
	}{
		{
			name:       "no source ttl maps to zero",
			ttlSeconds: 0,
			points:     []types.MetricPoint{{Timestamp: 900_000}},
			want:       0,
		},
		{
			name:       "no points maps to zero",
			ttlSeconds: 300,
			points:     nil,
			want:       0,
		},
		{
			// Points arrive newest-first (mergePoints sorts descending), so points[0]
			// (900s) is the newest; age = 1000-900 = 100s. Raw seconds, no day rounding
			// here — rowTTLDays quantizes.
			name:       "retention = remaining + age of newest point (points[0])",
			ttlSeconds: 300,
			points:     []types.MetricPoint{{Timestamp: 900_000}, {Timestamp: 800_000}},
			want:       (300 + 100) * time.Second,
		},
		{
			name:       "future point floors age at zero",
			ttlSeconds: 300,
			points:     []types.MetricPoint{{Timestamp: 1_100_000}}, // 1100s, after now
			want:       300 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := reconstructWriteTTL(tc.ttlSeconds, tc.points, now); got != tc.want {
				t.Errorf("reconstructWriteTTL() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIdsAfter(t *testing.T) {
	t.Parallel()

	ids := []types.MetricID{2, 5, 8, 13}

	cases := []struct {
		after types.MetricID
		want  []types.MetricID
	}{
		{after: 0, want: []types.MetricID{2, 5, 8, 13}}, // whole slice
		{after: 5, want: []types.MetricID{8, 13}},       // strictly after an existing id
		{after: 6, want: []types.MetricID{8, 13}},       // after a gap
		{after: 13, want: []types.MetricID{}},           // after the last
		{after: 99, want: []types.MetricID{}},           // beyond the end
	}

	for _, tc := range cases {
		got := idsAfter(ids, tc.after)
		if len(got) == 0 && len(tc.want) == 0 {
			continue
		}

		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("idsAfter(ids, %d) = %v, want %v", tc.after, got, tc.want)
		}
	}
}

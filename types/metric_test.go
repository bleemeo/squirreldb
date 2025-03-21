// Copyright 2015-2025 Bleemeo
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

package types

import (
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/model/value"
)

func TestDeduplicatePoints(t *testing.T) { //nolint:maintidx
	type args struct {
		points []MetricPoint
	}

	tests := []struct {
		name string
		args args
		want []MetricPoint
	}{
		{
			name: "no_duplicated_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 20000,
						Value:     30,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 0,
					Value:     10,
				},
				{
					Timestamp: 10000,
					Value:     20,
				},
				{
					Timestamp: 20000,
					Value:     30,
				},
			},
		},
		{
			name: "no_duplicated_no_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 20000,
						Value:     30,
					},
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 0,
					Value:     10,
				},
				{
					Timestamp: 10000,
					Value:     20,
				},
				{
					Timestamp: 20000,
					Value:     30,
				},
			},
		},
		{
			name: "duplicated_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 20000,
						Value:     30,
					},
					{
						Timestamp: 20000,
						Value:     30,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 0,
					Value:     10,
				},
				{
					Timestamp: 10000,
					Value:     20,
				},
				{
					Timestamp: 20000,
					Value:     30,
				},
			},
		},
		{
			name: "duplicated_no_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 20000,
						Value:     30,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 20000,
						Value:     30,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 0,
					Value:     10,
				},
				{
					Timestamp: 10000,
					Value:     20,
				},
				{
					Timestamp: 20000,
					Value:     30,
				},
			},
		},
		{
			name: "points_empty",
			args: args{
				points: []MetricPoint{},
			},
			want: []MetricPoint{},
		},
		{
			name: "points_nil",
			args: args{
				points: nil,
			},
			want: nil,
		},
		{
			name: "duplicated_nan_never_preferred",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 10000,
						Value:     math.NaN(),
					},
					{
						Timestamp: 10000,
						Value:     10,
					},
					{
						Timestamp: 20000,
						Value:     math.NaN(),
					},
					{
						Timestamp: 20000,
						Value:     20,
					},
					{
						Timestamp: 30000,
						Value:     math.Float64frombits(value.NormalNaN),
					},
					{
						Timestamp: 30000,
						Value:     30,
					},
					{
						Timestamp: 40000,
						Value:     40,
					},
					{
						Timestamp: 40000,
						Value:     math.Float64frombits(value.StaleNaN),
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 10000,
					Value:     10,
				},
				{
					Timestamp: 20000,
					Value:     20,
				},
				{
					Timestamp: 30000,
					Value:     30,
				},
				{
					Timestamp: 40000,
					Value:     40,
				},
			},
		},
		{
			name: "duplicated_nan_never_preferred_unsorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 10000,
						Value:     math.NaN(),
					},
					{
						Timestamp: 30000,
						Value:     30,
					},
					{
						Timestamp: 10000,
						Value:     10,
					},
					{
						Timestamp: 20000,
						Value:     math.NaN(),
					},
					{
						Timestamp: 40000,
						Value:     40,
					},
					{
						Timestamp: 30000,
						Value:     math.Float64frombits(value.NormalNaN),
					},
					{
						Timestamp: 30000,
						Value:     math.Float64frombits(value.NormalNaN),
					},
					{
						Timestamp: 40000,
						Value:     math.Float64frombits(value.StaleNaN),
					},
					{
						Timestamp: 20000,
						Value:     20,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 10000,
					Value:     10,
				},
				{
					Timestamp: 20000,
					Value:     20,
				},
				{
					Timestamp: 30000,
					Value:     30,
				},
				{
					Timestamp: 40000,
					Value:     40,
				},
			},
		},
		{
			name: "duplicated_preferred_latest_point",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 10000,
						Value:     1,
					},
					{
						Timestamp: 30000,
						Value:     1,
					},
					{
						Timestamp: 10000,
						Value:     2,
					},
					{
						Timestamp: 20000,
						Value:     2,
					},
					{
						Timestamp: 40000,
						Value:     2,
					},
					{
						Timestamp: 50000,
						Value:     2,
					},
					{
						Timestamp: 30000,
						Value:     3,
					},
					{
						Timestamp: 30000,
						Value:     4,
					},
					{
						Timestamp: 40000,
						Value:     4,
					},
					{
						Timestamp: 20000,
						Value:     5,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 10000,
					Value:     2,
				},
				{
					Timestamp: 20000,
					Value:     5,
				},
				{
					Timestamp: 30000,
					Value:     4,
				},
				{
					Timestamp: 40000,
					Value:     4,
				},
				{
					Timestamp: 50000,
					Value:     2,
				},
			},
		},
		{
			name: "duplicated_preferred_latest_point2",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 10000,
						Value:     1,
					},
					{
						Timestamp: 20000,
						Value:     2,
					},
					{
						Timestamp: 30000,
						Value:     3,
					},
					{
						Timestamp: 40000,
						Value:     4,
					},
					{
						Timestamp: 20000,
						Value:     5,
					},
					{
						Timestamp: 30000,
						Value:     6,
					},
					{
						Timestamp: 40000,
						Value:     7,
					},
					{
						Timestamp: 50000,
						Value:     8,
					},
					{
						Timestamp: 40000,
						Value:     9,
					},
					{
						Timestamp: 50000,
						Value:     10,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 10000,
					Value:     1,
				},
				{
					Timestamp: 20000,
					Value:     5,
				},
				{
					Timestamp: 30000,
					Value:     6,
				},
				{
					Timestamp: 40000,
					Value:     9,
				},
				{
					Timestamp: 50000,
					Value:     10,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DeduplicatePoints(tt.args.points); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DeduplicatePoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSortPoints(t *testing.T) {
	type args struct {
		points []MetricPoint
	}

	tests := []struct {
		name string
		args args
		want []MetricPoint
	}{
		{
			name: "sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 20000,
						Value:     30,
					},
					{
						Timestamp: 30000,
						Value:     40,
					},
					{
						Timestamp: 40000,
						Value:     50,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 0,
					Value:     10,
				},
				{
					Timestamp: 10000,
					Value:     20,
				},
				{
					Timestamp: 20000,
					Value:     30,
				},
				{
					Timestamp: 30000,
					Value:     40,
				},
				{
					Timestamp: 40000,
					Value:     50,
				},
			},
		},
		{
			name: "unsorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 20000,
						Value:     30,
					},
					{
						Timestamp: 40000,
						Value:     50,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 30000,
						Value:     40,
					},
					{
						Timestamp: 0,
						Value:     10,
					},
				},
			},
			want: []MetricPoint{
				{
					Timestamp: 0,
					Value:     10,
				},
				{
					Timestamp: 10000,
					Value:     20,
				},
				{
					Timestamp: 20000,
					Value:     30,
				},
				{
					Timestamp: 30000,
					Value:     40,
				},
				{
					Timestamp: 40000,
					Value:     50,
				},
			},
		},
		{
			name: "points_empty",
			args: args{
				points: []MetricPoint{},
			},
			want: []MetricPoint{},
		},
		{
			name: "points_nil",
			args: args{
				points: nil,
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		var result []MetricPoint
		if tt.args.points != nil {
			result = make([]MetricPoint, len(tt.args.points))
			copy(result, tt.args.points)
		}

		t.Run(tt.name, func(t *testing.T) {
			sortPoints(result)

			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("sortPoints() = %v, want %v", result, tt.want)
			}
		})
	}
}

func BenchmarkDeduplicatePoints(b *testing.B) {
	rnd := rand.New(rand.NewSource(42))

	tests := []struct {
		name   string
		points []MetricPoint
	}{
		{
			name:   "no_duplicated_sorted_30",
			points: MakePointsForTest(30),
		},
		{
			name:   "no_duplicated_sorted_1000",
			points: MakePointsForTest(1000),
		},
		{
			name:   "no_duplicated_sorted_10000",
			points: MakePointsForTest(10000),
		},
		{
			name:   "duplicated_sorted_1100",
			points: AddDuplicateForTest(MakePointsForTest(1000), 100, rnd),
		},
		{
			name:   "duplicated_1100",
			points: ShuffleForTest(AddDuplicateForTest(MakePointsForTest(1000), 100, rnd), rnd),
		},
		{
			name:   "two_duplicated_block_2000",
			points: append(MakePointsForTest(1000), MakePointsForTest(1000)...),
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for range b.N {
				_ = DeduplicatePoints(tt.points)
			}
		})
	}
}

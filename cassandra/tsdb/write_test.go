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

package tsdb

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/aggregate"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func Test_PointsEncode(t *testing.T) {
	type args struct {
		points []types.MetricPoint
		t0     int64
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "offset_0",
			args: args{
				t0: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: 0},
					{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000, Value: 2},
					{Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000, Value: 3},
				},
			},
		},
		{
			name: "offset_0_big",
			args: args{
				t0: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: 0},
					{Timestamp: time.Date(2019, 9, 17, 9, 47, 44, 0, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2019, 9, 17, 9, 52, 44, 0, time.UTC).UnixNano() / 1000000, Value: 2},
				},
			},
		},
		{
			name: "one-point",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 40, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 0, time.UTC).UnixNano() / 1000000, Value: 0},
				},
			},
		},
		{
			name: "two-points",
			args: args{
				t0: time.Date(2020, 3, 4, 0, 0, 10, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 0, 0, 10, 0, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 4, 0, 0, 20, 0, time.UTC).UnixNano() / 1000000, Value: 2},
				},
			},
		},
		{
			// This case test something that should NOT happen... but aggregation does it :(
			// We should normally don't use gorrilaEncode with t0 == baseTimestamp, but preaggregation did it.
			// Hopefully the issue seems only when there is only ONE point, which don't happen to much with aggregated data.
			// We need to add test for this and "support" it until a way to update aggregated data (including migrating
			// existing data) is found.
			name: "two-points-t0==base timestamp",
			args: args{
				t0: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 4, 0, 0, 10, 0, time.UTC).UnixNano() / 1000000, Value: 2},
				},
			},
		},
		{
			name: "one-point-t0-changed",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 30, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 0, time.UTC).UnixNano() / 1000000, Value: 0},
				},
			},
		},
		{
			name: "one-point-ms",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000, Value: 0},
				},
			},
		},
		{
			name: "multiple-point-ms",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000, Value: 0},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 43e6, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 50, 150e6, time.UTC).UnixNano() / 1000000, Value: 2},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 59, 999e6, time.UTC).UnixNano() / 1000000, Value: 3},
				},
			},
		},
		{
			name: "large-delta",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 0},
					{Timestamp: time.Date(2020, 3, 5, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 6, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 2},
					{Timestamp: time.Date(2020, 4, 6, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 3},
					{Timestamp: time.Date(2020, 4, 6, 8, 6, 0, 1e6, time.UTC).UnixNano() / 1000000, Value: 4},
				},
			},
		},
		{
			name: "49_days_delta",
			args: args{
				t0: time.Date(2020, 1, 1, 0, 0, 1, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					// delta with t0 and first point must fit in 14-bits
					{Timestamp: time.Date(2020, 1, 1, 0, 0, 17, 383, time.UTC).UnixNano() / 1000000, Value: 0},
					// first point +49 days
					{Timestamp: time.Date(2020, 2, 19, 0, 0, 17, 383, time.UTC).UnixNano() / 1000000, Value: 1},
				},
			},
		},
		{
			name: "50_min",
			args: args{
				t0:     1568706164000, // First timestamp of MakePointsForTest
				points: types.MakePointsForTest(10 * 300 / 10),
			},
		},
		{
			name: "500_min",
			args: args{
				t0:     1568706164000, // First timestamp of MakePointsForTest
				points: types.MakePointsForTest(100 * 300 / 10),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CassandraTSDB{
				bytesPool: sync.Pool{
					New: func() interface{} {
						return make([]byte, 15)
					},
				},
				xorChunkPool: chunkenc.NewPool(),
			}

			buffer, err := c.encodePoints(tt.args.points)
			if err != nil {
				t.Errorf("encodePoints failed: %v", err)
			}

			// Run test at least twice... tsz MUTATE data on *READ* !! :(
			for range 2 {
				buffer := buffer

				got, err := c.decodePoints(buffer, nil)
				if err != nil {
					t.Errorf("decodePoints() failed: %v", err)
				}

				if !reflect.DeepEqual(got, tt.args.points) {
					t.Errorf("decodePoints(encodePoints() = %v, want %v", got, tt.args.points)
				}
			}
		})
	}
}

func Benchmark_pointsEncode(b *testing.B) {
	type args struct {
		points []types.MetricPoint
		t0     int64
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "5_min",
			args: args{
				t0:     1568706164000, // First timestamp of MakePointsForTest
				points: types.MakePointsForTest(300 / 10),
			},
		},
		{
			name: "50_min",
			args: args{
				t0:     1568706164000, // First timestamp of MakePointsForTest
				points: types.MakePointsForTest(10 * 300 / 10),
			},
		},
		{
			name: "multiple-point-ms",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000, Value: 0},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 43e6, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 50, 150e6, time.UTC).UnixNano() / 1000000, Value: 2},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 59, 999e6, time.UTC).UnixNano() / 1000000, Value: 3},
				},
			},
		},
		{
			name: "large-delta",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 0},
					{Timestamp: time.Date(2020, 3, 5, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 6, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 2},
					{Timestamp: time.Date(2020, 4, 6, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 3},
					{Timestamp: time.Date(2020, 4, 6, 8, 6, 0, 1e6, time.UTC).UnixNano() / 1000000, Value: 4},
				},
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			c := CassandraTSDB{
				bytesPool: sync.Pool{
					New: func() interface{} {
						return make([]byte, 15)
					},
				},
				xorChunkPool: chunkenc.NewPool(),
			}

			for range b.N {
				_, err := c.encodePoints(tt.args.points)
				if err != nil {
					b.Error(err)
				}
			}
		})
	}
}

func Benchmark_pointsDecode(b *testing.B) {
	type args struct {
		points []types.MetricPoint
		t0     int64
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "5_min",
			args: args{
				t0:     1568706164000, // First timestamp of MakePointsForTest
				points: types.MakePointsForTest(300 / 10),
			},
		},
		{
			name: "50_min",
			args: args{
				t0:     1568706164000, // First timestamp of MakePointsForTest
				points: types.MakePointsForTest(10 * 300 / 10),
			},
		},
		{
			name: "multiple-point-ms",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000, Value: 0},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 43e6, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 50, 150e6, time.UTC).UnixNano() / 1000000, Value: 2},
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 59, 999e6, time.UTC).UnixNano() / 1000000, Value: 3},
				},
			},
		},
		{
			name: "large-delta",
			args: args{
				t0: time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 0},
					{Timestamp: time.Date(2020, 3, 5, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 6, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 2},
					{Timestamp: time.Date(2020, 4, 6, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000, Value: 3},
					{Timestamp: time.Date(2020, 4, 6, 8, 6, 0, 1e6, time.UTC).UnixNano() / 1000000, Value: 4},
				},
			},
		},
	}

	for _, tt := range tests {
		for _, reuse := range []bool{false, true} {
			if reuse {
				tt.name += "-reuse"
			}

			b.Run(tt.name, func(b *testing.B) {
				c := CassandraTSDB{
					bytesPool: sync.Pool{
						New: func() interface{} {
							return make([]byte, 15)
						},
					},
					xorChunkPool: chunkenc.NewPool(),
				}

				data, err := c.encodePoints(tt.args.points)
				if err != nil {
					b.Error(err)
				}

				data2 := make([]byte, len(data))

				var tmp []types.MetricPoint

				b.ResetTimer()

				for range b.N {
					if !reuse {
						tmp = nil
					}

					// TSZ force use to copy data buffer, because tsz mutate it...
					// To be fair between TSZ and new format, do the copy always
					copy(data2, data)

					tmp, err = c.decodePoints(data2, tmp)
					if err != nil {
						b.Error(err)
					}
				}
			})
		}
	}
}

func Test_EncodeAggregate(t *testing.T) {
	resolution := aggregateResolution.Milliseconds()

	metricHundredHours := types.MetricData{
		ID:         types.MetricID(100),
		TimeToLive: 42,
		Points:     types.MakePointsForTest(100 * 3600 / 10),
	}
	aggregatedMetricHundredHours := aggregate.Aggregate(metricHundredHours, resolution)

	type args struct {
		aggregatedPoints []aggregate.AggregatedPoint
		t0               int64
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "offset_0",
			args: args{
				t0: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: time.Date(2019, 9, 17, 9, 45, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: time.Date(2019, 9, 17, 9, 50, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       1337,
						Max:       64,
						Average:   42,
						Count:     1337,
					},
				},
			},
		},
		{
			name: "large_delta",
			args: args{
				t0: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: time.Date(2019, 9, 20, 9, 45, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: time.Date(2019, 9, 25, 9, 50, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       1337,
						Max:       64,
						Average:   42,
						Count:     1337,
					},
				},
			},
		},
		{
			name: "very_large_delta",
			args: args{
				t0: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: time.Date(2019, 9, 20, 9, 45, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: time.Date(2019, 9, 25, 9, 50, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       1337,
						Max:       64,
						Average:   42,
						Count:     1337,
					},
				},
			},
		},
		{
			name: "100_hours",
			args: args{
				t0:               aggregatedMetricHundredHours.Points[0].Timestamp,
				aggregatedPoints: aggregatedMetricHundredHours.Points,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CassandraTSDB{
				bytesPool: sync.Pool{
					New: func() interface{} {
						return make([]byte, 15)
					},
				},
				xorChunkPool: chunkenc.NewPool(),
			}

			testedFun := []string{
				"min", "min_over_time",
				"max", "max_over_time",
				"avg", "avg_over_time",
				"count", "count_over_time",
			}

			for _, function := range testedFun {
				buffer, err := c.encodeAggregatedPoints(tt.args.aggregatedPoints)
				if err != nil {
					t.Errorf("encodeAggregatedPoints failed: %v", err)
				}

				want := make([]types.MetricPoint, len(tt.args.aggregatedPoints))
				got, err := c.decodeAggregatedPoints(buffer, function, nil)

				for i, p := range tt.args.aggregatedPoints {
					want[i].Timestamp = p.Timestamp

					switch function {
					case "min", "min_over_time":
						want[i].Value = p.Min
					case "max", "max_over_time":
						want[i].Value = p.Max
					case "avg", "avg_over_time":
						want[i].Value = p.Average
					case "count", "count_over_time":
						want[i].Value = p.Count
					}
				}

				if err != nil {
					t.Fatalf("gorillaDecodeAggregate(gorillaEncodeAggregate(), \"%s\") failed: %v", function, err)
				}

				if !reflect.DeepEqual(got, want) {
					t.Fatalf("gorillaDecodeAggregate(gorillaEncodeAggregate(), \"%s\") = %v, want = %v", function, got, want)
				}
			}
		})
	}
}

func Benchmark_EncodeAggregate(b *testing.B) {
	const resolution = 300000

	metricOneHour := types.MetricData{
		ID:         types.MetricID(1),
		TimeToLive: 42,
		Points:     types.MakePointsForTest(3600 / 10),
	}
	metricOneDay := types.MetricData{
		ID:         types.MetricID(741),
		TimeToLive: 42,
		Points:     types.MakePointsForTest(86400 / 10),
	}
	metricHundredHours := types.MetricData{
		ID:         types.MetricID(778955),
		TimeToLive: 42,
		Points:     types.MakePointsForTest(100 * 3600 / 10),
	}
	aggregatedMetricOneHour := aggregate.Aggregate(metricOneHour, resolution)
	aggregatedMetricOneDay := aggregate.Aggregate(metricOneDay, resolution)
	aggregatedMetricHundredHours := aggregate.Aggregate(metricHundredHours, resolution)

	type args struct {
		aggregatedPoints []aggregate.AggregatedPoint
		t0               int64
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "offset_0",
			args: args{
				t0: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: time.Date(2019, 9, 17, 9, 45, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: time.Date(2019, 9, 17, 9, 50, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       1337,
						Max:       64,
						Average:   42,
						Count:     1337,
					},
				},
			},
		},
		{
			name: "one_hour",
			args: args{
				t0:               aggregatedMetricOneHour.Points[0].Timestamp,
				aggregatedPoints: aggregatedMetricOneHour.Points,
			},
		},
		{
			name: "one_day",
			args: args{
				t0:               aggregatedMetricOneDay.Points[0].Timestamp,
				aggregatedPoints: aggregatedMetricOneDay.Points,
			},
		},
		{
			name: "100_hours",
			args: args{
				t0:               aggregatedMetricHundredHours.Points[0].Timestamp,
				aggregatedPoints: aggregatedMetricHundredHours.Points,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			c := CassandraTSDB{
				bytesPool: sync.Pool{
					New: func() interface{} {
						return make([]byte, 15)
					},
				},
				xorChunkPool: chunkenc.NewPool(),
			}

			for range b.N {
				_, err := c.encodeAggregatedPoints(tt.args.aggregatedPoints)
				if err != nil {
					b.Errorf("encodeAggregatedPoints failed: %v", err)
				}
			}
		})
	}
}

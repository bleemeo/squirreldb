package tsdb

import (
	"reflect"
	"squirreldb/aggregate"
	"squirreldb/types"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// bitStringToByte convert a bitstring to []byte
// e.g. bitStringToByte("010010") == []byte{18}
func bitStringToByte(input ...string) []byte {
	var out []byte

	str := strings.Join(input, "")

	for i := 0; i < len(str); i += 8 {
		end := i + 8
		if end > len(str) {
			end = len(str)
		}
		v, err := strconv.ParseUint(str[i:end], 2, 8)
		if i+8-end > 0 {
			v = v << uint(i+8-end)
		}
		if err != nil {
			panic(err)
		}
		out = append(out, byte(v))
	}
	return out
}

func Test_gorillaEncode(t *testing.T) {
	// Test that our encoding is Gorilla TSZ storage described in
	// https://www.vldb.org/pvldb/vol8/p1816-teller.pdf

	// Our gorillaEncode is slighly modified (to store millisecond value)
	// See docstring from gorillaEncode

	// Example from the paper
	want := bitStringToByte(
		// go-tsz use 32-bits timestamp (while paper use 64-bits timestamp)
		// 64-bit timestamp of March 24 2015, 02:00:00 UTC in seconds
		"01010101000100001100010100100000",
		// value 62 using 14 bits
		"00000000111110",
		// 12 as 64-bits float
		"0100000000101000000000000000000000000000000000000000000000000000",
		// bits '10' then -2 as 7-bits integer
		"101111110",
		"0",
		"0",
		// bits '11', then 11 as 5-bit integer, then 1 as 6-bit integer, then bit '1'
		"11010110000011",
		// not specified... the end-of-stream marked :(
		// go-tsz use 36 "1" followed by "0"
		"1111111111111111111111111111111111110",
	)

	t0 := time.Date(2015, 3, 24, 2, 0, 0, 0, time.UTC).Unix()
	points := []types.MetricPoint{
		{
			// Note: normally we should use millisecond timestamp... but we
			// want to generate the exact output from the paper
			Timestamp: time.Date(2015, 3, 24, 2, 1, 2, 0, time.UTC).Unix(),
			Value:     12.0,
		},
		{
			Timestamp: time.Date(2015, 3, 24, 2, 2, 2, 0, time.UTC).Unix(),
			Value:     12.0,
		},
		{
			Timestamp: time.Date(2015, 3, 24, 2, 3, 2, 0, time.UTC).Unix(),
			Value:     24.0,
		},
	}
	got := gorillaEncode(points, uint32(t0), 0)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("gorillaEncode(...) = %v, want %v", got, want)
	}
}

func Test_PointsEncode(t *testing.T) {
	type args struct {
		points        []types.MetricPoint
		t0            int64
		baseTimestamp int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "offset_0",
			args: args{
				baseTimestamp: time.Date(2019, 9, 17, 9, 42, 43, 999e6, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2019, 9, 17, 9, 42, 43, 999e6, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 40, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 0, time.UTC).UnixNano() / 1000000, Value: 0},
				},
			},
		},
		{
			name: "two-points",
			args: args{
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 0, 0, 10, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000, Value: 1},
					{Timestamp: time.Date(2020, 3, 4, 0, 0, 10, 0, time.UTC).UnixNano() / 1000000, Value: 2},
				},
			},
		},
		{
			name: "one-point-t0-changed",
			args: args{
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 30, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 0, time.UTC).UnixNano() / 1000000, Value: 0},
				},
			},
		},
		{
			name: "one-point-ms",
			args: args{
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000, Value: 0},
				},
			},
		},
		{
			name: "multiple-point-ms",
			args: args{
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 1, 1, 0, 0, 1, 0, time.UTC).UnixNano() / 1000000,
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 1, 1, 0, 0, 17, 383, time.UTC).UnixNano() / 1000000, Value: 0},  // delta with t0 and first point must fit in 14-bits
					{Timestamp: time.Date(2020, 2, 19, 0, 0, 17, 383, time.UTC).UnixNano() / 1000000, Value: 1}, // first point +49 days
				},
			},
		},
		{
			name: "50_min",
			args: args{
				baseTimestamp: 1568678400000,
				t0:            1568706164000, // First timestamp of MakePointsForTest
				points:        types.MakePointsForTest(10 * 300 / 10),
			},
		},
		{
			name: "500_min",
			args: args{
				baseTimestamp: 1568678400000,
				t0:            1568706164000, // First timestamp of MakePointsForTest
				points:        types.MakePointsForTest(100 * 300 / 10),
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		future := time.Date(2025, 2, 19, 0, 0, 17, 383, time.UTC)

		for _, timestamp := range []int64{0, future.Unix()} {
			timestamp := timestamp
			name := tt.name + "-new"
			if timestamp != 0 {
				name = tt.name + "-tsz"
			}

			t.Run(name, func(t *testing.T) {
				c := CassandraTSDB{
					bytesPool: sync.Pool{
						New: func() interface{} {
							return make([]byte, 15)
						},
					},
					xorChunkPool:    chunkenc.NewPool(),
					newFormatCutoff: timestamp,
				}

				buffer, err := c.encodePoints(tt.args.points, tt.args.baseTimestamp, tt.args.t0-tt.args.baseTimestamp)
				if err != nil {
					t.Errorf("encodePoints failed: %v", err)
				}

				// Run test at least twice... tsz MUTATE data on *READ* !! :(
				for n := 0; n < 2; n++ {
					buffer := buffer
					if timestamp != 0 {
						//  because tsz mutate data... we must copy the initial buffer before.
						tmp := make([]byte, len(buffer))
						copy(tmp, buffer)
						buffer = tmp
					}

					got, err := c.decodePoints(buffer, tt.args.baseTimestamp, tt.args.t0-tt.args.baseTimestamp, nil)
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
}

func Benchmark_pointsEncode(b *testing.B) {
	type args struct {
		points        []types.MetricPoint
		t0            int64
		baseTimestamp int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "5_min",
			args: args{
				baseTimestamp: 1568678400000,
				t0:            1568706164000, // First timestamp of MakePointsForTest
				points:        types.MakePointsForTest(300 / 10),
			},
		},
		{
			name: "50_min",
			args: args{
				baseTimestamp: 1568678400000,
				t0:            1568706164000, // First timestamp of MakePointsForTest
				points:        types.MakePointsForTest(10 * 300 / 10),
			},
		},
		{
			name: "multiple-point-ms",
			args: args{
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000,
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
		future := time.Date(2025, 2, 19, 0, 0, 17, 383, time.UTC)

		for _, timestamp := range []int64{0, future.Unix()} {
			timestamp := timestamp
			name := tt.name + "-new"
			if timestamp != 0 {
				name = tt.name + "-tsz"
			}

			b.Run(name, func(b *testing.B) {
				c := CassandraTSDB{
					bytesPool: sync.Pool{
						New: func() interface{} {
							return make([]byte, 15)
						},
					},
					xorChunkPool:    chunkenc.NewPool(),
					newFormatCutoff: timestamp,
				}

				for n := 0; n < b.N; n++ {
					_, err := c.encodePoints(tt.args.points, tt.args.baseTimestamp, tt.args.t0-tt.args.baseTimestamp)
					if err != nil {
						b.Error(err)
					}
				}
			})
		}
	}
}

func Benchmark_pointsDecode(b *testing.B) {
	type args struct {
		points        []types.MetricPoint
		t0            int64
		baseTimestamp int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "5_min",
			args: args{
				baseTimestamp: 1568678400000,
				t0:            1568706164000, // First timestamp of MakePointsForTest
				points:        types.MakePointsForTest(300 / 10),
			},
		},
		{
			name: "50_min",
			args: args{
				baseTimestamp: 1568678400000,
				t0:            1568706164000, // First timestamp of MakePointsForTest
				points:        types.MakePointsForTest(10 * 300 / 10),
			},
		},
		{
			name: "multiple-point-ms",
			args: args{
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 40, 42e6, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2020, 3, 4, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2020, 3, 4, 8, 6, 0, 0, time.UTC).UnixNano() / 1000000,
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
		future := time.Date(2025, 2, 19, 0, 0, 17, 383, time.UTC)

		for _, timestamp := range []int64{0, future.Unix()} {
			timestamp := timestamp
			name := tt.name + "-new"
			if timestamp != 0 {
				name = tt.name + "-tsz"
			}

			for _, reuse := range []bool{false, true} {
				if reuse {
					name = name + "-reuse"
				}

				b.Run(name, func(b *testing.B) {
					c := CassandraTSDB{
						bytesPool: sync.Pool{
							New: func() interface{} {
								return make([]byte, 15)
							},
						},
						xorChunkPool:    chunkenc.NewPool(),
						newFormatCutoff: timestamp,
					}

					data, err := c.encodePoints(tt.args.points, tt.args.baseTimestamp, tt.args.t0-tt.args.baseTimestamp)
					if err != nil {
						b.Error(err)
					}

					data2 := make([]byte, len(data))

					var tmp []types.MetricPoint
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						if !reuse {
							tmp = nil
						}

						// TSZ force use to copy data buffer, because tsz mutate it...
						// To be fair between TSZ and new format, do the copy always
						copy(data2, data)

						tmp, err = c.decodePoints(data2, tt.args.baseTimestamp, tt.args.t0-tt.args.baseTimestamp, tmp)
						if err != nil {
							b.Error(err)
						}
					}
				})
			}
		}
	}
}

func Test_EncodeAggregate(t *testing.T) {
	resolution := aggregateResolution.Milliseconds()

	metricHunderdHours := types.MetricData{
		ID:         types.MetricID(100),
		TimeToLive: 42,
		Points:     types.MakePointsForTest(100 * 3600 / 10),
	}
	aggregatedMetricHunderdHours := aggregate.Aggregate(metricHunderdHours, resolution)

	type args struct {
		aggregatedPoints []aggregate.AggregatedPoint
		baseTimestamp    int64
		t0               int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "offset_0",
			args: args{
				baseTimestamp: time.Date(2019, 9, 17, 9, 35, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2019, 9, 16, 23, 50, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(1980, 9, 16, 0, 0, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp:    1568678400000,
				t0:               aggregatedMetricHunderdHours.Points[0].Timestamp,
				aggregatedPoints: aggregatedMetricHunderdHours.Points,
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		future := time.Date(2025, 2, 19, 0, 0, 17, 383, time.UTC)

		for _, timestamp := range []int64{0, future.Unix()} {
			timestamp := timestamp
			name := tt.name + "-new"
			if timestamp != 0 {
				name = tt.name + "-tsz"
			}

			t.Run(name, func(t *testing.T) {
				c := CassandraTSDB{
					bytesPool: sync.Pool{
						New: func() interface{} {
							return make([]byte, 15)
						},
					},
					xorChunkPool:    chunkenc.NewPool(),
					newFormatCutoff: timestamp,
				}

				testedFun := []string{"min", "max", "avg", "count"}
				buffer, err := c.encodeAggregatedPoints(tt.args.aggregatedPoints, tt.args.baseTimestamp, tt.args.t0-tt.args.baseTimestamp)
				if err != nil {
					t.Errorf("encodeAggregatedPoints failed: %v", err)
				}

				for _, function := range testedFun {
					want := make([]types.MetricPoint, len(tt.args.aggregatedPoints))
					got, err := c.decodeAggregatedPoints(buffer, tt.args.baseTimestamp, tt.args.t0-tt.args.baseTimestamp, function, nil)
					for i, p := range tt.args.aggregatedPoints {
						want[i].Timestamp = p.Timestamp
						switch function {
						case "min":
							want[i].Value = p.Min
						case "max":
							want[i].Value = p.Max
						case "avg":
							want[i].Value = p.Average
						case "count":
							want[i].Value = p.Count
						}
					}
					if err != nil {
						t.Errorf("gorillaDecodeAggregate(gorillaEncodeAggregate(), \"%s\") failed: %v", function, err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("gorillaDecodeAggregate(gorillaEncodeAggregate(), \"%s\") = %v, want = %v", function, got, want)
					}
				}
			})
		}
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
		baseTimestamp    int64
		t0               int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "offset_0",
			args: args{
				baseTimestamp: time.Date(2019, 9, 17, 9, 35, 0, 0, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp:    1568678400000,
				t0:               aggregatedMetricOneHour.Points[0].Timestamp,
				aggregatedPoints: aggregatedMetricOneHour.Points,
			},
		},
		{
			name: "one_day",
			args: args{
				baseTimestamp:    1568678400000,
				t0:               aggregatedMetricOneDay.Points[0].Timestamp,
				aggregatedPoints: aggregatedMetricOneDay.Points,
			},
		},
		{
			name: "100_hours",
			args: args{
				baseTimestamp:    1568678400000,
				t0:               aggregatedMetricHundredHours.Points[0].Timestamp,
				aggregatedPoints: aggregatedMetricHundredHours.Points,
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		future := time.Date(2025, 2, 19, 0, 0, 17, 383, time.UTC)

		for _, timestamp := range []int64{0, future.Unix()} {
			timestamp := timestamp
			name := tt.name + "-new"
			if timestamp != 0 {
				name = tt.name + "-tsz"
			}

			b.Run(name, func(b *testing.B) {
				c := CassandraTSDB{
					bytesPool: sync.Pool{
						New: func() interface{} {
							return make([]byte, 15)
						},
					},
					xorChunkPool:    chunkenc.NewPool(),
					newFormatCutoff: timestamp,
				}

				for n := 0; n < b.N; n++ {
					_, err := c.encodeAggregatedPoints(tt.args.aggregatedPoints, tt.args.baseTimestamp, tt.args.t0-tt.args.baseTimestamp)
					if err != nil {
						b.Errorf("encodeAggregatedPoints failed: %v", err)
					}
				}
			})
		}
	}
}

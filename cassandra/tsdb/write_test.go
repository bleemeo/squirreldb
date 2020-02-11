package tsdb

import (
	"reflect"
	"squirreldb/aggregate"
	"squirreldb/types"
	"strconv"
	"strings"
	"testing"
	"time"
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
	got := gorillaEncode(points, t0, 0)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("gorillaEncode(...) = %v, want %v", got, want)
	}
}

func Test_gorillaEncode2(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			buffer := gorillaEncode(tt.args.points, tt.args.t0, tt.args.baseTimestamp)
			got, err := gorillaDecode(buffer, tt.args.baseTimestamp)
			if err != nil {
				t.Errorf("gorillaDecode() failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.args.points) {
				t.Errorf("gorillaDecode(gorillaEncode() = %v, want %v", got, tt.args.points)
			}
		})
	}
}

func Benchmark_gorillaEncode(b *testing.B) {
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
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = gorillaEncode(tt.args.points, tt.args.t0, tt.args.baseTimestamp)
			}
		})
	}
}

func Test_gorillaEncodeAggregate(t *testing.T) {

	idHunderdHours := types.MetricID(100)
	metrics := map[types.MetricID]types.MetricData{
		idHunderdHours: types.MetricData{
			TimeToLive: 42,
			Points:     types.MakePointsForTest(100 * 3600 / 10),
		},
	}
	aggregatedMetrics := aggregate.Aggregate(metrics, 300000)

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
				baseTimestamp: time.Date(2019, 9, 17, 9, 42, 43, 999e6, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: time.Date(2019, 9, 17, 9, 47, 44, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: time.Date(2019, 9, 17, 9, 52, 44, 0, time.UTC).UnixNano() / 1000000,
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
				baseTimestamp: time.Date(2019, 9, 16, 23, 59, 59, 999e6, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: time.Date(2019, 9, 20, 9, 47, 44, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: time.Date(2019, 9, 25, 9, 52, 44, 0, time.UTC).UnixNano() / 1000000,
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
				t0:               aggregatedMetrics[idHunderdHours].Points[0].Timestamp,
				aggregatedPoints: aggregatedMetrics[idHunderdHours].Points,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testedFun := []string{"min", "max", "avg", "count"}
			buffer := gorillaEncodeAggregate(tt.args.aggregatedPoints, tt.args.t0, tt.args.baseTimestamp)

			for _, function := range testedFun {
				want := make([]types.MetricPoint, len(tt.args.aggregatedPoints))
				got, err := gorillaDecodeAggregate(buffer, tt.args.baseTimestamp, function)
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

func Benchmark_gorillaEncodeAggregate(b *testing.B) {

	idOneHour := types.MetricID(1)
	idOneDay := types.MetricID(741)
	idHunderdHours := types.MetricID(778955)

	metrics := map[types.MetricID]types.MetricData{
		idOneHour: types.MetricData{
			TimeToLive: 42,
			Points:     types.MakePointsForTest(3600 / 10),
		},
		idOneDay: types.MetricData{
			TimeToLive: 42,
			Points:     types.MakePointsForTest(86400 / 10),
		},
		idHunderdHours: types.MetricData{
			TimeToLive: 42,
			Points:     types.MakePointsForTest(100 * 3600 / 10),
		},
	}
	aggregatedMetrics := aggregate.Aggregate(metrics, 300000)

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
				baseTimestamp: time.Date(2019, 9, 17, 9, 42, 43, 999e6, time.UTC).UnixNano() / 1000000,
				t0:            time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: time.Date(2019, 9, 17, 9, 47, 44, 0, time.UTC).UnixNano() / 1000000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: time.Date(2019, 9, 17, 9, 52, 44, 0, time.UTC).UnixNano() / 1000000,
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
				t0:               aggregatedMetrics[idOneHour].Points[0].Timestamp,
				aggregatedPoints: aggregatedMetrics[idOneHour].Points,
			},
		},
		{
			name: "one_day",
			args: args{
				baseTimestamp:    1568678400000,
				t0:               aggregatedMetrics[idOneDay].Points[0].Timestamp,
				aggregatedPoints: aggregatedMetrics[idOneDay].Points,
			},
		},
		{
			name: "100_hours",
			args: args{
				baseTimestamp:    1568678400000,
				t0:               aggregatedMetrics[idHunderdHours].Points[0].Timestamp,
				aggregatedPoints: aggregatedMetrics[idHunderdHours].Points,
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = gorillaEncodeAggregate(tt.args.aggregatedPoints, tt.args.t0, tt.args.baseTimestamp)
			}
		})
	}
}

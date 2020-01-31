package tsdb

import (
	"reflect"
	"squirreldb/aggregate"
	"squirreldb/types"
	"testing"

	gouuid "github.com/gofrs/uuid"
)

func Test_rawValuesFromPoints(t *testing.T) {

	type args struct {
		points          []types.MetricPoint
		baseTimestamp   int64
		offsetTimestamp int64
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "offset_0",
			args: args{
				baseTimestamp:   1568706164000,
				offsetTimestamp: 0,
				points: []types.MetricPoint{
					{Timestamp: 1568706164000, Value: 42},
					{Timestamp: 1568706164000 + 5000, Value: 1337},
					{Timestamp: 1568706164000 + 15000, Value: 64},
					{Timestamp: 1568706164000 + 21000, Value: 42},
				},
			},
			want: []byte{
				0, 0,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				0, 5,
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
				0, 15,
				64, 80, 0, 0, 0, 0, 0, 0, // encoding of value 64.0
				0, 21,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
			},
		},
		{
			name: "offset_27764",
			args: args{
				baseTimestamp:   1568678400000,
				offsetTimestamp: 27764000, // 1568706164 = 1568678400 + 27764, in millisecond
				points: []types.MetricPoint{
					{Timestamp: 1568706164000, Value: 42},
					{Timestamp: 1568706164000 + 5000, Value: 1337},
					{Timestamp: 1568706164000 + 15000, Value: 64},
					{Timestamp: 1568706164000 + 21000, Value: 42},
				},
			},
			want: []byte{
				0, 0,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				0, 5,
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
				0, 15,
				64, 80, 0, 0, 0, 0, 0, 0, // encoding of value 64.0
				0, 21,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := rawValuesFromPoints(tt.args.points, tt.args.baseTimestamp, tt.args.offsetTimestamp)
			if (err != nil) != tt.wantErr {
				t.Errorf("rawValuesFromPoints() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("rawValuesFromPoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_rawValuesFromPoints(b *testing.B) {

	type args struct {
		points          []types.MetricPoint
		baseTimestamp   int64
		offsetTimestamp int64
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "offset_0",
			args: args{
				baseTimestamp:   1568706164000,
				offsetTimestamp: 0,
				points: []types.MetricPoint{
					{Timestamp: 1568706164000, Value: 42},
					{Timestamp: 1568706164000 + 5000, Value: 1337},
					{Timestamp: 1568706164000 + 15000, Value: 64},
					{Timestamp: 1568706164000 + 21000, Value: 42},
				},
			},
			want: []byte{
				0, 0,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				0, 5,
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
				0, 15,
				64, 80, 0, 0, 0, 0, 0, 0, // encoding of value 64.0
				0, 21,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
			},
		},
		{
			name: "offset_27764",
			args: args{
				baseTimestamp:   1568678400000,
				offsetTimestamp: 27764000, // 1568706164 = 1568678400 + 27764, in millisecond
				points: []types.MetricPoint{
					{Timestamp: 1568706164000, Value: 42},
					{Timestamp: 1568706164000 + 5000, Value: 1337},
					{Timestamp: 1568706164000 + 15000, Value: 64},
					{Timestamp: 1568706164000 + 21000, Value: 42},
				},
			},
			want: []byte{
				0, 0,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				0, 5,
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
				0, 15,
				64, 80, 0, 0, 0, 0, 0, 0, // encoding of value 64.0
				0, 21,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
			},
		},
		{
			name: "5_min",
			args: args{
				baseTimestamp:   1568678400000,
				offsetTimestamp: 27764000, // 1568706164 = 1568678400 + 27764, in millisecond
				points:          types.MakePointsForTest(300 / 10),
			},
		},
		{
			name: "50_min",
			args: args{
				baseTimestamp:   1568678400000,
				offsetTimestamp: 27764000, // 1568706164 = 1568678400 + 27764
				points:          types.MakePointsForTest(10 * 300 / 10),
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, _ = rawValuesFromPoints(tt.args.points, tt.args.baseTimestamp, tt.args.offsetTimestamp)
			}
		})
	}
}

func Test_aggregateValuesFromAggregatedPoints(t *testing.T) {

	type args struct {
		aggregatedPoints []aggregate.AggregatedPoint
		baseTimestamp    int64
		offsetTimestamp  int64
		resolution       int64
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "offset_0",
			args: args{
				baseTimestamp:   1568706164000,
				offsetTimestamp: 0,
				resolution:      300,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: 1568706164000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: 1568706164000 + 300000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: 1568706164000 + 900000,
						Min:       1337,
						Max:       64,
						Average:   42,
						Count:     1337,
					},
				},
			},
			want: []byte{
				0, 0,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				0, 1,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 80, 0, 0, 0, 0, 0, 0, // encoding of value 64.0
				0, 3,
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
				64, 80, 0, 0, 0, 0, 0, 0, // encoding of value 64.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
			},
		},
		{
			name: "offset_27764",
			args: args{
				baseTimestamp:   1568678400000,
				offsetTimestamp: 27764000, // 1568706164 = 1568678400 + 27764, in milliseconds
				resolution:      300,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: 1568706164000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: 1568706164000 + 300000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: 1568706164000 + 900000,
						Min:       1337,
						Max:       64,
						Average:   42,
						Count:     1337,
					},
				},
			},
			want: []byte{
				0, 0,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				0, 1,
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 80, 0, 0, 0, 0, 0, 0, // encoding of value 64.0
				0, 3,
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
				64, 80, 0, 0, 0, 0, 0, 0, // encoding of value 64.0
				64, 69, 0, 0, 0, 0, 0, 0, // encoding of value 42.0
				64, 148, 228, 0, 0, 0, 0, 0, // encoding of value 1337.0
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := aggregateValuesFromAggregatedPoints(tt.args.aggregatedPoints, tt.args.baseTimestamp, tt.args.offsetTimestamp, tt.args.resolution)
			if (err != nil) != tt.wantErr {
				t.Errorf("aggregateValuesFromAggregatedPoints() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("aggregateValuesFromAggregatedPoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_aggregateValuesFromAggregatedPoints(b *testing.B) {

	uuidOneHour := gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001")
	uuidOneDay := gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000024")
	uuidHunderdHours := gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000100")

	metrics := map[gouuid.UUID]types.MetricData{
		uuidOneHour: types.MetricData{
			TimeToLive: 42,
			Points:     types.MakePointsForTest(3600 / 10),
		},
		uuidOneDay: types.MetricData{
			TimeToLive: 42,
			Points:     types.MakePointsForTest(86400 / 10),
		},
		uuidHunderdHours: types.MetricData{
			TimeToLive: 42,
			Points:     types.MakePointsForTest(100 * 3600 / 10),
		},
	}
	aggregatedMetrics := aggregate.Aggregate(metrics, 300000)

	type args struct {
		aggregatedPoints []aggregate.AggregatedPoint
		baseTimestamp    int64
		offsetTimestamp  int64
		resolution       int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "offset_0",
			args: args{
				baseTimestamp:   1568706164000,
				offsetTimestamp: 0,
				resolution:      300,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: 1568706164000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: 1568706164000 + 300000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: 1568706164000 + 900000,
						Min:       1337,
						Max:       64,
						Average:   42,
						Count:     1337,
					},
				},
			},
		},
		{
			name: "offset_27764",
			args: args{
				baseTimestamp:   1568678400000,
				offsetTimestamp: 27764000, // 1568706164 = 1568678400 + 27764, in milliseconds
				resolution:      300,
				aggregatedPoints: []aggregate.AggregatedPoint{
					{
						Timestamp: 1568706164000,
						Min:       42,
						Max:       42,
						Average:   42,
						Count:     42,
					},
					{
						Timestamp: 1568706164000 + 300000,
						Min:       42,
						Max:       1337,
						Average:   42,
						Count:     64,
					},
					{
						Timestamp: 1568706164000 + 900000,
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
				offsetTimestamp:  27764000, // 1568706164 = 1568678400 + 27764, in milliseconds
				resolution:       300,
				aggregatedPoints: aggregatedMetrics[uuidOneHour].Points,
			},
		},
		{
			name: "one_day",
			args: args{
				baseTimestamp:    1568678400000,
				offsetTimestamp:  27764000, // 1568706164 = 1568678400 + 27764, in milliseconds
				resolution:       300,
				aggregatedPoints: aggregatedMetrics[uuidOneDay].Points,
			},
		},
		{
			name: "100_hours",
			args: args{
				baseTimestamp:    1568678400000,
				offsetTimestamp:  27764000, // 1568706164 = 1568678400 + 27764, in milliseconds
				resolution:       300,
				aggregatedPoints: aggregatedMetrics[uuidHunderdHours].Points,
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, _ = aggregateValuesFromAggregatedPoints(tt.args.aggregatedPoints, tt.args.baseTimestamp, tt.args.offsetTimestamp, tt.args.resolution)
			}
		})
	}
}

package aggregate

import (
	"reflect"
	"squirreldb/types"
	"testing"
	"time"
)

const (
	MetricIDTest1 = 1
	MetricIDTest2 = 2
)

func Test_aggregateData(t *testing.T) {
	type args struct {
		data       types.MetricData
		resolution int64
	}

	tests := []struct {
		name string
		args args
		want AggregatedData
	}{
		{
			name: "test",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest1,
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     100,
						},
						{
							Timestamp: 100000,
							Value:     200,
						},
						{
							Timestamp: 200000,
							Value:     300,
						},
						{
							Timestamp: 300000,
							Value:     400,
						},
						{
							Timestamp: 400000,
							Value:     500,
						},
					},
					TimeToLive: 3600,
				},
				resolution: 200000,
			},
			want: AggregatedData{
				ID: MetricIDTest1,
				Points: []AggregatedPoint{
					{
						Timestamp: 0,
						Min:       100,
						Max:       200,
						Average:   150,
						Count:     2,
					},
					{
						Timestamp: 200000,
						Min:       300,
						Max:       400,
						Average:   350,
						Count:     2,
					},
					{
						Timestamp: 400000,
						Min:       500,
						Max:       500,
						Average:   500,
						Count:     1,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-real-timestamp",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: 500},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: 1000},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000, Value: 1500},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000, Value: 2000},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 34, 0, time.UTC).UnixNano() / 1000000, Value: 2500},
					},
					TimeToLive: 3600,
				},
				resolution: 300000,
			},
			want: AggregatedData{
				ID: MetricIDTest2,
				Points: []AggregatedPoint{
					{
						Timestamp: time.Date(2019, 9, 17, 9, 40, 0, 0, time.UTC).UnixNano() / 1000000,
						Min:       500,
						Max:       2500,
						Average:   1500,
						Count:     5,
					},
				},
				TimeToLive: 3600,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Aggregate(tt.args.data, tt.args.resolution); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("aggregateData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_aggregatePoints(t *testing.T) {
	type args struct {
		points    []types.MetricPoint
		timestamp int64
	}

	tests := []struct {
		name string
		args args
		want AggregatedPoint
	}{
		{
			name: "test",
			args: args{
				points: []types.MetricPoint{
					{
						Timestamp: 0,
						Value:     500,
					},
					{
						Timestamp: 200000,
						Value:     1000,
					},
					{
						Timestamp: 400000,
						Value:     1500,
					},
					{
						Timestamp: 600000,
						Value:     2000,
					},
					{
						Timestamp: 800000,
						Value:     2500,
					},
				},
				timestamp: 0,
			},
			want: AggregatedPoint{
				Timestamp: 0,
				Min:       500,
				Max:       2500,
				Average:   1500,
				Count:     5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := aggregatePoints(tt.args.points, tt.args.timestamp); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("aggregatePoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_aggregateData(b *testing.B) {
	tests := []struct {
		Name       string
		Size       int
		PointStep  int
		Resolution int64
	}{
		{
			Name:       "small-300",
			Size:       30,
			PointStep:  10,
			Resolution: 300,
		},
		{
			Name:       "small-900",
			Size:       90,
			PointStep:  10,
			Resolution: 900,
		},
		{
			Name:       "medium-900",
			Size:       180,
			PointStep:  10,
			Resolution: 900,
		},
		{
			Name:       "large-900",
			Size:       1000,
			PointStep:  10,
			Resolution: 900,
		},
	}

	for _, tt := range tests {
		b.Run(tt.Name, func(b *testing.B) {
			startTS := time.Now().Unix()
			data := types.MetricData{
				Points: make([]types.MetricPoint, tt.Size),
			}
			for i := range data.Points {
				data.Points[i].Timestamp = (startTS + int64(tt.PointStep*i)) * 1000
				data.Points[i].Value = float64(i)
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_ = Aggregate(data, tt.Resolution)
			}
		})
	}
}

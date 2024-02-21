package aggregate

import (
	"math"
	"reflect"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/prometheus/model/value"
)

const (
	MetricIDTest1 = 1
	MetricIDTest2 = 2
)

func Test_aggregateData(t *testing.T) { //nolint:maintidx
	type args struct {
		data       types.MetricData
		resolution int64
	}

	anotherNaN := math.Float64frombits(value.StaleNaN) + 1.0

	if !math.IsNaN(anotherNaN) {
		t.Fatalf("anotherNaN is not NaN")
	}

	if math.Float64bits(anotherNaN) == value.NormalNaN {
		t.Fatalf("anotherNaN is exactly NormalNaN. It should be different")
	}

	if math.Float64bits(anotherNaN) == value.StaleNaN {
		t.Fatalf("anotherNaN is exactly StaleNaN. It should be different")
	}

	tests := []struct {
		name string
		want AggregatedData
		args args
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
		{
			name: "test-min-max",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: -500},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: -1000},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000, Value: -1500},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000, Value: -2000},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 34, 0, time.UTC).UnixNano() / 1000000, Value: -2500},
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
						Min:       -2500,
						Max:       -500,
						Average:   -1500,
						Count:     5,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-min-max-2",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: -2500},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: -2000},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000, Value: -1500},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000, Value: -1000},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 34, 0, time.UTC).UnixNano() / 1000000, Value: -500},
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
						Min:       -2500,
						Max:       -500,
						Average:   -1500,
						Count:     5,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-+inf",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: 500},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: math.Inf(1)},
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
						Max:       math.Inf(1),
						Average:   math.Inf(1),
						Count:     5,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test--inf",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: 500},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: math.Inf(-1)},
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
						Min:       math.Inf(-1),
						Max:       2500,
						Average:   math.Inf(-1),
						Count:     5,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-both-inf",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: 500},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: math.Inf(-1)},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000, Value: 1500},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000, Value: math.Inf(1)},
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
						Min:       math.Inf(-1),
						Max:       math.Inf(1),
						Average:   math.NaN(),
						Count:     5,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-stale-nan",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: 500},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: 1000},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
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
						Count:     4,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-stale-nan-2",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{
							Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: 1000},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
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
						Min:       1000,
						Max:       2500,
						Average:   1750,
						Count:     2,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-only-stale-nan",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{
							Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 34, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
					},
					TimeToLive: 3600,
				},
				resolution: 300000,
			},
			want: AggregatedData{
				ID:         MetricIDTest2,
				Points:     []AggregatedPoint{},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-only-nan",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{
							Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000,
							Value:     anotherNaN, // This should normally be a NormalNaN, but it allow to ensire NaN are normalized
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 34, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.StaleNaN),
						},
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
						Min:       math.Float64frombits(value.NormalNaN),
						Max:       math.Float64frombits(value.NormalNaN),
						Average:   math.Float64frombits(value.NormalNaN),
						Count:     1,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-nan",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000, Value: 500},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: 1000},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.NormalNaN),
						},
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
						Average:   math.Float64frombits(value.NormalNaN),
						Count:     5,
					},
				},
				TimeToLive: 3600,
			},
		},
		{
			name: "test-nan-first-and-last",
			args: args{
				data: types.MetricData{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{
							Timestamp: time.Date(2019, 9, 17, 9, 42, 44, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.NormalNaN),
						},
						{Timestamp: time.Date(2019, 9, 17, 9, 42, 54, 0, time.UTC).UnixNano() / 1000000, Value: 1000},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 4, 0, time.UTC).UnixNano() / 1000000, Value: 1500},
						{Timestamp: time.Date(2019, 9, 17, 9, 43, 14, 0, time.UTC).UnixNano() / 1000000, Value: 2000},
						{
							Timestamp: time.Date(2019, 9, 17, 9, 43, 34, 0, time.UTC).UnixNano() / 1000000,
							Value:     math.Float64frombits(value.NormalNaN),
						},
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
						Min:       1000,
						Max:       2000,
						Average:   math.Float64frombits(value.NormalNaN),
						Count:     5,
					},
				},
				TimeToLive: 3600,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Aggregate(tt.args.data, tt.args.resolution)
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateNaNs(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("aggregateData() mismatch (-want +got):\n%s", diff)
			}

			// Check that NaN are using NormalNaN and not another kind of NaN
			for i, pts := range got.Points {
				if math.IsNaN(pts.Average) && math.Float64bits(pts.Average) != value.NormalNaN {
					t.Errorf("aggregateData().Points[%d].Average = 0x%x want 0x%x", i, math.Float64bits(pts.Average), value.NormalNaN)
				}

				if math.IsNaN(pts.Max) && math.Float64bits(pts.Max) != value.NormalNaN {
					t.Errorf("aggregateData().Points[%d].Max = 0x%x want 0x%x", i, math.Float64bits(pts.Max), value.NormalNaN)
				}

				if math.IsNaN(pts.Min) && math.Float64bits(pts.Min) != value.NormalNaN {
					t.Errorf("aggregateData().Points[%d].Min = 0x%x want 0x%x", i, math.Float64bits(pts.Min), value.NormalNaN)
				}
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
			if got, _ := aggregatePoints(tt.args.points, tt.args.timestamp); !reflect.DeepEqual(got, tt.want) {
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

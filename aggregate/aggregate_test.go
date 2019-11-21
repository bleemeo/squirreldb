package aggregate

import (
	"reflect"
	"squirreldb/types"
	"testing"
)

func uuidFromStringOrNil(s string) types.MetricUUID {
	uuid, _ := types.UUIDFromString(s)

	return uuid
}

func TestAggregate(t *testing.T) {
	type args struct {
		metrics    map[types.MetricUUID]types.MetricData
		resolution int64
	}
	tests := []struct {
		name string
		args args
		want map[types.MetricUUID]AggregatedData
	}{
		{
			name: "test",
			args: args{
				metrics: map[types.MetricUUID]types.MetricData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
				},
				resolution: 50,
			},
			want: map[types.MetricUUID]AggregatedData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []AggregatedPoint{
						{
							Timestamp: 0,
							Min:       10,
							Max:       50,
							Average:   30,
							Count:     5,
						},
					},
					TimeToLive: 300,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []AggregatedPoint{
						{
							Timestamp: 0,
							Min:       50,
							Max:       150,
							Average:   100,
							Count:     3,
						},
						{
							Timestamp: 50,
							Min:       200,
							Max:       250,
							Average:   225,
							Count:     2,
						},
					},
					TimeToLive: 1200,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Aggregate(tt.args.metrics, tt.args.resolution); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Aggregate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPointsSort(t *testing.T) {
	type args struct {
		aggregatedPoints []AggregatedPoint
	}
	tests := []struct {
		name string
		args args
		want []AggregatedPoint
	}{
		{
			name: "sorted",
			args: args{
				aggregatedPoints: []AggregatedPoint{
					{
						Timestamp: 0,
						Min:       10,
						Max:       50,
						Average:   30,
						Count:     5,
					},
					{
						Timestamp: 50,
						Min:       60,
						Max:       100,
						Average:   80,
						Count:     5,
					},
					{
						Timestamp: 100,
						Min:       110,
						Max:       150,
						Average:   130,
						Count:     5,
					},
				},
			},
			want: []AggregatedPoint{
				{
					Timestamp: 0,
					Min:       10,
					Max:       50,
					Average:   30,
					Count:     5,
				},
				{
					Timestamp: 50,
					Min:       60,
					Max:       100,
					Average:   80,
					Count:     5,
				},
				{
					Timestamp: 100,
					Min:       110,
					Max:       150,
					Average:   130,
					Count:     5,
				},
			},
		},
		{
			name: "unsorted",
			args: args{
				aggregatedPoints: []AggregatedPoint{
					{
						Timestamp: 50,
						Min:       60,
						Max:       100,
						Average:   80,
						Count:     5,
					},
					{
						Timestamp: 100,
						Min:       110,
						Max:       150,
						Average:   130,
						Count:     5,
					},
					{
						Timestamp: 0,
						Min:       10,
						Max:       50,
						Average:   30,
						Count:     5,
					},
				},
			},
			want: []AggregatedPoint{
				{
					Timestamp: 0,
					Min:       10,
					Max:       50,
					Average:   30,
					Count:     5,
				},
				{
					Timestamp: 50,
					Min:       60,
					Max:       100,
					Average:   80,
					Count:     5,
				},
				{
					Timestamp: 100,
					Min:       110,
					Max:       150,
					Average:   130,
					Count:     5,
				},
			},
		},
		{
			name: "aggregatedPoints_empty",
			args: args{
				aggregatedPoints: []AggregatedPoint{},
			},
			want: nil,
		},
		{
			name: "aggregatedPoints_nil",
			args: args{
				aggregatedPoints: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PointsSort(tt.args.aggregatedPoints); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PointsSort() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     100,
						},
						{
							Timestamp: 100,
							Value:     200,
						},
						{
							Timestamp: 200,
							Value:     300,
						},
						{
							Timestamp: 300,
							Value:     400,
						},
						{
							Timestamp: 400,
							Value:     500,
						},
					},
					TimeToLive: 3600,
				},
				resolution: 200,
			},
			want: AggregatedData{
				Points: []AggregatedPoint{
					{
						Timestamp: 0,
						Min:       100,
						Max:       200,
						Average:   150,
						Count:     2,
					},
					{
						Timestamp: 200,
						Min:       300,
						Max:       400,
						Average:   350,
						Count:     2,
					},
					{
						Timestamp: 400,
						Min:       500,
						Max:       500,
						Average:   500,
						Count:     1,
					},
				},
				TimeToLive: 3600,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := aggregateData(tt.args.data, tt.args.resolution); !reflect.DeepEqual(got, tt.want) {
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
						Timestamp: 200,
						Value:     1000,
					},
					{
						Timestamp: 400,
						Value:     1500,
					},
					{
						Timestamp: 600,
						Value:     2000,
					},
					{
						Timestamp: 800,
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

package aggregate

import (
	"reflect"
	"squirreldb/types"
	"testing"
)

func uuidify(value string) types.MetricUUID {
	uuid := types.MetricLabels{
		{
			Name:  "__bleemeo_uuid__",
			Value: value,
		},
	}.UUID()

	return uuid
}

func TestMetrics(t *testing.T) {
	type args struct {
		metrics       types.Metrics
		fromTimestamp int64
		toTimestamp   int64
		resolution    int64
	}
	tests := []struct {
		name string
		args args
		want AggregatedMetrics
	}{
		{
			name: "metrics",
			args: args{
				metrics: map[types.MetricUUID]types.MetricPoints{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						{
							Timestamp: 0,
							Value:     0,
						},
						{
							Timestamp: 20,
							Value:     20,
						},
						{
							Timestamp: 40,
							Value:     40,
						},
						{
							Timestamp: 60,
							Value:     60,
						},
						{
							Timestamp: 80,
							Value:     80,
						},
						{
							Timestamp: 100,
							Value:     100,
						},
					},
				},
				fromTimestamp: 20,
				toTimestamp:   80,
				resolution:    40,
			},
			want: map[types.MetricUUID]AggregatedPoints{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					{
						Timestamp: 20,
						Min:       20,
						Max:       40,
						Average:   30,
						Count:     2,
					},
					{
						Timestamp: 60,
						Min:       60,
						Max:       80,
						Average:   70,
						Count:     2,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Metrics(tt.args.metrics, tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.resolution); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Metrics() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetricPoints(t *testing.T) {
	type args struct {
		points        types.MetricPoints
		fromTimestamp int64
		toTimestamp   int64
		resolution    int64
	}
	tests := []struct {
		name string
		args args
		want AggregatedPoints
	}{
		{
			name: "metric_points",
			args: args{
				points: []types.MetricPoint{
					{
						Timestamp: 0,
						Value:     0,
					},
					{
						Timestamp: 20,
						Value:     20,
					},
					{
						Timestamp: 40,
						Value:     40,
					},
					{
						Timestamp: 60,
						Value:     60,
					},
					{
						Timestamp: 80,
						Value:     80,
					},
					{
						Timestamp: 100,
						Value:     100,
					},
				},
				fromTimestamp: 20,
				toTimestamp:   80,
				resolution:    40,
			},
			want: []AggregatedPoint{
				{
					Timestamp: 20,
					Min:       20,
					Max:       40,
					Average:   30,
					Count:     2,
				},
				{
					Timestamp: 60,
					Min:       60,
					Max:       80,
					Average:   70,
					Count:     2,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MetricPoints(tt.args.points, tt.args.fromTimestamp, tt.args.toTimestamp, tt.args.resolution); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MetricPoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_aggregate(t *testing.T) {
	type args struct {
		timestamp int64
		points    types.MetricPoints
	}
	tests := []struct {
		name string
		args args
		want AggregatedPoint
	}{
		{
			name: "aggregate",
			args: args{
				timestamp: 0,
				points: []types.MetricPoint{
					{
						Timestamp: 0,
						Value:     0,
					},
					{
						Timestamp: 20,
						Value:     20,
					},
					{
						Timestamp: 40,
						Value:     40,
					},
					{
						Timestamp: 60,
						Value:     60,
					},
					{
						Timestamp: 80,
						Value:     80,
					},
				},
			},
			want: AggregatedPoint{
				Timestamp: 0,
				Min:       0,
				Max:       80,
				Average:   40,
				Count:     5,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := aggregate(tt.args.timestamp, tt.args.points); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("aggregate() = %v, want %v", got, tt.want)
			}
		})
	}
}

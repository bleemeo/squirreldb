package remotestorage

import (
	"reflect"
	"squirreldb/types"
	"testing"

	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"
)

func Test_requestFromPromQuery(t *testing.T) {
	type args struct {
		promQuery *prompb.Query
		index     types.Index
	}
	tests := []struct {
		name string
		args args
		want types.MetricRequest
	}{
		{
			name: "promQuery_hints",
			args: args{
				promQuery: &prompb.Query{
					StartTimestampMs: 0,
					EndTimestampMs:   50000,
					Matchers:         nil,
					Hints: &prompb.ReadHints{
						StepMs:  10000,
						Func:    "avg",
						StartMs: 0,
						EndMs:   50000,
					},
				},
				index: mockIndex{fixedSearchUUID: "00000000-0000-0000-0000-000000000001"},
			},
			want: types.MetricRequest{
				UUIDs: []gouuid.UUID{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				},
				FromTimestamp: 0,
				ToTimestamp:   50,
				Step:          10,
				Function:      "avg",
			},
		},
		{
			name: "promQuery_no_hints",
			args: args{
				promQuery: &prompb.Query{
					StartTimestampMs: 0,
					EndTimestampMs:   50000,
					Matchers:         nil,
					Hints:            nil,
				},
				index: mockIndex{fixedSearchUUID: "00000000-0000-0000-0000-000000000001"},
			},
			want: types.MetricRequest{
				UUIDs: []gouuid.UUID{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				},
				FromTimestamp: 0,
				ToTimestamp:   50,
				Step:          0,
				Function:      "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := requestFromPromQuery(tt.args.promQuery, tt.args.index)
			if err != nil {
				t.Errorf("requestFromPromQuery() failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("requestFromPromQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_requestsFromPromReadRequest(t *testing.T) {
	type args struct {
		promReadRequest *prompb.ReadRequest
		index           types.Index
	}
	tests := []struct {
		name string
		args args
		want []types.MetricRequest
	}{
		{
			name: "promReadRequest_queries_filled",
			args: args{
				promReadRequest: &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							StartTimestampMs: 0,
							EndTimestampMs:   50000,
							Matchers:         nil,
							Hints: &prompb.ReadHints{
								StepMs:  10000,
								Func:    "avg",
								StartMs: 0,
								EndMs:   50000,
							},
						},
						{
							StartTimestampMs: 50000,
							EndTimestampMs:   100000,
							Matchers:         nil,
							Hints: &prompb.ReadHints{
								StepMs:  5000,
								Func:    "count",
								StartMs: 50000,
								EndMs:   100000,
							},
						},
					},
				},
				index: mockIndex{fixedSearchUUID: "00000000-0000-0000-0000-000000000001"},
			},
			want: []types.MetricRequest{
				{
					UUIDs: []gouuid.UUID{
						gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					},
					FromTimestamp: 0,
					ToTimestamp:   50,
					Step:          10,
					Function:      "avg",
				},
				{
					UUIDs: []gouuid.UUID{
						gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					},
					FromTimestamp: 50,
					ToTimestamp:   100,
					Step:          5,
					Function:      "count",
				},
			},
		},
		{
			name: "promReadRequest_queries_empty",
			args: args{
				promReadRequest: &prompb.ReadRequest{
					Queries: []*prompb.Query{},
				},
				index: nil,
			},
			want: nil,
		},
		{
			name: "promReadRequest_queries_nil",
			args: args{
				promReadRequest: &prompb.ReadRequest{
					Queries: nil,
				},
				index: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := requestsFromPromReadRequest(tt.args.promReadRequest, tt.args.index)
			if err != nil {
				t.Errorf("requestsFromPromReadRequest() failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("requestsFromPromReadRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_promSamplesFromPoints(t *testing.T) {
	type args struct {
		points []types.MetricPoint
	}
	tests := []struct {
		name string
		args args
		want []prompb.Sample
	}{
		{
			name: "points_filled",
			args: args{
				points: []types.MetricPoint{
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
					{
						Timestamp: 50,
						Value:     60,
					},
				},
			},
			want: []prompb.Sample{
				{
					Value:     10,
					Timestamp: 0,
				},
				{
					Value:     20,
					Timestamp: 10000,
				},
				{
					Value:     30,
					Timestamp: 20000,
				},
				{
					Value:     40,
					Timestamp: 30000,
				},
				{
					Value:     50,
					Timestamp: 40000,
				},
				{
					Value:     60,
					Timestamp: 50000,
				},
			},
		},
		{
			name: "points_empty",
			args: args{
				points: []types.MetricPoint{},
			},
			want: nil,
		},
		{
			name: "points_bil",
			args: args{
				points: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := promSamplesFromPoints(tt.args.points); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("promSamplesFromPoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_promSeriesFromMetric(t *testing.T) {
	type args struct {
		uuid  gouuid.UUID
		data  types.MetricData
		index types.Index
	}
	tests := []struct {
		name string
		args args
		want *prompb.TimeSeries
	}{
		{
			name: "metric",
			args: args{
				uuid: gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				data: types.MetricData{
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
						{
							Timestamp: 50,
							Value:     60,
						},
					},
				},
				index: mockIndex{
					fixedLabels: []*prompb.Label{
						{
							Name:  "__name__",
							Value: "up",
						},
						{
							Name:  "monitor",
							Value: "codelab",
						},
					},
				},
			},
			want: &prompb.TimeSeries{
				Labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "up",
					},
					{
						Name:  "monitor",
						Value: "codelab",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     10,
						Timestamp: 0,
					},
					{
						Value:     20,
						Timestamp: 10000,
					},
					{
						Value:     30,
						Timestamp: 20000,
					},
					{
						Value:     40,
						Timestamp: 30000,
					},
					{
						Value:     50,
						Timestamp: 40000,
					},
					{
						Value:     60,
						Timestamp: 50000,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := promSeriesFromMetric(tt.args.uuid, tt.args.data, tt.args.index)
			if err != nil {
				t.Errorf("promSeriesFromMetric() failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("promSeriesFromMetric() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_promTimeseriesFromMetrics(t *testing.T) {
	type args struct {
		metrics map[gouuid.UUID]types.MetricData
		index   types.Index
	}
	tests := []struct {
		name string
		args args
		want []*prompb.TimeSeries
	}{
		{
			name: "metrics_filled",
			args: args{
				metrics: map[gouuid.UUID]types.MetricData{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"): {
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
							{
								Timestamp: 50,
								Value:     60,
							},
						},
					},
				},
				index: mockIndex{
					fixedLabels: []*prompb.Label{
						{
							Name:  "__name__",
							Value: "up",
						},
						{
							Name:  "monitor",
							Value: "codelab",
						},
					},
				},
			},
			want: []*prompb.TimeSeries{
				{
					Labels: []*prompb.Label{
						{
							Name:  "__name__",
							Value: "up",
						},
						{
							Name:  "monitor",
							Value: "codelab",
						},
					},
					Samples: []prompb.Sample{
						{
							Value:     10,
							Timestamp: 0,
						},
						{
							Value:     20,
							Timestamp: 10000,
						},
						{
							Value:     30,
							Timestamp: 20000,
						},
						{
							Value:     40,
							Timestamp: 30000,
						},
						{
							Value:     50,
							Timestamp: 40000,
						},
						{
							Value:     60,
							Timestamp: 50000,
						},
					},
				},
			},
		},
		{
			name: "metrics_empty",
			args: args{
				metrics: make(map[gouuid.UUID]types.MetricData),
				index:   nil,
			},
			want: nil,
		},
		{
			name: "metrics_nil",
			args: args{
				metrics: nil,
				index:   nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := promTimeseriesFromMetrics(tt.args.metrics, tt.args.index)
			if err != nil {
				t.Errorf("promTimeseriesFromMetrics() failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("promTimeseriesFromMetrics() = %v, want %v", got, tt.want)
			}
		})
	}
}

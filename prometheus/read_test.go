package prometheus

import (
	"github.com/prometheus/prometheus/prompb"
	"reflect"
	"squirreldb/types"
	"testing"
)

func uuidFromStringOrNil(s string) types.MetricUUID {
	uuid, _ := types.UUIDFromString(s)

	return uuid
}

func Test_matchersFromPromMatchers(t *testing.T) {
	type args struct {
		promMatchers []*prompb.LabelMatcher
	}
	tests := []struct {
		name string
		args args
		want []types.MetricLabelMatcher
	}{
		{
			name: "promMatchers_filled",
			args: args{
				promMatchers: []*prompb.LabelMatcher{
					{
						Type:  1,
						Name:  "__name__",
						Value: "up",
					},
					{
						Type:  2,
						Name:  "monitor",
						Value: "codelab",
					},
				},
			},
			want: []types.MetricLabelMatcher{
				{
					MetricLabel: types.MetricLabel{
						Name:  "__name__",
						Value: "up",
					},
					Type: 1,
				},
				{
					MetricLabel: types.MetricLabel{
						Name:  "monitor",
						Value: "codelab",
					},
					Type: 2,
				},
			},
		},
		{
			name: "promMatchers_empty",
			args: args{
				promMatchers: []*prompb.LabelMatcher{},
			},
			want: nil,
		},
		{
			name: "promMatchers_nil",
			args: args{
				promMatchers: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchersFromPromMatchers(tt.args.promMatchers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("matchersFromPromMatchers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_requestFromPromQuery(t *testing.T) {
	type args struct {
		promQuery *prompb.Query
		fun       func(matchers []types.MetricLabelMatcher, all bool) []types.MetricUUID
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
				fun: func(matchers []types.MetricLabelMatcher, all bool) []types.MetricUUID {
					uuids := []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					}

					return uuids
				},
			},
			want: types.MetricRequest{
				UUIDs: []types.MetricUUID{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
				fun: func(matchers []types.MetricLabelMatcher, all bool) []types.MetricUUID {
					uuids := []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					}

					return uuids
				},
			},
			want: types.MetricRequest{
				UUIDs: []types.MetricUUID{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
			if got := requestFromPromQuery(tt.args.promQuery, tt.args.fun); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("requestFromPromQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_requestsFromPromReadRequest(t *testing.T) {
	type args struct {
		promReadRequest *prompb.ReadRequest
		fun             func(matchers []types.MetricLabelMatcher, all bool) []types.MetricUUID
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
				fun: func(matchers []types.MetricLabelMatcher, all bool) []types.MetricUUID {
					uuids := []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					}

					return uuids
				},
			},
			want: []types.MetricRequest{
				{
					UUIDs: []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					},
					FromTimestamp: 0,
					ToTimestamp:   50,
					Step:          10,
					Function:      "avg",
				},
				{
					UUIDs: []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
				fun: nil,
			},
			want: nil,
		},
		{
			name: "promReadRequest_queries_nil",
			args: args{
				promReadRequest: &prompb.ReadRequest{
					Queries: nil,
				},
				fun: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requestsFromPromReadRequest(tt.args.promReadRequest, tt.args.fun); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("requestsFromPromReadRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_promLabelsFromLabels(t *testing.T) {
	type args struct {
		labels []types.MetricLabel
	}
	tests := []struct {
		name string
		args args
		want []*prompb.Label
	}{
		{
			name: "labels_filled",
			args: args{
				labels: []types.MetricLabel{
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
			want: []*prompb.Label{
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
		{
			name: "labels_empty",
			args: args{
				labels: []types.MetricLabel{},
			},
			want: nil,
		},
		{
			name: "labels_nil",
			args: args{
				labels: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := promLabelsFromLabels(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("promLabelsFromLabels() = %v, want %v", got, tt.want)
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
		uuid types.MetricUUID
		data types.MetricData
		fun  func(uuid types.MetricUUID) []types.MetricLabel
	}
	tests := []struct {
		name string
		args args
		want *prompb.TimeSeries
	}{
		{
			name: "metric",
			args: args{
				uuid: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
				fun: func(uuid types.MetricUUID) []types.MetricLabel {
					labels := []types.MetricLabel{
						{
							Name:  "__name__",
							Value: "up",
						},
						{
							Name:  "monitor",
							Value: "codelab",
						},
					}

					return labels
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
			if got := promSeriesFromMetric(tt.args.uuid, tt.args.data, tt.args.fun); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("promSeriesFromMetric() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_promTimeseriesFromMetrics(t *testing.T) {
	type args struct {
		metrics map[types.MetricUUID]types.MetricData
		fun     func(uuid types.MetricUUID) []types.MetricLabel
	}
	tests := []struct {
		name string
		args args
		want []*prompb.TimeSeries
	}{
		{
			name: "metrics_filled",
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
							{
								Timestamp: 50,
								Value:     60,
							},
						},
					},
				},
				fun: func(uuid types.MetricUUID) []types.MetricLabel {
					labels := []types.MetricLabel{
						{
							Name:  "__name__",
							Value: "up",
						},
						{
							Name:  "monitor",
							Value: "codelab",
						},
					}

					return labels
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
				metrics: make(map[types.MetricUUID]types.MetricData),
				fun:     nil,
			},
			want: nil,
		},
		{
			name: "metrics_nil",
			args: args{
				metrics: nil,
				fun:     nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := promTimeseriesFromMetrics(tt.args.metrics, tt.args.fun); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("promTimeseriesFromMetrics() = %v, want %v", got, tt.want)
			}
		})
	}
}

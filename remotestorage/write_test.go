package remotestorage

import (
	"errors"
	"reflect"
	"squirreldb/types"
	"testing"

	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"
)

type mockIndex struct {
	fixedLookupUUID string
	fixedSearchUUID string
	fixedLabels     []types.MetricLabel
}

func (i mockIndex) AllUUIDs() ([]gouuid.UUID, error) {
	return nil, errors.New("not implemented")
}
func (i mockIndex) LookupLabels(uuid gouuid.UUID) ([]types.MetricLabel, error) {
	return i.fixedLabels, nil
}

func (i mockIndex) LookupUUID(labels []types.MetricLabel) (gouuid.UUID, error) {
	return uuidFromStringOrNil(i.fixedLookupUUID), nil
}

func (i mockIndex) Search(matchers []types.MetricLabelMatcher) ([]gouuid.UUID, error) {
	if i.fixedSearchUUID == "" {
		return nil, nil
	}
	return []gouuid.UUID{uuidFromStringOrNil(i.fixedSearchUUID)}, nil
}

func Test_labelsFromPromLabels(t *testing.T) {
	type args struct {
		promLabels []*prompb.Label
	}
	tests := []struct {
		name string
		args args
		want []types.MetricLabel
	}{
		{
			name: "promLabels_filled",
			args: args{
				promLabels: []*prompb.Label{
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
			want: []types.MetricLabel{
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
			name: "promLabels_empty",
			args: args{
				promLabels: []*prompb.Label{},
			},
			want: nil,
		},
		{
			name: "promLabels_nil",
			args: args{
				promLabels: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := labelsFromPromLabels(tt.args.promLabels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("labelsFromPromLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_metricFromPromSeries(t *testing.T) {
	type args struct {
		promSeries *prompb.TimeSeries
		index      types.Index
	}
	tests := []struct {
		name  string
		args  args
		want  gouuid.UUID
		want1 types.MetricData
	}{
		{
			name: "promSeries",
			args: args{
				promSeries: &prompb.TimeSeries{
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
				index: mockIndex{fixedLookupUUID: "00000000-0000-0000-0000-000000000001"},
			},
			want: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
			want1: types.MetricData{
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
				TimeToLive: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := metricFromPromSeries(tt.args.promSeries, tt.args.index)
			if err != nil {
				t.Errorf("metricFromPromSeries() failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("metricFromPromSeries() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("metricFromPromSeries() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_metricsFromTimeseries(t *testing.T) {
	type args struct {
		promTimeseries []*prompb.TimeSeries
		index          types.Index
	}
	tests := []struct {
		name string
		args args
		want map[gouuid.UUID]types.MetricData
	}{
		{
			name: "promTimeseries_filled",
			args: args{
				promTimeseries: []*prompb.TimeSeries{
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
				index: mockIndex{fixedLookupUUID: "00000000-0000-0000-0000-000000000001"},
			},
			want: map[gouuid.UUID]types.MetricData{
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
					TimeToLive: 0,
				},
			},
		},
		{
			name: "promTimeseries_empty",
			args: args{
				promTimeseries: []*prompb.TimeSeries{},
				index:          nil,
			},
			want: nil,
		},
		{
			name: "promTimeseries_nil",
			args: args{
				promTimeseries: nil,
				index:          nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := metricsFromTimeseries(tt.args.promTimeseries, tt.args.index)
			if err != nil {
				t.Errorf("metricsFromTimeseries() failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("metricsFromTimeseries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_pointsFromPromSamples(t *testing.T) {
	type args struct {
		promSamples []prompb.Sample
	}
	tests := []struct {
		name string
		args args
		want []types.MetricPoint
	}{
		{
			name: "samples_filled",
			args: args{
				promSamples: []prompb.Sample{
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
			want: []types.MetricPoint{
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
		{
			name: "samples_empty",
			args: args{
				promSamples: []prompb.Sample{},
			},
			want: nil,
		},
		{
			name: "samples_filled",
			args: args{
				promSamples: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pointsFromPromSamples(tt.args.promSamples); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("pointsFromPromSamples() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_timeToLiveFromLabels(t *testing.T) {
	tests := []struct {
		name       string
		labels     []types.MetricLabel
		want       int64
		wantLabels []types.MetricLabel
		wantErr    bool
	}{
		{
			name: "no ttl",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
			want: 0,
			wantLabels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
				{Name: "__ttl__", Value: "3600"},
			},
			want: 3600,
			wantLabels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl2",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
			want: 3600,
			wantLabels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := timeToLiveFromLabels(&tt.labels)
			if (err != nil) != tt.wantErr {
				t.Errorf("timeToLiveFromLabels() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("timeToLiveFromLabels() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.labels, tt.wantLabels) {
				t.Errorf("timeToLiveFromLabels() labels = %v, want %v", tt.labels, tt.wantLabels)
			}
		})
	}
}

func Benchmark_timeToLiveFromLabels(b *testing.B) {
	tests := []struct {
		name       string
		labels     []types.MetricLabel
		wantTTL    int64
		wantLabels []types.MetricLabel
		wantErr    bool
	}{
		{
			name: "no ttl",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "12 labels no ttl",
			labels: []types.MetricLabel{
				{Name: "job", Value: "scrape"},
				{Name: "__name__", Value: "up"},
				{Name: "labels1", Value: "value1"},
				{Name: "labels2", Value: "value2"},
				{Name: "labels3", Value: "value3"},
				{Name: "labels4", Value: "value4"},
				{Name: "labels5", Value: "value5"},
				{Name: "labels6", Value: "value6"},
				{Name: "labels7", Value: "value7"},
				{Name: "labels8", Value: "value8"},
				{Name: "labels9", Value: "value9"},
				{Name: "labels10", Value: "value10"},
			},
		},
		{
			name: "12 labels ttl",
			labels: []types.MetricLabel{
				{Name: "job", Value: "scrape"},
				{Name: "__name__", Value: "up"},
				{Name: "labels1", Value: "value1"},
				{Name: "labels2", Value: "value2"},
				{Name: "labels3", Value: "value3"},
				{Name: "labels4", Value: "value4"},
				{Name: "labels5", Value: "value5"},
				{Name: "labels6", Value: "value6"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "labels7", Value: "value7"},
				{Name: "labels8", Value: "value8"},
				{Name: "labels9", Value: "value9"},
				{Name: "labels10", Value: "value10"},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				labelsIn := make([]types.MetricLabel, len(tt.labels))
				copy(labelsIn, tt.labels)
				_, _ = timeToLiveFromLabels(&labelsIn)
			}
		})
	}
}

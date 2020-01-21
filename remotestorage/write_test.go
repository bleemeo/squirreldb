package remotestorage

import (
	"bytes"
	"errors"
	"io/ioutil"
	"reflect"
	"squirreldb/types"
	"testing"

	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"
)

const defaultTTL = 3600

type mockIndex struct {
	fixedLookupUUID string
	fixedSearchUUID string
	fixedLabels     []*prompb.Label
}

func (i mockIndex) AllUUIDs() ([]gouuid.UUID, error) {
	return nil, errors.New("not implemented")
}
func (i mockIndex) LookupLabels(uuid gouuid.UUID) ([]*prompb.Label, error) {
	return i.fixedLabels, nil
}

func (i mockIndex) LookupUUID(labels []*prompb.Label) (gouuid.UUID, int64, error) {
	return uuidFromStringOrNil(i.fixedLookupUUID), defaultTTL, nil
}

func (i mockIndex) Search(matchers []*prompb.LabelMatcher) ([]gouuid.UUID, error) {
	if i.fixedSearchUUID == "" {
		return nil, nil
	}
	return []gouuid.UUID{uuidFromStringOrNil(i.fixedSearchUUID)}, nil
}

func Test_metricFromPromSeries(t *testing.T) {
	type args struct {
		promSeries *prompb.TimeSeries
		index      types.Index
	}
	tests := []struct {
		name string
		args args
		want types.MetricData
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
			want: types.MetricData{
				UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
				TimeToLive: defaultTTL,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := metricFromPromSeries(tt.args.promSeries, tt.args.index)
			if err != nil {
				t.Errorf("metricFromPromSeries() failed: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("metricFromPromSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_metricsFromPromSeries(b *testing.B) {
	dummyIndex := mockIndex{
		fixedLookupUUID: "00000000-0000-0000-0000-000000000001",
	}
	tests := []string{
		"testdata/write_req_empty",
		"testdata/write_req_one",
		"testdata/write_req_backlog",
		"testdata/write_req_large",
	}
	for _, file := range tests {
		b.Run(file, func(b *testing.B) {
			wr := prompb.WriteRequest{}
			reqCtx := requestContext{
				pb: &wr,
			}
			data, err := ioutil.ReadFile(file)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			reader := bytes.NewReader(data)
			decodeRequest(reader, &reqCtx)
			for n := 0; n < b.N; n++ {
				metricsFromTimeseries(wr.Timeseries, dummyIndex)
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
		want []types.MetricData
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
			want: []types.MetricData{
				{
					UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					TimeToLive: defaultTTL,
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

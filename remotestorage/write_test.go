package remotestorage

import (
	"bytes"
	"errors"
	"io/ioutil"
	"reflect"
	"squirreldb/types"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

const defaultTTL = 3600

type mockIndex struct {
	fixedLookupID types.MetricID
	fixedSearchID types.MetricID
	fixedLabels   []*prompb.Label
}

func (i mockIndex) AllIDs() ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}
func (i mockIndex) LookupLabels(id types.MetricID) ([]*prompb.Label, error) {
	return i.fixedLabels, nil
}

func (i mockIndex) LookupIDs(labelsList [][]*prompb.Label) ([]types.MetricID, []int64, error) {
	if len(labelsList) != 1 {
		return nil, nil, errors.New("not implemented for more than one metrics")

	}

	return []types.MetricID{i.fixedLookupID}, []int64{defaultTTL}, nil
}

func (i mockIndex) Search(matchers []*prompb.LabelMatcher) ([]types.MetricID, error) {
	return []types.MetricID{i.fixedSearchID}, nil
}

func Benchmark_metricsFromPromSeries(b *testing.B) {
	dummyIndex := mockIndex{
		fixedLookupID: MetricIDTest1,
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
				index: mockIndex{fixedLookupID: MetricIDTest1},
			},
			want: []types.MetricData{
				{
					ID: MetricIDTest1,
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10000,
							Value:     20,
						},
						{
							Timestamp: 20000,
							Value:     30,
						},
						{
							Timestamp: 30000,
							Value:     40,
						},
						{
							Timestamp: 40000,
							Value:     50,
						},
						{
							Timestamp: 50000,
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
					Timestamp: 10000,
					Value:     20,
				},
				{
					Timestamp: 20000,
					Value:     30,
				},
				{
					Timestamp: 30000,
					Value:     40,
				},
				{
					Timestamp: 40000,
					Value:     50,
				},
				{
					Timestamp: 50000,
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

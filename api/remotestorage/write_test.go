package remotestorage

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"reflect"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

const defaultTTL = 3600

type mockIndex struct {
	fixedLookupID types.MetricID
	fixedSearchID types.MetricID
	fixedLabels   labels.Labels
}

func (i mockIndex) AllIDs(ctx context.Context, start time.Time, end time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (i mockIndex) LookupIDs(ctx context.Context, requests []types.LookupRequest) ([]types.MetricID, []int64, error) {
	if len(requests) != 1 {
		return nil, nil, errors.New("not implemented for more than one metrics")
	}

	return []types.MetricID{i.fixedLookupID}, []int64{defaultTTL}, nil
}

func (i mockIndex) Search(ctx context.Context, start time.Time, end time.Time, matchers []*labels.Matcher) (types.MetricsSet, error) {
	return &dummy.MetricsLabel{
		List: []types.MetricLabel{{ID: i.fixedSearchID, Labels: i.fixedLabels}},
	}, nil
}

func (i mockIndex) LabelNames(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (i mockIndex) LabelValues(ctx context.Context, start, end time.Time, name string, matchers []*labels.Matcher) ([]string, error) {
	return nil, errors.New("not implemented")
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

			if err := decodeRequest(reader, &reqCtx); err != nil {
				b.Fatal(err)
			}

			for n := 0; n < b.N; n++ {
				if _, _, err := metricsFromTimeseries(context.Background(), wr.Timeseries, dummyIndex); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Test_metricsFromTimeseries(t *testing.T) {
	nowTS := time.Now().Unix() * 1000

	type args struct {
		promTimeseries []prompb.TimeSeries
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
				promTimeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
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
								Timestamp: nowTS + 0,
							},
							{
								Value:     20,
								Timestamp: nowTS + 10000,
							},
							{
								Value:     30,
								Timestamp: nowTS + 20000,
							},
							{
								Value:     40,
								Timestamp: nowTS + 30000,
							},
							{
								Value:     50,
								Timestamp: nowTS + 40000,
							},
							{
								Value:     60,
								Timestamp: nowTS + 50000,
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
							Timestamp: nowTS + 0,
							Value:     10,
						},
						{
							Timestamp: nowTS + 10000,
							Value:     20,
						},
						{
							Timestamp: nowTS + 20000,
							Value:     30,
						},
						{
							Timestamp: nowTS + 30000,
							Value:     40,
						},
						{
							Timestamp: nowTS + 40000,
							Value:     50,
						},
						{
							Timestamp: nowTS + 50000,
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
				promTimeseries: []prompb.TimeSeries{},
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
			got, _, err := metricsFromTimeseries(context.Background(), tt.args.promTimeseries, tt.args.index)
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

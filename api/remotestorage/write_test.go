package remotestorage

import (
	"context"
	"errors"
	"reflect"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	defaultTTL    = 3600
	metricIDTest1 = 1
)

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

func (i mockIndex) Search(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) (types.MetricsSet, error) {
	return &dummy.MetricsLabel{
		List: []types.MetricLabel{{ID: i.fixedSearchID, Labels: i.fixedLabels}},
	}, nil
}

func (i mockIndex) LabelNames(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (i mockIndex) LabelValues(
	ctx context.Context,
	start, end time.Time,
	name string, matchers []*labels.Matcher,
) ([]string, error) {
	return nil, errors.New("not implemented")
}

func Test_metricsFromTimeseries(t *testing.T) {
	nowTS := time.Now().Unix() * 1000

	type args struct {
		promTimeseries []timeSeries
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
				promTimeseries: []timeSeries{
					{
						Labels: labels.Labels{
							{
								Name:  "__name__",
								Value: "up",
							},
							{
								Name:  "monitor",
								Value: "codelab",
							},
						},
						Samples: []types.MetricPoint{
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
				index: mockIndex{fixedLookupID: metricIDTest1},
			},
			want: []types.MetricData{
				{
					ID: metricIDTest1,
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
				promTimeseries: []timeSeries{},
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

func Test_validateLabels(t *testing.T) {
	tests := []struct {
		name           string
		promTimeseries []timeSeries
		wantErr        bool
	}{
		{
			name: "validMetricName",
			promTimeseries: []timeSeries{
				{
					Labels: []labels.Label{
						{
							Name:  "__name__",
							Value: "up",
						},
					},
				},
				{
					Labels: []labels.Label{
						{
							Name:  "__name__",
							Value: "Up",
						},
					},
				},
				{
					Labels: []labels.Label{
						{
							Name:  "__name__",
							Value: "__987daDp:fez",
						},
					},
				},
				{
					Labels: []labels.Label{
						{
							Name:  "__name__",
							Value: ":8_987daDp:fez",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "validLabelName",
			promTimeseries: []timeSeries{
				{
					Labels: []labels.Label{
						{
							Name:  "instance",
							Value: "localhost:8000",
						},
						{
							Name:  "__bleemeo_account__",
							Value: "320663cd-8c99-4c6a-878a-012bebeff9b1",
						},
						{
							Name:  "A1_",
							Value: "a",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalidMetricNameMinus",
			promTimeseries: []timeSeries{
				{
					Labels: []labels.Label{
						{
							Name:  "__name__",
							Value: "TODO-if-absent-not-tsdb-points",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalidMetricNameDigit",
			promTimeseries: []timeSeries{
				{
					Labels: []labels.Label{
						{
							Name:  "__name__",
							Value: "0a",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalidMetricNameEmpty",
			promTimeseries: []timeSeries{
				{
					Labels: []labels.Label{
						{
							Name:  "__name__",
							Value: "",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalidLabelNameMinus",
			promTimeseries: []timeSeries{
				{
					Labels: []labels.Label{
						{
							Name:  "TODO-if-absent-not-tsdb-points",
							Value: "a",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalidLabelNameDigit",
			promTimeseries: []timeSeries{
				{
					Labels: []labels.Label{
						{
							Name:  "0a",
							Value: "a",
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateLabels(tt.promTimeseries); (err == nil) == tt.wantErr {
				t.Fatalf("Failed to validate labels: wantErr=%v, err=%v", tt.wantErr, err)
			}
		})
	}
}

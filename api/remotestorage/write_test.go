// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remotestorage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
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

func (i mockIndex) AllIDs(_ context.Context, _ time.Time, _ time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (i mockIndex) LookupIDs(_ context.Context, requests []types.LookupRequest) ([]types.MetricID, []int64, error) {
	if len(requests) != 1 {
		return nil, nil, errors.New("not implemented for more than one metrics")
	}

	return []types.MetricID{i.fixedLookupID}, []int64{defaultTTL}, nil
}

func (i mockIndex) Search(
	_ context.Context,
	_, _ time.Time,
	_ []*labels.Matcher,
) (types.MetricsSet, error) {
	return &dummy.MetricsLabel{
		List: []types.MetricLabel{{ID: i.fixedSearchID, Labels: i.fixedLabels}},
	}, nil
}

func (i mockIndex) LabelNames(_ context.Context, _, _ time.Time, _ []*labels.Matcher) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (i mockIndex) LabelValues(
	_ context.Context,
	_, _ time.Time,
	_ string, _ []*labels.Matcher,
) ([]string, error) {
	return nil, errors.New("not implemented")
}

func Test_metricsFromTimeSeries(t *testing.T) {
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

	var rs RemoteStorage // Using this sole instance for all test cases because the only
	// fields used in metricsFromTimeSeries are for logging purposes.

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := rs.metricsFromTimeSeries(context.Background(), tt.args.promTimeseries, tt.args.index, 0)
			if err != nil {
				t.Errorf("metricsFromTimeSeries() failed: %v", err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("metricsFromTimeSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_validateLabels(t *testing.T) {
	tests := []struct {
		name    string
		labels  labels.Labels
		wantErr bool
	}{
		{
			name: "validMetricName",
			labels: []labels.Label{
				{
					Name:  "__name__",
					Value: "up",
				},
				{
					Name:  "__name__",
					Value: "Up",
				},
				{
					Name:  "__name__",
					Value: "__987daDp:fez",
				},
				{
					Name:  "__name__",
					Value: ":8_987daDp:fez",
				},
				{
					Name:  "__name__",
					Value: "utilisation mémoire",
				},
			},
			wantErr: false,
		},
		{
			name: "validLabelName",
			labels: []labels.Label{
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
				{
					Name:  "système",
					Value: "a-b",
				},
			},
			wantErr: false,
		},
		{
			name: "invalidMetricNameEmpty",
			labels: []labels.Label{
				{
					Name:  "__name__",
					Value: "",
				},
			},
			wantErr: true,
		},
		{
			name: "invalidMetricNameNotUTF8",
			labels: []labels.Label{
				{
					Name:  "__name__",
					Value: "\xC0",
				},
			},
			wantErr: true,
		},
		{
			name: "invalidMetricNameContainsPipeChar",
			labels: []labels.Label{
				{
					Name:  "__name__",
					Value: "one|two",
				},
			},
			wantErr: true,
		},
		{
			name: "invalidLabelNameNotUTF8",
			labels: []labels.Label{
				{
					Name:  "label-\xC0",
					Value: "value",
				},
			},
			wantErr: true,
		},
		{
			name: "invalidLabelNameContainsPipeChar",
			labels: []labels.Label{
				{
					Name:  "one|two",
					Value: "value",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateLabels(tt.labels); (err == nil) == tt.wantErr {
				t.Fatalf("Failed to validate labels: wantErr=%v, err=%v", tt.wantErr, err)
			}
		})
	}
}

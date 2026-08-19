// Copyright 2015-2026 Bleemeo
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

package tsdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

// mockIndex implements types.Index; only LookupIDs is functional.
// ids[i] and ttls[i] are returned for the i-th request in each call.
type mockIndex struct {
	ids  []types.MetricID
	ttls []int64
}

func (m *mockIndex) LookupIDs(_ context.Context, requests []types.LookupRequest) ([]types.MetricID, []int64, error) {
	return m.ids[:len(requests)], m.ttls[:len(requests)], nil
}

func (m *mockIndex) AllIDs(_ context.Context, _, _ time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (m *mockIndex) Search(_ context.Context, _, _ time.Time, _ []*labels.Matcher) (types.MetricsSet, error) {
	return nil, errors.New("not implemented")
}

func (m *mockIndex) LabelValues(_ context.Context, _, _ time.Time, _ string, _ []*labels.Matcher) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (m *mockIndex) LabelNames(_ context.Context, _, _ time.Time, _ []*labels.Matcher) ([]string, error) {
	return nil, errors.New("not implemented")
}

func newTestAppender(index types.Index, ttl int64) *batchingAppender {
	return &batchingAppender{
		c: &CassandraTSDB{
			index:  index,
			logger: zerolog.Nop(),
		},
		ttl:               ttl,
		ctx:               context.Background(),
		pendingTimeSeries: make(map[uint64]types.Timeseries),
	}
}

func TestResolveMetricIDs(t *testing.T) {
	nowTS := time.Now().Unix() * 1000

	tests := []struct {
		name   string
		series []types.Timeseries
		ids    []types.MetricID
		ttls   []int64
		want   []types.MetricData
	}{
		{
			name: "single_series",
			series: []types.Timeseries{
				{
					Labels: labels.FromStrings("__name__", "up", "job", "test"),
					Points: []types.MetricPoint{
						{Timestamp: nowTS, Value: 10},
						{Timestamp: nowTS + 10000, Value: 20},
						{Timestamp: nowTS + 20000, Value: 30},
					},
				},
			},
			ids:  []types.MetricID{1},
			ttls: []int64{3600},
			want: []types.MetricData{
				{
					ID: 1,
					Points: []types.MetricPoint{
						{Timestamp: nowTS, Value: 10},
						{Timestamp: nowTS + 10000, Value: 20},
						{Timestamp: nowTS + 20000, Value: 30},
					},
					TimeToLive: 3600,
				},
			},
		},
		{
			name:   "empty_input",
			series: []types.Timeseries{},
			want:   nil,
		},
		{
			name: "same_id_merges_points_and_takes_max_ttl",
			series: []types.Timeseries{
				{
					Labels: labels.FromStrings("__name__", "up", "job", "a"),
					Points: []types.MetricPoint{{Timestamp: nowTS, Value: 1}},
				},
				{
					Labels: labels.FromStrings("__name__", "up", "job", "b"),
					Points: []types.MetricPoint{{Timestamp: nowTS + 10000, Value: 2}},
				},
			},
			ids:  []types.MetricID{42, 42},
			ttls: []int64{3600, 7200},
			want: []types.MetricData{
				{
					ID: 42,
					Points: []types.MetricPoint{
						{Timestamp: nowTS, Value: 1},
						{Timestamp: nowTS + 10000, Value: 2},
					},
					TimeToLive: 7200,
				},
			},
		},
		{
			name: "distinct_ids_stay_separate",
			series: []types.Timeseries{
				{
					Labels: labels.FromStrings("__name__", "up", "job", "a"),
					Points: []types.MetricPoint{{Timestamp: nowTS, Value: 1}},
				},
				{
					Labels: labels.FromStrings("__name__", "up", "job", "b"),
					Points: []types.MetricPoint{{Timestamp: nowTS + 10000, Value: 2}},
				},
			},
			ids:  []types.MetricID{1, 2},
			ttls: []int64{3600, 3600},
			want: []types.MetricData{
				{
					ID:         1,
					Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 1}},
					TimeToLive: 3600,
				},
				{
					ID:         2,
					Points:     []types.MetricPoint{{Timestamp: nowTS + 10000, Value: 2}},
					TimeToLive: 3600,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newTestAppender(&mockIndex{ids: tt.ids, ttls: tt.ttls}, 3600)

			got, err := app.resolveMetricIDs(t.Context(), tt.series)
			if err != nil {
				t.Fatalf("resolveMetricIDs() unexpected error: %v", err)
			}

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("resolveMetricIDs() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

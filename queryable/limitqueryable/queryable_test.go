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

package limitqueryable

import (
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/tests/testcases"
	"github.com/bleemeo/squirreldb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/prometheus/model/labels"
)

// Ensure it Queryable implement the QueryableWithOptions interface (compile time).
var _ types.QueryableWithOptions = &Queryable{}

func TestFilterMatchers(t *testing.T) {
	metricsConstant := testcases.GetConstant()
	labelsMetric1 := metricsConstant.LabelsMetric1
	labelsMetric2 := metricsConstant.LabelsMetric2
	labelsMetric3 := metricsConstant.LabelsMetric3

	tsdb := new(dummy.MemoryTSDB)
	tsdb.AddData(
		[]types.Timeseries{
			{Labels: labelsMetric1.Copy(), Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}}},
			{Labels: labelsMetric2.Copy(), Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}}},
			{Labels: labelsMetric3.Copy(), Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}}},
		},
		86400,
	)

	tests := []struct {
		name                string
		options             Options
		wantSelect          []labels.Labels
		wantNames           []string
		wantMountpathValues []string
		matcherQuery        []*labels.Matcher
	}{
		{
			name: "filter-account-id-1",
			options: Options{
				ForcedMatchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						testcases.AccountLabel,
						"1234",
					),
				},
			},
			matcherQuery: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
			},
			wantSelect:          []labels.Labels{labelsMetric1},
			wantNames:           []string{testcases.AccountLabel, "__name__", "mountpath"},
			wantMountpathValues: []string{"/home"},
		},
		{
			name: "filter-account-id-1-query-empty-tenant",
			options: Options{
				ForcedMatchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						testcases.AccountLabel,
						"1234",
					),
				},
			},
			matcherQuery: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
				labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, ""),
			},
			wantSelect:          []labels.Labels{},
			wantNames:           []string{},
			wantMountpathValues: []string{},
		},
		{
			name: "filter-account-id-2",
			options: Options{
				ForcedMatchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						testcases.AccountLabel,
						"5678",
					),
				},
			},
			matcherQuery: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
			},
			wantSelect:          []labels.Labels{labelsMetric2},
			wantNames:           []string{testcases.AccountLabel, "__name__", "mountpath"},
			wantMountpathValues: []string{"/srv"},
		},
		{
			name: "filter-account-id-absent",
			options: Options{
				ForcedMatchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						testcases.AccountLabel,
						"",
					),
				},
			},
			matcherQuery: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
			},
			wantSelect:          []labels.Labels{},
			wantNames:           []string{},
			wantMountpathValues: []string{},
		},
		{
			name: "filter-name-absent",
			options: Options{
				ForcedMatchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						testcases.LabelName,
						"",
					),
				},
			},
			matcherQuery: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
			},
			wantSelect:          []labels.Labels{},
			wantNames:           []string{},
			wantMountpathValues: []string{},
		},
		{
			name: "filter-name",
			options: Options{
				ForcedMatchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						testcases.LabelName,
						"disk_used",
					),
				},
			},
			matcherQuery: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "mountpath", "/srv"),
			},
			wantSelect:          []labels.Labels{labelsMetric2},
			wantNames:           []string{testcases.AccountLabel, "__name__", "mountpath"},
			wantMountpathValues: []string{"/srv"},
		},
		{
			name: "filter_match_all",
			options: Options{
				ForcedMatchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchNotEqual,
						testcases.AccountLabel,
						"",
					),
				},
			},
			matcherQuery: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, testcases.LabelName, "disk.*"),
			},
			wantSelect:          []labels.Labels{labelsMetric1, labelsMetric3, labelsMetric2},
			wantNames:           []string{testcases.AccountLabel, "__name__", "fstype", "mountpath"},
			wantMountpathValues: []string{"/home", "/srv"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryable := NewLimiterQueryable(tsdb, tt.options)

			querier, err := queryable.Querier(
				time.Now().Add(-24*time.Hour).UnixMilli(),
				time.Now().UnixMilli(),
			)
			if err != nil {
				t.Fatal(err)
			}

			series := querier.Select(t.Context(), true, nil, tt.matcherQuery...)

			ts, err := convert.SeriesSetToTimeSeries(series)
			if err != nil {
				t.Fatal(err)
			}

			gotLabels := tsToLabels(ts)

			if diff := cmp.Diff(tt.wantSelect, gotLabels, cmp.Comparer(labels.Equal)); diff != "" {
				t.Errorf("Select() mismatch: (-want +got)\n%s", diff)
			}

			gotNames, _, err := querier.LabelNames(t.Context(), nil, tt.matcherQuery...)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tt.wantNames, gotNames, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("LabelNames() mismatch: (-want +got)\n%s", diff)
			}

			gotValues, _, err := querier.LabelValues(t.Context(), "mountpath", nil, tt.matcherQuery...)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tt.wantMountpathValues, gotValues, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("LabelNames() mismatch: (-want +got)\n%s", diff)
			}
		})
	}
}

func TestLimitSeries(t *testing.T) {
	metricsConstant := testcases.GetConstant()
	labelsMetric1 := metricsConstant.LabelsMetric1
	labelsMetric2 := metricsConstant.LabelsMetric2
	labelsMetric3 := metricsConstant.LabelsMetric3

	tsdb := new(dummy.MemoryTSDB)
	tsdb.AddData(
		[]types.Timeseries{
			{Labels: labelsMetric1.Copy(), Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}}},
			{Labels: labelsMetric2.Copy(), Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}}},
			{Labels: labelsMetric3.Copy(), Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}}},
		},
		86400,
	)

	type search struct {
		matchers  []*labels.Matcher
		wantCount int
		wantErr   bool
	}

	tests := []struct {
		name     string
		options  Options
		searches []search
	}{
		{
			name: "get-high-limit",
			options: Options{
				MaxEvaluatedSeries: 5000,
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, testcases.LabelName, "disk_.*"),
					},
					wantCount: 3,
					wantErr:   false,
				},
			},
		},
		{
			name: "get-medium-limit",
			options: Options{
				MaxEvaluatedSeries: 5,
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, testcases.LabelName, "disk_.*"),
					},
					wantCount: 1,
					wantErr:   true,
				},
			},
		},
		{
			name: "get-low-limit",
			options: Options{
				MaxEvaluatedSeries: 2,
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantErr: true,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, testcases.LabelName, "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "get-very-low-limit",
			options: Options{
				MaxEvaluatedSeries: 1,
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCount: 1,
					wantErr:   true,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantErr: true,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, testcases.LabelName, "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryable := NewLimiterQueryable(tsdb, tt.options)

			querier, err := queryable.Querier(
				time.Now().Add(-24*time.Hour).UnixMilli(),
				time.Now().UnixMilli(),
			)
			if err != nil {
				t.Fatal(err)
			}

			for i, query := range tt.searches {
				series := querier.Select(t.Context(), true, nil, query.matchers...)

				ts, err := convert.SeriesSetToTimeSeries(series)
				if (err != nil) != query.wantErr {
					t.Fatalf("err=%v and wantErr=%v", err, query.wantErr)
				}

				if len(ts) != query.wantCount {
					t.Errorf("limitingIndex.Search(#%d).Count() = %d, want %d", i, len(ts), query.wantCount)
				}
			}
		})
	}
}

func TestLimitSamples(t *testing.T) {
	metricsConstant := testcases.GetConstant()
	labelsMetric1 := metricsConstant.LabelsMetric1
	labelsMetric2 := metricsConstant.LabelsMetric2
	labelsMetric3 := metricsConstant.LabelsMetric3

	tsdb := new(dummy.MemoryTSDB)
	tsdb.AddData(
		[]types.Timeseries{
			{Labels: labelsMetric1.Copy(), Points: generatePoints(100)},
			{Labels: labelsMetric2.Copy(), Points: generatePoints(100)},
			{Labels: labelsMetric3.Copy(), Points: generatePoints(100)},
		},
		86400,
	)

	type readRequest struct {
		matchers        []*labels.Matcher
		wantCountPoints uint64
		wantErr         bool
	}

	tests := []struct {
		name    string
		options Options
		reads   []readRequest
	}{
		{
			name: "high-limit",
			options: Options{
				MaxEvaluatedPoints: 500000,
			},
			reads: []readRequest{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, "1234"),
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 100,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 200,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, "5678"),
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 100,
				},
			},
		},
		{
			name: "medium-limit",
			options: Options{
				MaxEvaluatedPoints: 300,
			},
			reads: []readRequest{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, "1234"),
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 100,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 200,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, "5678"),
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 0,
					wantErr:         true,
				},
			},
		},
		{
			name: "low-limit",
			options: Options{
				MaxEvaluatedPoints: 101,
			},
			reads: []readRequest{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, "1234"),
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 100,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 1,
					wantErr:         true,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, "5678"),
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 0,
					wantErr:         true,
				},
			},
		},
		{
			name: "very-low-limit",
			options: Options{
				MaxEvaluatedPoints: 99,
			},
			reads: []readRequest{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, "1234"),
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 99,
					wantErr:         true,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 0,
					wantErr:         true,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, testcases.AccountLabel, "5678"),
						labels.MustNewMatcher(labels.MatchEqual, testcases.LabelName, "disk_used"),
					},
					wantCountPoints: 0,
					wantErr:         true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryable := NewLimiterQueryable(tsdb, tt.options)

			querier, err := queryable.Querier(
				time.Now().Add(-24*time.Hour).UnixMilli(),
				time.Now().UnixMilli(),
			)
			if err != nil {
				t.Fatal(err)
			}

			for i, query := range tt.reads {
				series := querier.Select(t.Context(), true, nil, query.matchers...)

				ts, err := convert.SeriesSetToTimeSeries(series)
				if (err != nil) != query.wantErr {
					t.Fatalf("err=%v and wantErr=%v", err, query.wantErr)
				}

				gotCount := countPoints(ts)
				if gotCount != query.wantCountPoints {
					t.Errorf("Select(#%d).Count() = %d, want %d", i, gotCount, query.wantCountPoints)
				}
			}
		})
	}
}

func tsToLabels(data []types.Timeseries) []labels.Labels {
	result := make([]labels.Labels, len(data))

	for i, row := range data {
		result[i] = row.Labels
	}

	return result
}

func generatePoints(count int) []types.MetricPoint {
	result := make([]types.MetricPoint, count)
	now := time.Now().UnixMilli()

	for i := range count {
		result[i] = types.MetricPoint{
			Timestamp: now - (time.Duration(count-i) * 10 * time.Second).Milliseconds(),
			Value:     float64(i),
		}
	}

	return result
}

func countPoints(ts []types.Timeseries) uint64 {
	result := uint64(0)

	for _, row := range ts {
		result += uint64(len(row.Points))
	}

	return result
}

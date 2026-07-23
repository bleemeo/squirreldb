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

package testcases

import (
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type ReadTestCase struct {
	Name      string
	StoreData []types.Timeseries
	Queries   []QueryCase
}

func GetReadTestCase() []ReadTestCase {
	metricsConstant := GetConstant()
	labelsMetric1 := metricsConstant.LabelsMetric1
	labelsMetric2 := metricsConstant.LabelsMetric2
	labelsMetric3 := metricsConstant.LabelsMetric3

	now := time.Now()
	defaultStep := 10 * time.Second
	minutesPoints := int64((time.Minute) / defaultStep)

	defaultData := []types.Timeseries{
		{
			Labels: labelsMetric1,
			Points: GenerateFixedValuePoints(now.UnixMilli(), defaultStep.Milliseconds(), 100, 4.2e+09),
		},
		{
			Labels: labelsMetric2,
			Points: GenerateFixedValuePoints(now.UnixMilli(), defaultStep.Milliseconds(), 100, 42e+09),
		},
		{
			Labels: labelsMetric3,
			Points: GenerateFixedValuePoints(now.UnixMilli(), defaultStep.Milliseconds(), 100, 4e+02),
		},
	}

	lastMinutes1 := GenerateFixedValuePoints(now.UnixMilli(), defaultStep.Milliseconds(), minutesPoints, 4.2e+09)
	lastMinutes2 := GenerateFixedValuePoints(now.UnixMilli(), defaultStep.Milliseconds(), minutesPoints, 42e+09)
	lastMinutes3 := GenerateFixedValuePoints(now.UnixMilli(), defaultStep.Milliseconds(), minutesPoints, 4e+02)

	return []ReadTestCase{
		{
			Name:      "simple-select",
			StoreData: defaultData,
			Queries: []QueryCase{
				{
					Name: "no-sort-single-series",
					MinT: now.Add(-time.Minute),
					MaxT: now,
					Requests: []QueryRequest{
						{
							Sort: false,
							Matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant1),
								labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName1),
							},
							Want: []types.Timeseries{
								{Labels: labelsMetric1, Points: lastMinutes1},
							},
						},
						{
							Sort: false,
							Matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant2),
								labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName1),
							},
							Want: []types.Timeseries{
								{Labels: labelsMetric2, Points: lastMinutes2},
							},
						},
					},
				},
				{
					Name: "sort-single-series",
					MinT: now.Add(-time.Minute),
					MaxT: now,
					Requests: []QueryRequest{
						{
							Sort: true,
							Matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant2),
								labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName2),
							},
							Want: []types.Timeseries{
								{Labels: labelsMetric3, Points: lastMinutes3},
							},
						},
						{
							Sort: true,
							Matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant2),
								labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName1),
							},
							Want: []types.Timeseries{
								{Labels: labelsMetric2, Points: lastMinutes2},
							},
						},
					},
				},
				{
					Name: "sort-multi-series",
					MinT: now.Add(-time.Minute),
					MaxT: now,
					Requests: []QueryRequest{
						{
							Sort: true,
							Matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName1),
							},
							Want: []types.Timeseries{
								{Labels: labelsMetric1, Points: lastMinutes1},
								{Labels: labelsMetric2, Points: lastMinutes2},
							},
						},
						{
							Sort: true,
							Matchers: []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant2),
							},
							Want: []types.Timeseries{
								{Labels: labelsMetric3, Points: lastMinutes3},
								{Labels: labelsMetric2, Points: lastMinutes2},
							},
						},
					},
				},
			},
		},
	}
}

func (c ReadTestCase) RunTest(t *testing.T, queryable storage.Queryable) {
	t.Helper()

	for _, qc := range c.Queries {
		t.Run(qc.Name, func(t *testing.T) {
			t.Parallel()

			querier, err := queryable.Querier(qc.MinT.UnixMilli(), qc.MaxT.UnixMilli())
			if err != nil {
				t.Fatal(err)
			}

			runReadQueriesTest(t, qc, querier)
		})
	}
}

func runReadQueriesTest(t *testing.T, qc QueryCase, querier storage.Querier) {
	t.Helper()

	for i, req := range qc.Requests {
		series := querier.Select(t.Context(), req.Sort, nil, req.Matchers...)

		got, err := convert.SeriesSetToTimeSeries(series)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(req.Want, got, cmp.Comparer(labels.Equal)); diff != "" {
			t.Errorf("Select(req#%d) mismatch: (-want +got)\n%s", i, diff)
		}
	}
}

func GenerateFixedValuePoints(maxT int64, stepT int64, count int64, value float64) []types.MetricPoint {
	result := make([]types.MetricPoint, count)

	for i := range result {
		result[i] = types.MetricPoint{
			Timestamp: maxT - stepT*(count-int64(i)),
			Value:     value,
		}
	}

	return result
}

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

// Package testcases provides shared test cases for backends implementations (Appender, Queryable, ...).
package testcases

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// PromStore is the minimal interface required to run the appender tests.
// Implementations must support both writing via AppenderWithTTL and reading via Querier.
type PromStore interface {
	storage.Appendable
	storage.Queryable
}

// ReadWriteTestCase defines one scenario: a sequence of commits followed by queries to verify results.
type ReadWriteTestCase struct {
	Name    string
	Commits []CommitOp
	Queries []QueryCase
}

// CommitOp describes one appender lifecycle: AppenderWithTTL → Append×N → Commit (or Rollback).
// Multiple entries in Timeseries with identical Labels are valid and test merge behavior.
type CommitOp struct {
	Timeseries []types.Timeseries
	Rollback   bool
}

// QueryCase describes a query window and the expected results inside it.
type QueryCase struct {
	Name     string
	MinT     time.Time
	MaxT     time.Time
	Requests []QueryRequest
}

// QueryRequest is a single Select call with its expected results.
type QueryRequest struct {
	Sort     bool
	Matchers []*labels.Matcher
	Want     []types.Timeseries
}

// GetReadWriteTestCases returns the test suite that every AppendableWithTTL + Queryable backend should pass.
func GetReadWriteTestCases() []ReadWriteTestCase {
	now := time.Now()
	step := 10 * time.Second
	stepMs := step.Milliseconds()
	base := now.Add(-5 * time.Minute).UnixMilli()

	metricsConstant := GetConstant()
	labelsMetric1 := metricsConstant.LabelsMetric1
	labelsMetric2 := metricsConstant.LabelsMetric2
	labelsMetric3 := metricsConstant.LabelsMetric3

	points1 := generatePoints(base, stepMs, 3, 1.0)
	points2 := generatePoints(base, stepMs, 3, 2.0)
	points3 := generatePoints(base, stepMs, 3, 3.0)

	// Two non-overlapping batches used in the same-series test.
	pointsA := generatePoints(base, stepMs, 2, 1.0)
	pointsB := generatePoints(base+2*stepMs, stepMs, 2, 1.5)

	queryWindow := func(name string, requests []QueryRequest) QueryCase {
		return QueryCase{
			Name:     name,
			MinT:     now.Add(-10 * time.Minute),
			MaxT:     now,
			Requests: requests,
		}
	}

	return []ReadWriteTestCase{
		{
			Name: "simple-write",
			Commits: []CommitOp{
				{
					Timeseries: []types.Timeseries{
						{Labels: labelsMetric1, Points: points1},
						{Labels: labelsMetric2, Points: points2},
						{Labels: labelsMetric3, Points: points3},
					},
				},
			},
			Queries: []QueryCase{
				queryWindow("single-series-by-tenant", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant1),
						},
						Want: []types.Timeseries{{Labels: labelsMetric1, Points: points1}},
					},
				}),
				queryWindow("single-series-by-name", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName2),
						},
						Want: []types.Timeseries{{Labels: labelsMetric3, Points: points3}},
					},
				}),
				queryWindow("multi-series-sorted", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName1),
						},
						Want: []types.Timeseries{
							{Labels: labelsMetric1, Points: points1},
							{Labels: labelsMetric2, Points: points2},
						},
					},
				}),
			},
		},
		{
			// This case is here to ensure isolation between each TestCases
			Name: "isolation",
			Queries: []QueryCase{
				queryWindow("single-series-by-tenant", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant1),
						},
						Want: []types.Timeseries{},
					},
				}),
				queryWindow("single-series-by-name", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName2),
						},
						Want: []types.Timeseries{},
					},
				}),
				queryWindow("multi-series-sorted", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName1),
						},
						Want: []types.Timeseries{},
					},
				}),
			},
		},
		{
			Name: "multi-commit",
			Commits: []CommitOp{
				{
					Timeseries: []types.Timeseries{{Labels: labelsMetric1, Points: points1}},
				},
				{
					Timeseries: []types.Timeseries{{Labels: labelsMetric2, Points: points2}},
				},
			},
			Queries: []QueryCase{
				queryWindow("both-commits-visible", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, LabelName, MetricName1),
						},
						Want: []types.Timeseries{
							{Labels: labelsMetric1, Points: points1},
							{Labels: labelsMetric2, Points: points2},
						},
					},
				}),
			},
		},
		{
			Name: "rollback",
			Commits: []CommitOp{
				{
					Timeseries: []types.Timeseries{{Labels: labelsMetric1, Points: points1}},
					Rollback:   true,
				},
			},
			Queries: []QueryCase{
				queryWindow("no-data-after-rollback", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant1),
						},
						Want: nil,
					},
				}),
			},
		},
		{
			Name: "same-series-multiple-appends",
			Commits: []CommitOp{
				{
					// Two entries with the same Labels simulate multiple Append calls for
					// the same series within a single commit. The backend must merge them.
					Timeseries: []types.Timeseries{
						{Labels: labelsMetric1, Points: pointsA},
						{Labels: labelsMetric1, Points: pointsB},
					},
				},
			},
			Queries: []QueryCase{
				queryWindow("all-points-present", []QueryRequest{
					{
						Sort: true,
						Matchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, AccountLabel, Tenant1),
						},
						Want: []types.Timeseries{
							{Labels: labelsMetric1, Points: append(pointsA, pointsB...)},
						},
					},
				}),
			},
		},
	}
}

// RunTest writes all commits, then verifies all queries against the provided TSDB.
func (c ReadWriteTestCase) RunTest(t *testing.T, tsdb PromStore) {
	t.Helper()

	err := c.execCase(t.Context(), t, tsdb)
	if err != nil {
		t.Error(err)
	}
}

func (c ReadWriteTestCase) ExecCase(ctx context.Context, tsdb PromStore) error {
	return c.execCase(ctx, nil, tsdb)
}

type joinError interface {
	Unwrap() []error
}

func (c ReadWriteTestCase) execCase(ctx context.Context, optionalT *testing.T, tsdb PromStore) error { //nolint:thelper
	var errs []error

	for i, op := range c.Commits {
		app := tsdb.Appender(ctx)

		for _, ts := range op.Timeseries {
			for _, pt := range ts.Points {
				if _, err := app.Append(0, ts.Labels, pt.Timestamp, pt.Value); err != nil {
					return fmt.Errorf("commit %d: Append: %w", i, err)
				}
			}
		}

		if op.Rollback {
			if err := app.Rollback(); err != nil {
				return fmt.Errorf("commit %d: Rollback: %w", i, err)
			}
		} else {
			if err := app.Commit(); err != nil {
				return fmt.Errorf("commit %d: Commit: %w", i, err)
			}
		}
	}

	for _, qc := range c.Queries {
		subTask := func() error {
			var subErrs []error

			querier, err := tsdb.Querier(qc.MinT.UnixMilli(), qc.MaxT.UnixMilli())
			if err != nil {
				return err
			}

			defer querier.Close()

			for i, req := range qc.Requests {
				series := querier.Select(ctx, req.Sort, nil, req.Matchers...)

				got, err := convert.SeriesSetToTimeSeries(series)
				if err != nil {
					return err
				}

				if diff := cmp.Diff(req.Want, got, cmp.Comparer(labels.Equal), cmpopts.EquateEmpty()); diff != "" {
					subErrs = append(subErrs, fmt.Errorf("Select(req#%d) mismatch: (-want +got)\n%s", i, diff))
				}
			}

			return errors.Join(subErrs...)
		}

		if optionalT != nil { //nolint:nestif
			optionalT.Run(qc.Name, func(t *testing.T) {
				if err := subTask(); err != nil {
					if errList, ok := err.(joinError); ok {
						for _, err := range errList.Unwrap() {
							t.Error(err)
						}
					} else {
						t.Error(err)
					}
				}
			})
		} else {
			var (
				wg sync.WaitGroup
				l  sync.Mutex
			)

			wg.Go(func() {
				err := subTask()

				l.Lock()
				defer l.Unlock()

				if err != nil {
					errs = append(errs, err)
				}
			})

			wg.Wait()
		}
	}

	return errors.Join(errs...)
}

func generatePoints(startT, stepT, count int64, value float64) []types.MetricPoint {
	result := make([]types.MetricPoint, count)

	for i := range result {
		result[i] = types.MetricPoint{
			Timestamp: startT + stepT*int64(i),
			Value:     value,
		}
	}

	return result
}

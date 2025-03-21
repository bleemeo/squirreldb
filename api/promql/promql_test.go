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

package promql

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestPromQL_queryable(t *testing.T) { //nolint:maintidx
	now := time.Now()
	idx := dummy.Index{
		StoreMetricIDInMemory: true,
	}

	_, _, err := idx.LookupIDs(
		t.Context(),
		[]types.LookupRequest{
			{Labels: labelsMetric1.Copy(), Start: now, End: now},
			{Labels: labelsMetric2.Copy(), Start: now, End: now},
			{Labels: labelsMetric3.Copy(), Start: now, End: now},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	type fields struct {
		Index              types.Index
		Reader             *mockStore
		MaxEvaluatedSeries uint32
		MaxEvaluatedPoints uint64
	}

	type search struct {
		matchers  []*labels.Matcher
		wantCount int
		wantErr   bool
	}

	tests := []struct {
		name                  string
		fields                fields
		reqHeader             map[string]string
		searches              []search
		wantForcePreaggregate bool
		wantForceRaw          bool
	}{
		{
			name: "no-header",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
			},
			reqHeader: map[string]string{},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantCount: 3,
					wantErr:   false,
				},
			},
		},
		{
			name: "no-header-with-default",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			reqHeader: map[string]string{},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "filter-account",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
			},
			reqHeader: map[string]string{
				types.HeaderForcedMatcher: "__account_id=1234",
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 1,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantCount: 1,
					wantErr:   false,
				},
			},
		},
		{
			name: "limit-series",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
			},
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedSeries: "2",
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "limit-series-and-filter-account",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
			},
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedSeries: "2",
				types.HeaderForcedMatcher:      "__account_id=1234",
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 1,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantCount: 1,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "limit-points",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
			},
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedPoints: "200",
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "limit-points-filter-account",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
			},
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedPoints: "200",
				types.HeaderForcedMatcher:      "__account_id=1234",
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 1,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantCount: 1,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "limit-points-limit-series-filter-account",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
			},
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedSeries: "2",
				types.HeaderMaxEvaluatedPoints: "200",
				types.HeaderForcedMatcher:      "__account_id=1234",
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 1,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantCount: 1,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "limit-points-limit-series-filter-account-lower-default",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedSeries: "3",
				types.HeaderMaxEvaluatedPoints: "500",
				types.HeaderForcedMatcher:      "__account_id=1234",
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 1,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantCount: 1,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantCount: 1,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "header-no-force-aggregated",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			reqHeader: map[string]string{
				types.HeaderForcePreAggregated: "false",
			},
			wantForcePreaggregate: false,
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "header-force-aggregated",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			reqHeader: map[string]string{
				types.HeaderForcePreAggregated: "true",
			},
			wantForcePreaggregate: true,
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "header-no-force-raw",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			reqHeader: map[string]string{
				types.HeaderForceRaw: "false",
			},
			wantForceRaw: false,
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "header-force-raw",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			reqHeader: map[string]string{
				types.HeaderForceRaw: "true",
			},
			wantForceRaw: true,
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
		{
			name: "header-force-both",
			fields: fields{
				Index: &idx,
				Reader: &mockStore{
					pointsPerSeries: 100,
				},
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			reqHeader: map[string]string{
				types.HeaderForceRaw:           "true",
				types.HeaderForcePreAggregated: "true",
			},
			wantForceRaw:          true,
			wantForcePreaggregate: false,
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantCount: 2,
					wantErr:   false,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "__name__", "disk_.*"),
					},
					wantErr: true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryable := Store{
				Index:                     tt.fields.Index,
				Reader:                    tt.fields.Reader,
				DefaultMaxEvaluatedPoints: tt.fields.MaxEvaluatedPoints,
				DefaultMaxEvaluatedSeries: tt.fields.MaxEvaluatedSeries,
			}

			r := httptest.NewRequest("", "/", nil)

			for k, v := range tt.reqHeader {
				r.Header.Add(k, v)
			}

			queryier, err := queryable.Querier(0, 0)
			if err != nil {
				t.Fatal(err)
			}

			ctx, err := queryable.ContextFromRequest(r)
			if err != nil {
				t.Fatal("Failed to parse request:", err)
			}

			for i, query := range tt.searches {
				got := queryier.Select(ctx, false, nil, query.matchers...)
				count, err := countSeries(got)

				for _, req := range tt.fields.Reader.readRequest {
					if req.ForcePreAggregated != tt.wantForcePreaggregate {
						t.Errorf("ForcePreAggregated = %v, want %v", req.ForcePreAggregated, tt.wantForcePreaggregate)
					}
				}

				if (err != nil) != query.wantErr {
					t.Errorf("PromQL.queryable.Select(#%d) error = %v, wantErr %v", i, err, query.wantErr)

					return
				} else if count != query.wantCount {
					t.Errorf("len(PromQL.queryable.Select(#%d)) = %d, want %d", i, count, query.wantCount)
				}
			}
		})
	}
}

func countSeries(iter storage.SeriesSet) (int, error) {
	count := 0

	if iter == nil {
		return 0, errors.New("nil SeriesSet")
	}

	for iter.Next() {
		count++
	}

	return count, iter.Err()
}

func TestPromQL_InvalidForcedMatcher(t *testing.T) {
	idx := dummy.Index{
		StoreMetricIDInMemory: true,
	}

	reader := &mockStore{
		pointsPerSeries: 100,
	}

	queryable := Store{
		Index:  &idx,
		Reader: reader,
	}

	r := httptest.NewRequest("", "/", nil)
	r.Header.Add(types.HeaderForcedMatcher, "invalid")

	_, err := queryable.ContextFromRequest(r)
	if err == nil {
		t.Fatal("Expected the request to be invalid (errInvalidMatcher)")
	}

	if !errors.Is(err, errInvalidMatcher) {
		t.Fatalf("Unexpected error: want %v, got %v", errInvalidMatcher, err)
	}
}

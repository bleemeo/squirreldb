// Disable stylecheck because is complain on error message (should not be capitalized)
// but we prefer keeping the exact message used by Prometheus.

// nolint: stylecheck
package promql

import (
	"context"
	"errors"
	"net/http/httptest"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

func TestPromQL_queryable(t *testing.T) {
	now := time.Now()
	idx := dummy.Index{
		StoreMetricIDInMemory: true,
	}
	_, _, err := idx.LookupIDs(
		context.Background(),
		[]types.LookupRequest{
			{Labels: labelsMetric1.Copy(), Start: now, End: now},
			{Labels: labelsMetric2.Copy(), Start: now, End: now},
			{Labels: labelsMetric3.Copy(), Start: now, End: now},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	reader := mockStore{
		pointsPerSeries: 100,
	}

	type fields struct {
		Index              types.Index
		Reader             types.MetricReader
		MaxEvaluatedSeries uint32
		MaxEvaluatedPoints uint64
	}
	type search struct {
		matchers  []*labels.Matcher
		wantCount int
		wantErr   bool
	}

	tests := []struct {
		name      string
		fields    fields
		reqHeader map[string]string
		searches  []search
	}{
		{
			name: "no-header",
			fields: fields{
				Index:  &idx,
				Reader: reader,
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
				Index:              &idx,
				Reader:             reader,
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
				Index:  &idx,
				Reader: reader,
			},
			reqHeader: map[string]string{
				"X-PromQL-Forced-Matcher": "__account_id=1234",
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
				Index:  &idx,
				Reader: reader,
			},
			reqHeader: map[string]string{
				"X-PromQL-Max-Evaluated-Series": "2",
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
				Index:  &idx,
				Reader: reader,
			},
			reqHeader: map[string]string{
				"X-PromQL-Max-Evaluated-Series": "2",
				"X-PromQL-Forced-Matcher":       "__account_id=1234",
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
				Index:  &idx,
				Reader: reader,
			},
			reqHeader: map[string]string{
				"X-PromQL-Max-Evaluated-Points": "200",
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
				Index:  &idx,
				Reader: reader,
			},
			reqHeader: map[string]string{
				"X-PromQL-Max-Evaluated-Points": "200",
				"X-PromQL-Forced-Matcher":       "__account_id=1234",
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
				Index:  &idx,
				Reader: reader,
			},
			reqHeader: map[string]string{
				"X-PromQL-Max-Evaluated-Series": "2",
				"X-PromQL-Max-Evaluated-Points": "200",
				"X-PromQL-Forced-Matcher":       "__account_id=1234",
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
				Index:              &idx,
				Reader:             reader,
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			reqHeader: map[string]string{
				"X-PromQL-Max-Evaluated-Series": "5",
				"X-PromQL-Max-Evaluated-Points": "500",
				"X-PromQL-Forced-Matcher":       "__account_id=1234",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PromQL{
				Index:              tt.fields.Index,
				Reader:             tt.fields.Reader,
				MaxEvaluatedPoints: tt.fields.MaxEvaluatedPoints,
				MaxEvaluatedSeries: tt.fields.MaxEvaluatedSeries,
			}
			r := httptest.NewRequest("", "/", nil)

			for k, v := range tt.reqHeader {
				r.Header.Add(k, v)
			}

			queryable := p.queryable(r)

			queryier, err := queryable.Querier(context.Background(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}

			for i, query := range tt.searches {
				got := queryier.Select(false, nil, query.matchers...)
				count, err := countSeries(got)

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

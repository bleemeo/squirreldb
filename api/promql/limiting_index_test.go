package promql

import (
	"context"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

func Test_limitingIndex_Search(t *testing.T) {
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

	type fields struct {
		index          types.Index
		maxTotalSeries uint32
	}

	type search struct {
		matchers  []*labels.Matcher
		wantCount int
		wantErr   bool
	}

	tests := []struct {
		name     string
		fields   fields
		searches []search
	}{
		{
			name: "get-high-limit",
			fields: fields{
				index:          &idx,
				maxTotalSeries: 50000,
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
			name: "get-medium-limit",
			fields: fields{
				index:          &idx,
				maxTotalSeries: 5,
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
			name: "get-low-limit",
			fields: fields{
				index:          &idx,
				maxTotalSeries: 2,
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
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantErr: true,
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
			name: "get-very-low-limit",
			fields: fields{
				index:          &idx,
				maxTotalSeries: 1,
			},
			searches: []search{
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantErr: true,
				},
				{
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
					},
					wantErr: true,
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
			idx := limitingIndex{
				index:          tt.fields.index,
				maxTotalSeries: tt.fields.maxTotalSeries,
			}
			for i, query := range tt.searches {
				got, err := idx.Search(context.Background(), now, now, query.matchers)
				if (err != nil) != query.wantErr {
					t.Fatalf("limitingIndex.Search(#%d) error = %v, wantErr %v", i, err, query.wantErr)
					return
				}

				if got.Count() != query.wantCount {
					t.Errorf("limitingIndex.Search(#%d).Count() = %d, want %d", i, got.Count(), query.wantCount)
				}
			}
		})
	}
}

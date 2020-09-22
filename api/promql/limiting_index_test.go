package promql

import (
	"context"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
)

func Test_limitingIndex_Search(t *testing.T) {
	idx := dummy.Index{
		StoreMetricIDInMemory: true,
	}
	_, _, err := idx.LookupIDs(
		context.Background(),
		[]labels.Labels{
			labelsMetric1,
			labelsMetric2,
			labelsMetric3,
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
				got, err := idx.Search(query.matchers)
				if (err != nil) != query.wantErr {
					t.Errorf("limitingIndex.Search(#%d) error = %v, wantErr %v", i, err, query.wantErr)
					return
				}
				if len(got) != query.wantCount {
					t.Errorf("len(limitingIndex.Search(#%d)) = %d, want %d", i, len(got), query.wantCount)
				}
			}
		})
	}
}

package promql

import (
	"context"
	"sort"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

func Test_filteringIndex_Search(t *testing.T) {
	now := time.Now()
	idx := dummy.Index{
		StoreMetricIDInMemory: true,
	}
	ids, _, err := idx.LookupIDs(
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

	sortedLabels1 := labelsMetric1.Copy()
	sortedLabels2 := labelsMetric2.Copy()
	sortedLabels3 := labelsMetric3.Copy()

	sort.Sort(sortedLabels1)
	sort.Sort(sortedLabels2)
	sort.Sort(sortedLabels3)

	type fields struct {
		index   types.Index
		matcher *labels.Matcher
	}
	type args struct {
		matchers []*labels.Matcher
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    types.MetricsSet
		wantErr bool
	}{
		{
			name: "filter-account-id-1",
			fields: fields{
				index: &idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"1234",
				),
			},
			args: args{[]*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
			}},
			want: &dummy.MetricsLabel{
				List: []types.MetricLabel{{ID: ids[0], Labels: sortedLabels1}},
			},
		},
		{
			name: "filter-account-id-2",
			fields: fields{
				index: &idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"5678",
				),
			},
			args: args{[]*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
			}},
			want: &dummy.MetricsLabel{
				List: []types.MetricLabel{{ID: ids[1], Labels: sortedLabels2}},
			},
		},
		{
			name: "filter-account-id-absent",
			fields: fields{
				index: &idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"",
				),
			},
			args: args{[]*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_used"),
			}},
			want: &dummy.MetricsLabel{},
		},
		{
			name: "filter-name",
			fields: fields{
				index: &idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"disk_used",
				),
			},
			args: args{[]*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "mountpath", "/srv"),
			}},
			want: &dummy.MetricsLabel{
				List: []types.MetricLabel{{ID: ids[1], Labels: sortedLabels2}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := filteringIndex{
				index:   tt.fields.index,
				matcher: tt.fields.matcher,
			}
			got, err := idx.Search(now, now, tt.args.matchers)
			if (err != nil) != tt.wantErr {
				t.Errorf("filteringIndex.Search() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !dummy.MetricsSetEqual(got, tt.want) {
				t.Errorf("filteringIndex.Search() = %v, want %v", got, tt.want)
			}
		})
	}
}

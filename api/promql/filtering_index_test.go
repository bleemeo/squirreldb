package promql

import (
	"context"
	"reflect"
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
			got, err := idx.Search(context.Background(), now, now, tt.args.matchers)
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

func Test_filteringIndex_LabelValues(t *testing.T) {
	idx := dummy.NewIndex([]types.MetricLabel{
		{ID: metricID1, Labels: labelsMetric1},
		{ID: metricID2, Labels: labelsMetric2},
		{ID: metricID3, Labels: labelsMetric3},
	})
	now := time.Now()

	type fields struct {
		index   types.Index
		matcher *labels.Matcher
	}

	type args struct {
		name     string
		matchers []*labels.Matcher
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "filter-account-id-1",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"1234",
				),
			},
			args: args{
				name:     "__name__",
				matchers: nil,
			},
			want: []string{"disk_used"},
		},
		{
			name: "filter-account-id-2",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"5678",
				),
			},
			args: args{
				name:     "__name__",
				matchers: []*labels.Matcher{},
			},
			want: []string{"disk_free", "disk_used"},
		},
		{
			name: "filter-account-id-2-more-matcher",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"5678",
				),
			},
			args: args{
				name: "__name__",
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "mountpath", "/srv"),
				},
			},
			want: []string{"disk_free", "disk_used"},
		},
		{
			name: "filter-account-id-2-more-matcher-bis",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"5678",
				),
			},
			args: args{
				name: "__name__",
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_free"),
				},
			},
			want: []string{"disk_free"},
		},
		{
			name: "filter-account-id-2-label-mountpath",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"5678",
				),
			},
			args: args{
				name:     "mountpath",
				matchers: nil,
			},
			want: []string{"/srv"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := filteringIndex{
				index:   tt.fields.index,
				matcher: tt.fields.matcher,
			}
			got, err := idx.LabelValues(context.Background(), now, now, tt.args.name, tt.args.matchers)
			if (err != nil) != tt.wantErr {
				t.Errorf("filteringIndex.LabelValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filteringIndex.LabelValues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_filteringIndex_LabelNames(t *testing.T) {
	idx := dummy.NewIndex([]types.MetricLabel{
		{ID: metricID1, Labels: labelsMetric1},
		{ID: metricID2, Labels: labelsMetric2},
		{ID: metricID3, Labels: labelsMetric3},
	})
	now := time.Now()

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
		want    []string
		wantErr bool
	}{
		{
			name: "filter-account-id-1",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"1234",
				),
			},
			args: args{
				matchers: nil,
			},
			want: []string{"__account_id", "__name__", "mountpath"},
		},
		{
			name: "filter-account-id-2",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"5678",
				),
			},
			args: args{
				matchers: []*labels.Matcher{},
			},
			want: []string{"__account_id", "__name__", "mountpath"},
		},
		{
			name: "filter-account-id-2-more-matcher",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"5678",
				),
			},
			args: args{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "mountpath", "/srv"),
				},
			},
			want: []string{"__account_id", "__name__", "mountpath"},
		},
		{
			name: "filter-account-id-2-more-matcher-bis",
			fields: fields{
				index: idx,
				matcher: labels.MustNewMatcher(
					labels.MatchEqual,
					"__account_id",
					"5678",
				),
			},
			args: args{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "disk_free"),
				},
			},
			want: []string{"__account_id", "__name__", "mountpath"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := filteringIndex{
				index:   tt.fields.index,
				matcher: tt.fields.matcher,
			}
			got, err := idx.LabelNames(context.Background(), now, now, tt.args.matchers)
			if (err != nil) != tt.wantErr {
				t.Errorf("filteringIndex.LabelNames() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filteringIndex.LabelNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

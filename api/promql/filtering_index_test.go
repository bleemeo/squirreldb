package promql

import (
	"context"
	"reflect"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
)

func Test_filteringIndex_Search(t *testing.T) {

	idx := dummy.Index{
		StoreMetricIDInMemory: true,
	}
	ids, _, err := idx.LookupIDs(
		context.Background(),
		[]labels.Labels{
			labelsMetric1,
			labelsMetric2,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

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
		want    []types.MetricID
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
			want: []types.MetricID{ids[0]},
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
			want: []types.MetricID{ids[1]},
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
			want: []types.MetricID{},
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
			want: []types.MetricID{ids[1]},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := filteringIndex{
				index:   tt.fields.index,
				matcher: tt.fields.matcher,
			}
			got, err := idx.Search(tt.args.matchers)
			if (err != nil) != tt.wantErr {
				t.Errorf("filteringIndex.Search() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filteringIndex.Search() = %v, want %v", got, tt.want)
			}
		})
	}
}

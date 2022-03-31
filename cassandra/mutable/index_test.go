package mutable_test

import (
	"context"
	"squirreldb/cassandra/mutable"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

func TestMutableIndex(t *testing.T) {
	t.Parallel()

	now := time.Now()

	metrics := []types.MetricLabel{
		{
			ID:     1,
			Labels: labels.FromStrings("__account_id", "1234", "__name__", "cpu_used", "instance", "server1"),
		},
		{
			ID:     2,
			Labels: labels.FromStrings("__account_id", "1234", "__name__", "cpu_used", "instance", "server2"),
		},
		{
			ID:     3,
			Labels: labels.FromStrings("__account_id", "1234", "__name__", "cpu_used", "instance", "server3"),
		},
		{
			ID:     4,
			Labels: labels.FromStrings("__account_id", "1234", "__name__", "cpu_used", "instance", "server4"),
		},
	}

	dummyIndex := dummy.NewIndex(metrics)
	store := dummy.NewMutableLabelStore(dummy.DefaultMutableLabels)
	provider := mutable.NewProvider(context.Background(), nil, &dummy.LocalCluster{}, store)
	labelProcessor := mutable.NewLabelProcessor(provider, "__account_id")
	idx := mutable.NewIndexWrapper(dummyIndex, labelProcessor)

	tests := []struct {
		want     types.MetricsSet
		name     string
		matchers []*labels.Matcher
	}{
		{
			name: "add-mutable-labels",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server4"),
			},
			want: &dummy.MetricsLabel{
				List: []types.MetricLabel{
					{
						ID: 4,
						Labels: labels.FromStrings(
							"__account_id", "1234", "__name__", "cpu_used", "instance", "server4",
							"group", "group3", "environment", "prod",
						),
					},
				},
			},
		},
		{
			name: "search-by-mutable-labels",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "group", "group3"),
			},
			want: &dummy.MetricsLabel{
				List: []types.MetricLabel{
					{
						ID: 4,
						Labels: labels.FromStrings(
							"__account_id", "1234", "__name__", "cpu_used", "instance", "server4",
							"group", "group3", "environment", "prod",
						),
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := idx.Search(context.Background(), now, now, test.matchers)
			if err != nil {
				t.Fatal(err)
			}

			if !dummy.MetricsSetEqual(got, test.want) {
				t.Errorf("mutableIndex.Search() = %v, want %v", got, test.want)
			}
		})
	}
}

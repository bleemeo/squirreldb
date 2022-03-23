package mutable_test

import (
	"reflect"
	"sort"
	"squirreldb/cassandra/mutable"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

type mockLabelProvider struct{}

func (lp mockLabelProvider) mutableLabels(tenant, name string) map[string]labels.Labels {
	type key struct {
		tenant string
		name   string
	}

	lbls := map[key]map[string]labels.Labels{
		key{
			tenant: "1234",
			name:   "group",
		}: {
			"group1": {
				{Name: "instance", Value: "server1"},
				{Name: "instance", Value: "server2"},
				{Name: "instance", Value: "server3"},
			},
			"group2": {
				{Name: "instance", Value: "server2"},
				{Name: "instance", Value: "server3"},
			},
			"group3": {
				{Name: "instance", Value: "server4"},
			},
		},
	}

	return lbls[key{tenant: tenant, name: name}]
}

func (lp mockLabelProvider) Get(tenant, name, value string) (labels.Labels, error) {
	ls := lp.mutableLabels(tenant, name)[value]

	return ls, nil
}

func (lp mockLabelProvider) AllValues(tenant, name string) ([]string, error) {
	ls := lp.mutableLabels(tenant, name)

	keys := make([]string, 0, len(ls))
	for k := range ls {
		keys = append(keys, k)
	}

	// Always return the keys in the same orders.
	sort.Strings(keys)

	return keys, nil
}

// isMutableLabel returns whether the label is mutable.
func (lp mockLabelProvider) IsMutableLabel(name string) bool {
	return name == "group"
}

// IsTenantLabel returns whether this label identifies the tenant.
func (lp mockLabelProvider) IsTenantLabel(name string) bool {
	return name == "__account_id"
}

func TestProcessMutableLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		matchers     []*labels.Matcher
		wantMatchers []*labels.Matcher
	}{
		{
			name: "equal",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "group", "group1"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server1|server2|server3"),
			},
		},
		{
			name: "not-equal",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchNotEqual, "group", "group1"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "server1|server2|server3"),
			},
		},
		{
			name: "regex",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchRegexp, "group", "group1|group3"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server1|server2|server3|server4"),
			},
		},
		{
			name: "not-regex",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "group", "group1|group3"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server2|server3"), // Matches group2.
			},
		},
		{
			name: "collision-with-non-mutable-labels",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "group", "group1"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-unknown"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server1|server2|server3"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-unknown"),
			},
		},
	}

	lp := mutable.NewLabelProcessor(mockLabelProvider{})

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotMatchers, err := lp.ProcessMutableLabels(test.matchers)
			if err != nil {
				t.Errorf("Failed to process labels: %v", err)
			}

			if !reflect.DeepEqual(test.wantMatchers, gotMatchers) {
				t.Errorf("ProcessMutableLabels() = %v, want %v", gotMatchers, test.wantMatchers)
			}
		})
	}
}

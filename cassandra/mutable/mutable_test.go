package mutable_test

import (
	"reflect"
	"sort"
	"squirreldb/cassandra/mutable"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

type mockLabelProvider struct{}

func (lp mockLabelProvider) mutableLabels() map[string]labels.Labels {
	return map[string]labels.Labels{
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
	}
}

func (lp mockLabelProvider) Get(name, value string) (labels.Labels, bool) {
	ls, ok := lp.mutableLabels()[value]

	return ls, ok
}

func (lp mockLabelProvider) AllValues() []string {
	ls := lp.mutableLabels()

	keys := make([]string, 0, len(ls))
	for k := range ls {
		keys = append(keys, k)
	}

	// Always return the keys in the same orders.
	sort.Strings(keys)

	return keys
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
				labels.MustNewMatcher(labels.MatchEqual, "group", "group1"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server1|server2|server3"),
			},
		},
		{
			name: "not-equal",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "group", "group1"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "server1|server2|server3"),
			},
		},
		{
			name: "regex",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "group", "group1|group3"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server1|server2|server3|server4"),
			},
		},
		{
			name: "not-regex",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "group", "group1|group3"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server2|server3"), // Matches group2.
			},
		},
		{
			name: "collision-with-non-mutable-labels",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "group", "group1"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-unknown"),
			},
			wantMatchers: []*labels.Matcher{
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

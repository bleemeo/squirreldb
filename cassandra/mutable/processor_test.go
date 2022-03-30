package mutable_test

import (
	"reflect"
	"sort"
	"squirreldb/cassandra/mutable"
	"squirreldb/dummy"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

func TestReplaceMutableLabels(t *testing.T) {
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
				labels.MustNewMatcher(labels.MatchEqual, "instance", "other-instance"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "server1|server2|server3"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "other-instance"),
			},
		},
		{
			name: "no-mutable-labels",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "job", "job1"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "other-instance"),
			},
			wantMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "job", "job1"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "other-instance"),
			},
		},
	}

	provider := dummy.NewMutableLabelProvider(dummy.DefaultMutableLabels)
	lp := mutable.NewLabelProcessor(provider, "__account_id")

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotMatchers, err := lp.ReplaceMutableLabels(test.matchers)
			if err != nil {
				t.Errorf("Failed to process labels: %v", err)
			}

			if !reflect.DeepEqual(test.wantMatchers, gotMatchers) {
				t.Errorf("ReplaceMutableLabels() = %v, want %v", gotMatchers, test.wantMatchers)
			}
		})
	}
}

func TestAddMutableLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		labels     labels.Labels
		wantLabels labels.Labels
	}{
		{
			name:   "match-two-groups",
			labels: labels.FromStrings("__account_id", "1234", "instance", "server2", "job", "job1"),
			// server2 is both in group1 and group2, but only group1 should be returned.
			wantLabels: labels.FromStrings(
				"__account_id", "1234", "instance", "server2", "job", "job1", "group", "group1",
			),
		},
		{
			name:   "multiple-mutable-labels",
			labels: labels.FromStrings("__account_id", "1234", "instance", "server4", "job", "job1"),
			wantLabels: labels.FromStrings(
				"__account_id", "1234", "instance", "server4", "job", "job1",
				"group", "group3", "environment", "prod",
			),
		},
	}

	provider := dummy.NewMutableLabelProvider(dummy.DefaultMutableLabels)
	lp := mutable.NewLabelProcessor(provider, "__account_id")

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotLabels, err := lp.AddMutableLabels(test.labels)
			if err != nil {
				t.Errorf("Failed to process labels: %v", err)
			}

			sort.Sort(gotLabels)

			if !reflect.DeepEqual(test.wantLabels, gotLabels) {
				t.Errorf("AddMutableLabels() = %v, want %v", gotLabels, test.wantLabels)
			}
		})
	}
}

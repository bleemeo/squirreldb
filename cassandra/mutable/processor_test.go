package mutable_test

import (
	"context"
	"reflect"
	"sort"
	"squirreldb/cassandra/mutable"
	"squirreldb/dummy"
	"squirreldb/logger"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

	store := dummy.NewMutableLabelStore(dummy.DefaultMutableLabels)
	provider := mutable.NewProvider(context.Background(), nil, &dummy.LocalCluster{}, store, logger.NewTestLogger(true))
	lp := mutable.NewLabelProcessor(provider, "__account_id")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotMatchers, err := lp.ReplaceMutableLabels(context.Background(), test.matchers)
			if err != nil {
				t.Errorf("Failed to process labels: %v", err)
			}

			allowUnexp := cmp.AllowUnexported(labels.Matcher{}, labels.FastRegexMatcher{})
			ignoreFields := cmpopts.IgnoreFields(labels.FastRegexMatcher{}, "matchString") // functions are not comparable
			ignoreIfaces := cmpopts.IgnoreInterfaces(struct{ labels.StringMatcher }{})

			if diff := cmp.Diff(test.wantMatchers, gotMatchers, allowUnexp, ignoreFields, ignoreIfaces); diff != "" {
				t.Errorf("ReplaceMutableLabels() = %v, want %v\n%s", gotMatchers, test.wantMatchers, diff)
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
		{
			name:   "remove-input-mutable-label",
			labels: labels.FromStrings("__account_id", "1234", "instance", "server1", "group", "group-to-remove"),
			wantLabels: labels.FromStrings(
				"__account_id", "1234", "instance", "server1", "group", "group1",
			),
		},
	}

	store := dummy.NewMutableLabelStore(dummy.DefaultMutableLabels)
	provider := mutable.NewProvider(context.Background(), nil, &dummy.LocalCluster{}, store, logger.NewTestLogger(true))
	lp := mutable.NewLabelProcessor(provider, "__account_id")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotLabels, err := lp.AddMutableLabels(context.Background(), test.labels)
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

func TestMergeRegex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		input          []string
		shouldNotMatch []string
	}{
		{
			name:           "one-string",
			input:          []string{"web-1"},
			shouldNotMatch: []string{"a", "web-0", "web1"},
		},
		{
			name:           "windows-path",
			input:          []string{`C:\\Users\\Dummy\\Documents`, `C:\\Users\\Dummy\\Videos`},
			shouldNotMatch: []string{"a", `C:\Users\Dummy\Downloads`, `D:\Users\Dummy\Videos`},
		},
		{
			name:           "dot",
			input:          []string{`./file1`, `../../file2`},
			shouldNotMatch: []string{"a", "a/file1", "aa/aa/file2"},
		},
		{
			name:           "special-chars",
			input:          []string{`.*[\a^$`, `^$*.]`},
			shouldNotMatch: []string{"a", "a/file1", "aa/aa/file2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			merged, err := mutable.MergeRegex(test.input)
			if err != nil {
				t.Errorf("Failed to merge regex %v: %v", test.input, err)
			}

			matcher, err := labels.NewMatcher(labels.MatchRegexp, test.name, merged)
			if err != nil {
				t.Errorf("Failed to create matcher: %v", err)
			}

			for _, shouldMatch := range test.input {
				if !matcher.Matches(shouldMatch) {
					t.Errorf("%v doesn't match %v", shouldMatch, merged)
				}
			}

			for _, shouldNotMatch := range test.shouldNotMatch {
				if matcher.Matches(shouldNotMatch) {
					t.Errorf("%v matches when it shouldn't %v", shouldNotMatch, merged)
				}
			}
		})
	}
}

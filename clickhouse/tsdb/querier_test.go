// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/labels"
)

func TestMatcherCondition(t *testing.T) {
	lq := &labelQuerier{tenantLabel: "__bleemeo_account__"}

	cases := []struct {
		name     string
		matcher  *labels.Matcher
		wantCond string
		wantArgs []any
	}{
		{
			name:     "equal on __name__",
			matcher:  labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "cpu_used"),
			wantCond: "metric_name = ?",
			wantArgs: []any{"cpu_used"},
		},
		{
			name:     "equal on tenant",
			matcher:  labels.MustNewMatcher(labels.MatchEqual, "__bleemeo_account__", "tenant-1"),
			wantCond: "tenant = ?",
			wantArgs: []any{"tenant-1"},
		},
		{
			name:     "not-equal on tag",
			matcher:  labels.MustNewMatcher(labels.MatchNotEqual, "item", "/"),
			wantCond: "tags[?] != ?",
			wantArgs: []any{"item", "/"},
		},
		{
			name:     "regexp on tag is anchored",
			matcher:  labels.MustNewMatcher(labels.MatchRegexp, "instance", "srv.+"),
			wantCond: "match(tags[?], ?)",
			wantArgs: []any{"instance", "^(?:srv.+)$"},
		},
		{
			name:     "not-regexp on __name__ is anchored",
			matcher:  labels.MustNewMatcher(labels.MatchNotRegexp, labels.MetricName, "node_.*"),
			wantCond: "NOT match(metric_name, ?)",
			wantArgs: []any{"^(?:node_.*)$"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cond, args := lq.matcherCondition(tc.matcher)

			if cond != tc.wantCond {
				t.Errorf("condition = %q, want %q", cond, tc.wantCond)
			}

			if diff := cmp.Diff(tc.wantArgs, args); diff != "" {
				t.Errorf("args mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildWhere(t *testing.T) {
	lq := &labelQuerier{tenantLabel: "__bleemeo_account__"}

	t.Run("no matcher is always true", func(t *testing.T) {
		where, args := lq.buildWhere(nil)
		if where != "1" || args != nil {
			t.Errorf("got (%q, %v), want (\"1\", nil)", where, args)
		}
	})

	t.Run("matchers are ANDed with ordered args", func(t *testing.T) {
		where, args := lq.buildWhere([]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "cpu_used"),
			labels.MustNewMatcher(labels.MatchRegexp, "item", "/.*"),
		})

		wantWhere := "metric_name = ? AND match(tags[?], ?)"
		if where != wantWhere {
			t.Errorf("where = %q, want %q", where, wantWhere)
		}

		// Arg order must follow the order of the ? in the string.
		if diff := cmp.Diff([]any{"cpu_used", "item", "^(?:/.*)$"}, args); diff != "" {
			t.Errorf("args mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestBuildLabels(t *testing.T) {
	lq := &labelQuerier{tenantLabel: "__bleemeo_account__"}

	t.Run("reconstructs the full label set", func(t *testing.T) {
		got := lq.buildLabels("cpu_used", "tenant-1", map[string]string{"item": "/", "job": "node"})

		want := labels.FromStrings(
			"__name__", "cpu_used",
			"__bleemeo_account__", "tenant-1",
			"item", "/",
			"job", "node",
		)

		if !labels.Equal(want, got) {
			t.Errorf("buildLabels = %s, want %s", got, want)
		}

		// The reconstructed hash must match a direct construction (needed for the
		// point-(a) batcher probe later).
		if want.Hash() != got.Hash() {
			t.Errorf("hash mismatch: got %d, want %d", got.Hash(), want.Hash())
		}
	})

	t.Run("omits the tenant label when empty", func(t *testing.T) {
		got := lq.buildLabels("up", "", nil)
		want := labels.FromStrings("__name__", "up")

		if !labels.Equal(want, got) {
			t.Errorf("buildLabels = %s, want %s", got, want)
		}
	})
}

func TestQuoteString(t *testing.T) {
	cases := map[string]string{
		"__bleemeo_account__": "'__bleemeo_account__'",
		"a'b":                 `'a\'b'`,
		`a\b`:                 `'a\\b'`,
	}

	for in, want := range cases {
		if got := quoteString(in); got != want {
			t.Errorf("quoteString(%q) = %q, want %q", in, got, want)
		}
	}
}

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

package mutable_test

import (
	"slices"
	"testing"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

// newMutableQueryable returns a MemoryTSDB pre-filled with four server metrics,
// wrapped so mutable labels work on read.
func newMutableQueryable(t *testing.T) types.TSDB {
	t.Helper()

	db := &dummy.MemoryTSDB{}

	for _, instance := range []string{"server1", "server2", "server3", "server4"} {
		db.AddData([]types.Timeseries{
			{
				Labels: labels.FromStrings(
					"__account_id", "1234", "__name__", "cpu_used", "instance", instance,
				),
				Points: []types.MetricPoint{{Timestamp: 1000, Value: 42}},
			},
		}, 0)
	}

	store := dummy.NewMutableLabelStore(dummy.DefaultMutableLabels)
	provider := mutable.NewProvider(t.Context(), nil, &dummy.LocalCluster{}, store, logger.NewTestLogger(true))
	processor := mutable.NewLabelProcessor(provider, "__account_id")

	return mutable.NewQueryableWrapper(db, processor, logger.NewTestLogger(true))
}

// selectLabels runs Select and returns the label set of every matching series.
func selectLabels(t *testing.T, q types.TSDB, matchers ...*labels.Matcher) []labels.Labels {
	t.Helper()

	querier, err := q.QuerierWithOptions(types.QuerierOptions{}, 0, 2000)
	if err != nil {
		t.Fatal(err)
	}

	defer querier.Close()

	set := querier.Select(t.Context(), true, nil, matchers...)

	var got []labels.Labels

	for set.Next() {
		got = append(got, set.At().Labels())
	}

	if err := set.Err(); err != nil {
		t.Fatal(err)
	}

	return got
}

func TestQueryableWrapperSelect(t *testing.T) {
	t.Parallel()

	q := newMutableQueryable(t)

	server4 := labels.FromStrings(
		"__account_id", "1234", "__name__", "cpu_used", "instance", "server4",
		"group", "group3", "environment", "prod",
	)

	tests := []struct {
		name     string
		matchers []*labels.Matcher
		want     []labels.Labels
	}{
		{
			// A query on a mutable label resolves to the underlying series, with the
			// mutable labels re-added — the read path the ClickHouse backend was missing.
			name: "search-by-mutable-label",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "group", "group3"),
			},
			want: []labels.Labels{server4},
		},
		{
			// A query on the raw label still gets the mutable labels added to the result.
			name: "add-mutable-labels",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "server4"),
			},
			want: []labels.Labels{server4},
		},
		{
			// A mutable matcher with no match resolves to an empty result.
			name: "mutable-label-no-match",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234"),
				labels.MustNewMatcher(labels.MatchEqual, "group", "does-not-exist"),
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := selectLabels(t, q, test.matchers...)

			if len(got) != len(test.want) {
				t.Fatalf("Select() returned %d series, want %d: %v", len(got), len(test.want), got)
			}

			for i := range got {
				if labels.Compare(got[i], test.want[i]) != 0 {
					t.Errorf("Select() series[%d] = %v, want %v", i, got[i], test.want[i])
				}
			}
		})
	}
}

func TestQueryableWrapperLabels(t *testing.T) {
	t.Parallel()

	q := newMutableQueryable(t)

	querier, err := q.QuerierWithOptions(types.QuerierOptions{}, 0, 2000)
	if err != nil {
		t.Fatal(err)
	}

	defer querier.Close()

	tenantMatcher := labels.MustNewMatcher(labels.MatchEqual, "__account_id", "1234")

	// LabelValues on a mutable name returns its values from the provider.
	values, _, err := querier.LabelValues(t.Context(), "group", nil, tenantMatcher)
	if err != nil {
		t.Fatal(err)
	}

	wantValues := []string{"group1", "group2", "group3"}
	if len(values) != len(wantValues) {
		t.Fatalf("LabelValues(group) = %v, want %v", values, wantValues)
	}

	for i := range values {
		if values[i] != wantValues[i] {
			t.Fatalf("LabelValues(group) = %v, want %v", values, wantValues)
		}
	}

	// LabelNames includes the mutable label names.
	names, _, err := querier.LabelNames(t.Context(), nil, tenantMatcher)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"group", "environment"} {
		if !slices.Contains(names, want) {
			t.Errorf("LabelNames() = %v, missing mutable label %q", names, want)
		}
	}
}

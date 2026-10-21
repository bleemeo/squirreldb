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

package source

import (
	"context"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const tenantLabelName = "__account_id"

// constantSeries returns a series with a point every 10s whose value is its
// timestamp in seconds plus offset.
func constantSeries(offset float64, lbls ...string) Series {
	return Series{
		Labels: labels.FromStrings(lbls...),
		Step:   10000,
		Value: func(_ context.Context, ts int64) float64 {
			return float64(ts)/1000 + offset
		},
	}
}

type selectedSeries struct {
	Labels string
	Points [][2]float64
}

func selectAll(t *testing.T, q storage.Querier, sortSeries bool, matchers ...*labels.Matcher) []selectedSeries {
	t.Helper()

	set := q.Select(t.Context(), sortSeries, nil, matchers...)

	var result []selectedSeries

	for set.Next() {
		s := set.At()
		it := s.Iterator(nil)
		row := selectedSeries{Labels: s.Labels().String()}

		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			row.Points = append(row.Points, [2]float64{float64(ts), v})
		}

		result = append(result, row)
	}

	if err := set.Err(); err != nil {
		t.Fatal(err)
	}

	return result
}

func testSeries() []Series {
	file := []Series{
		constantSeries(0, "__name__", "cpu_used", "instance", "server1"),
		constantSeries(0, "__name__", "cpu_used", "instance", "server2"),
		constantSeries(0, "__name__", "mem_used", "instance", "server1"),
		constantSeries(0, "__name__", "expiry", "instance", "server1"),
	}
	promql := []Series{
		constantSeries(1000, "__name__", "expiry", "instance", "server1"),
		constantSeries(0, "__name__", "promql_only", "instance", "server3"),
	}

	return Merge(promql, file)
}

func TestMerge(t *testing.T) {
	t.Parallel()

	series := testSeries()

	got := make([]string, len(series))
	for i, s := range series {
		got[i] = s.Labels.String()
	}

	want := []string{
		`{__name__="cpu_used", instance="server1"}`,
		`{__name__="cpu_used", instance="server2"}`,
		`{__name__="expiry", instance="server1"}`,
		`{__name__="mem_used", instance="server1"}`,
		`{__name__="promql_only", instance="server3"}`,
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("merged series mismatch (-want +got):\n%s", diff)
	}

	// The first list (the PromQL source) takes precedence.
	if v := series[2].Value(t.Context(), 0); v != 1000 {
		t.Errorf("expiry value = %v, want 1000 (from the first list)", v)
	}
}

func TestQuerierWithoutTenant(t *testing.T) {
	t.Parallel()

	q := NewQuerier(testSeries(), 5000, 30000, QuerierOptions{})

	got := selectAll(t, q, true, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "cpu_used")) //nolint:staticcheck,lll
	want := []selectedSeries{
		{
			Labels: `{__name__="cpu_used", instance="server1"}`,
			Points: [][2]float64{{10000, 10}, {20000, 20}, {30000, 30}},
		},
		{
			Labels: `{__name__="cpu_used", instance="server2"}`,
			Points: [][2]float64{{10000, 10}, {20000, 20}, {30000, 30}},
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Select mismatch (-want +got):\n%s", diff)
	}

	// Series don't have a tenant label: a tenant matcher matches nothing.
	got = selectAll(t, q, true, labels.MustNewMatcher(labels.MatchEqual, tenantLabelName, "1234"))
	if len(got) != 0 {
		t.Errorf("Select with tenant matcher returned %d series, want 0", len(got))
	}

	names, _, err := q.LabelNames(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff([]string{"__name__", "instance"}, names); diff != "" {
		t.Errorf("LabelNames mismatch (-want +got):\n%s", diff)
	}
}

func TestQuerierWithTenant(t *testing.T) {
	t.Parallel()

	q := NewQuerier(testSeries(), 5000, 30000, QuerierOptions{
		TenantLabel: labels.Label{Name: tenantLabelName, Value: "1234"},
	})

	tenantMatcher := labels.MustNewMatcher(labels.MatchEqual, tenantLabelName, "1234")

	got := selectAll(
		t,
		q,
		true,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "cpu_used"), //nolint:staticcheck
		labels.MustNewMatcher(labels.MatchEqual, "instance", "server2"),
		tenantMatcher,
	)
	want := []selectedSeries{
		{
			Labels: `{__account_id="1234", __name__="cpu_used", instance="server2"}`,
			Points: [][2]float64{{10000, 10}, {20000, 20}, {30000, 30}},
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Select mismatch (-want +got):\n%s", diff)
	}

	// Another tenant value doesn't match.
	got = selectAll(t, q, true, labels.MustNewMatcher(labels.MatchEqual, tenantLabelName, "other"))
	if len(got) != 0 {
		t.Errorf("Select with another tenant returned %d series, want 0", len(got))
	}

	names, _, err := q.LabelNames(t.Context(), nil, tenantMatcher)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff([]string{"__account_id", "__name__", "instance"}, names); diff != "" {
		t.Errorf("LabelNames mismatch (-want +got):\n%s", diff)
	}

	values, _, err := q.LabelValues(t.Context(), tenantLabelName, nil, tenantMatcher)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff([]string{"1234"}, values); diff != "" {
		t.Errorf("LabelValues(tenant) mismatch (-want +got):\n%s", diff)
	}

	values, _, err = q.LabelValues(t.Context(), labels.MetricName, nil, tenantMatcher) //nolint:staticcheck
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff([]string{"cpu_used", "expiry", "mem_used", "promql_only"}, values); diff != "" {
		t.Errorf("LabelValues(__name__) mismatch (-want +got):\n%s", diff)
	}
}

// TestQuerierTenantSort checks that series are still sorted once the tenant
// label is added, which may change their order.
func TestQuerierTenantSort(t *testing.T) {
	t.Parallel()

	series := Merge([]Series{
		constantSeries(0, "__name__", "up"),
		constantSeries(0, "__name__", "up", "a", "1"),
	})

	q := NewQuerier(series, 0, 10000, QuerierOptions{
		TenantLabel: labels.Label{Name: "tenant", Value: "1"},
	})

	got := selectAll(t, q, true, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")) //nolint:staticcheck
	if len(got) != 2 {
		t.Fatalf("got %d series, want 2", len(got))
	}

	want := []string{`{__name__="up", a="1", tenant="1"}`, `{__name__="up", tenant="1"}`}

	for i, s := range got {
		if s.Labels != want[i] {
			t.Errorf("series %d = %s, want %s", i, s.Labels, want[i])
		}
	}
}

// TestLabelValuesDeduplicated checks that values shared by several series (and
// by both sources) are returned once, sorted.
func TestLabelValuesDeduplicated(t *testing.T) {
	t.Parallel()

	q := NewQuerier(testSeries(), 0, 10000, QuerierOptions{})

	values, _, err := q.LabelValues(t.Context(), "instance", nil)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff([]string{"server1", "server2", "server3"}, values); diff != "" {
		t.Errorf("LabelValues mismatch (-want +got):\n%s", diff)
	}

	values, _, err = q.LabelValues(t.Context(), "instance", &storage.LabelHints{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff([]string{"server1", "server2"}, values); diff != "" {
		t.Errorf("LabelValues with limit mismatch (-want +got):\n%s", diff)
	}
}

// TestQuerierStats checks that the statistics given to OnClose are those of
// the querier only, not a cumulative total.
func TestQuerierStats(t *testing.T) {
	t.Parallel()

	type stats struct{ points, series uint64 }

	var got []stats

	opts := QuerierOptions{
		OnClose: func(points uint64, series uint64) {
			got = append(got, stats{points, series})
		},
	}

	for range 2 {
		q := NewQuerier(testSeries(), 5000, 30000, opts)

		selectAll(t, q, false, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "cpu_used")) //nolint:staticcheck,lll

		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
	}

	want := []stats{{6, 2}, {6, 2}}

	if diff := cmp.Diff(want, got, cmp.AllowUnexported(stats{})); diff != "" {
		t.Errorf("stats mismatch (-want +got):\n%s", diff)
	}
}

func TestIterator(t *testing.T) {
	t.Parallel()

	s := constantSeries(0, "__name__", "up")

	it := newIterator(t.Context(), s, 7000, 45000, nil)

	if it.Seek(3000) != chunkenc.ValFloat || it.AtT() != 10000 {
		t.Fatalf("Seek(3s) went to %d, want 10000 (first point after mint)", it.AtT())
	}

	// Seek never moves backward.
	if it.Seek(0) != chunkenc.ValFloat || it.AtT() != 10000 {
		t.Fatalf("Seek(0) went to %d, want 10000", it.AtT())
	}

	if it.Seek(21000) != chunkenc.ValFloat || it.AtT() != 30000 {
		t.Fatalf("Seek(21s) went to %d, want 30000", it.AtT())
	}

	if it.Next() != chunkenc.ValFloat || it.AtT() != 40000 {
		t.Fatalf("Next went to %d, want 40000", it.AtT())
	}

	if it.Next() != chunkenc.ValNone {
		t.Fatal("Next after maxt should return ValNone")
	}

	if it.Next() != chunkenc.ValNone || it.Seek(100000) != chunkenc.ValNone {
		t.Fatal("an exhausted iterator should stay exhausted")
	}

	it = newIterator(t.Context(), s, 41000, 45000, nil)
	if it.Next() != chunkenc.ValNone {
		t.Fatal("no point between 41s and 45s, Next should return ValNone")
	}
}

func TestAlignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		t, step, want int64
	}{
		{0, 10, 0},
		{1, 10, 10},
		{10, 10, 10},
		{11, 10, 20},
		{-1, 10, 0},
		{-10, 10, -10},
		{-11, 10, -10},
		{math.MaxInt64 - 1, 10, math.MaxInt64},
		{math.MinInt64, 10, math.MinInt64 + 8},
	}

	for _, tt := range tests {
		if got := alignUp(tt.t, tt.step); got != tt.want {
			t.Errorf("alignUp(%d, %d) = %d, want %d", tt.t, tt.step, got, tt.want)
		}
	}
}

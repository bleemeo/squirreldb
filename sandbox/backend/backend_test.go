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

package backend

import (
	"context"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/rs/zerolog"
)

const tenantLabel = "__account_id"

func startBackend(t *testing.T, reg prometheus.Registerer) *Backend {
	t.Helper()

	ctx := t.Context()

	b, err := New(ctx, zerolog.Nop(), Options{
		MetricRegistry: reg,
		Cluster:        &dummy.LocalCluster{},
		TenantLabel:    tenantLabel,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() { b.CloseAll(context.Background()) })

	return b
}

type result struct {
	labels labels.Labels
	points int
}

func query(t *testing.T, tsdb types.TSDB, tenant string, matchers ...*labels.Matcher) []result {
	t.Helper()

	maxt := time.Now().UnixMilli()
	mint := maxt - time.Hour.Milliseconds()

	q, err := tsdb.QuerierWithOptions(types.QuerierOptions{Tenant: tenant}, mint, maxt)
	if err != nil {
		t.Fatalf("QuerierWithOptions: %v", err)
	}

	defer q.Close()

	var results []result

	set := q.Select(t.Context(), true, nil, matchers...)
	for set.Next() {
		r := result{labels: set.At().Labels()}

		it := set.At().Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			r.points++
		}

		results = append(results, r)
	}

	if err := set.Err(); err != nil {
		t.Fatal(err)
	}

	return results
}

func TestReadEmbeddedDataset(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	b := startBackend(t, reg)

	tsdb, err := b.TSDB(t.Context())
	if err != nil {
		t.Fatalf("TSDB: %v", err)
	}

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "agent_status")
	tenantMatcher := labels.MustNewMatcher(labels.MatchEqual, tenantLabel, "1234")

	// Without tenant, series are returned as they are in the dataset.
	results := query(t, tsdb, "", nameMatcher)
	if len(results) == 0 {
		t.Fatal("agent_status not found in the embedded dataset")
	}

	for _, r := range results {
		if r.labels.Has(tenantLabel) {
			t.Errorf("series %s has a tenant label, want none", r.labels)
		}

		// One point every 10s over one hour.
		if r.points < 359 || r.points > 361 {
			t.Errorf("series %s has %d points, want ~360", r.labels, r.points)
		}
	}

	// A tenant matcher matches nothing without tenant.
	if got := query(t, tsdb, "", nameMatcher, tenantMatcher); len(got) != 0 {
		t.Errorf("got %d series with a tenant matcher but no tenant, want 0", len(got))
	}

	// With a tenant, the same series are returned with the tenant label.
	withTenant := query(t, tsdb, "1234", nameMatcher, tenantMatcher)
	if len(withTenant) != len(results) {
		t.Fatalf("got %d series with tenant, want %d", len(withTenant), len(results))
	}

	for _, r := range withTenant {
		if r.labels.Get(tenantLabel) != "1234" {
			t.Errorf("series %s doesn't have the tenant label", r.labels)
		}
	}

	// The PromQL series of the dataset are served too.
	expiry := query(t, tsdb, "", labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "probe_domain_expiry"))
	if len(expiry) != 1 {
		t.Errorf("got %d probe_domain_expiry series, want 1", len(expiry))
	}

	// Each of the 4 queriers is observed once, with its own series (not a
	// cumulative total).
	wantSeries := float64(len(results) + len(withTenant) + len(expiry))

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	found := false

	for _, mf := range families {
		if mf.GetName() != "squirreldb_sandbox_requests_series" {
			continue
		}

		found = true

		h := mf.GetMetric()[0].GetHistogram()
		if h.GetSampleCount() != 4 || h.GetSampleSum() != wantSeries {
			t.Errorf("requests_series: count=%d sum=%v, want count=4 sum=%v",
				h.GetSampleCount(), h.GetSampleSum(), wantSeries)
		}
	}

	if !found {
		t.Error("squirreldb_sandbox_requests_series not registered")
	}

	if got := testutil.CollectAndCount(reg, "squirreldb_sandbox_requests_points"); got != 1 {
		t.Errorf("got %d requests_points metrics, want 1", got)
	}
}

func TestPromQLSeriesValue(t *testing.T) {
	t.Parallel()

	b := startBackend(t, prometheus.NewRegistry())

	tsdb, err := b.TSDB(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

	q, err := tsdb.QuerierWithOptions(types.QuerierOptions{}, ts, ts)
	if err != nil {
		t.Fatal(err)
	}

	defer q.Close()

	set := q.Select(
		t.Context(),
		false,
		nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "probe_domain_expiry"),
	)
	if !set.Next() {
		t.Fatal("probe_domain_expiry not found")
	}

	it := set.At().Iterator(nil)
	if it.Next() != chunkenc.ValFloat {
		t.Fatal("no point")
	}

	// The default dataset defines it as "time() + 86400*30".
	gotTS, value := it.At()
	if want := float64(ts/1000 + 86400*30); gotTS != ts || value != want {
		t.Errorf("got (%d, %v), want (%d, %v)", gotTS, value, ts, want)
	}
}

func TestDiscardWrites(t *testing.T) {
	t.Parallel()

	b := startBackend(t, prometheus.NewRegistry())

	tsdb, err := b.TSDB(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	app := tsdb.AppenderWithOptions(t.Context(), types.AppenderOptions{WriteTTL: 86400})

	lbls := labels.FromStrings(labels.MetricName, "written_metric", tenantLabel, "1234")
	if _, err := app.Append(0, lbls, time.Now().UnixMilli(), 42); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got := query(t, tsdb, "1234", labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "written_metric"))
	if len(got) != 0 {
		t.Errorf("got %d written series, want 0: writes must be discarded", len(got))
	}
}

func TestInvalidDataset(t *testing.T) {
	t.Parallel()

	b, err := New(t.Context(), zerolog.Nop(), Options{
		MetricRegistry: prometheus.NewRegistry(),
		DatasetConfig:  "/does/not/exist.yaml",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Start(t.Context()); err == nil {
		t.Fatal("Start should fail with a missing dataset")
	}

	if _, err := b.TSDB(t.Context()); err == nil {
		t.Fatal("TSDB should fail when the backend isn't started")
	}
}

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
	"context"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/batch"
	"github.com/bleemeo/squirreldb/dummy/temporarystore"
	"github.com/bleemeo/squirreldb/types"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

const tenantLabelForTest = "__acme_account__"

func newTestTSDB(t *testing.T) *ClickHouseTSDB {
	t.Helper()

	reg := prometheus.NewRegistry()
	store := temporarystore.New(reg, zerolog.Nop())

	return &ClickHouseTSDB{
		temporaryStore: store,
		batcher:        batch.New(reg, 15*time.Minute, store, zerolog.Nop()),
		labelsTTL:      time.Hour,
		metrics:        newMetrics(reg),
		logger:         zerolog.Nop(),
		seenSeries:     make(map[uint64]seenEntry),
	}
}

// TestAppenderCommit checks that appending then committing stores both the
// labels (for later rebuild) and the points in the temporary store.
func TestAppenderCommit(t *testing.T) {
	ctx := context.Background()
	c := newTestTSDB(t)

	lbls := labels.FromStrings("__name__", "cpu_used", tenantLabelForTest, "tenant-1", "item", "/")
	id := types.MetricID(lbls.Hash())

	// Pre-register the series (nextRegister in the future) so Commit's registration
	// skips its SQL INSERT (this test has no ClickHouse connection); it validates
	// the temporary store effects of Commit. Registration is covered by its own test.
	c.seenSeries[lbls.Hash()] = seenEntry{lastSeen: time.Now(), nextRegister: time.Now().Add(time.Hour)}

	app := c.AppenderWithOptions(ctx, types.AppenderOptions{WriteTTL: 86400})

	if _, err := app.Append(0, lbls, 1000, 1.5); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := app.Append(0, lbls, 2000, 2.5); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotLabels, err := c.temporaryStore.GetLabelsByID(ctx, []types.MetricID{id})
	if err != nil {
		t.Fatalf("GetLabelsByID: %v", err)
	}

	if diff := cmp.Diff(lbls.String(), gotLabels[0].String()); diff != "" {
		t.Errorf("stored labels mismatch (-want +got):\n%s", diff)
	}

	data, _, err := c.temporaryStore.ReadPointsAndOffset(ctx, []types.MetricID{id})
	if err != nil {
		t.Fatalf("ReadPointsAndOffset: %v", err)
	}

	wantPoints := []types.MetricPoint{{Timestamp: 1000, Value: 1.5}, {Timestamp: 2000, Value: 2.5}}
	if diff := cmp.Diff(wantPoints, data[0].Points); diff != "" {
		t.Errorf("stored points mismatch (-want +got):\n%s", diff)
	}
}

func TestSeriesToInput(t *testing.T) {
	lbls := labels.FromStrings("__name__", "cpu_used", tenantLabelForTest, "tenant-1", "item", "/")

	t.Run("splits tenant, sorts tags, per-metric TTL", func(t *testing.T) {
		cols := seriesToInput(lbls, tenantLabelForTest, 7*86400, 365)

		if cols.metricName != "cpu_used" {
			t.Errorf("metric name = %q, want cpu_used", cols.metricName)
		}

		if cols.tenant != "tenant-1" {
			t.Errorf("tenant = %q, want tenant-1", cols.tenant)
		}

		// __name__ and the tenant label are excluded; the remaining keys must be
		// in sorted order so the id ClickHouse computes is stable across flushes.
		if diff := cmp.Diff([]string{"item"}, cols.tags.keys); diff != "" {
			t.Errorf("tags keys mismatch (-want +got):\n%s", diff)
		}

		if diff := cmp.Diff([]string{"/"}, cols.tags.values); diff != "" {
			t.Errorf("tags values mismatch (-want +got):\n%s", diff)
		}

		if cols.rowTTLDays != 7 {
			t.Errorf("row_ttl_days = %d, want 7", cols.rowTTLDays)
		}
	})

	t.Run("falls back to global default when TTL is zero or sub-day", func(t *testing.T) {
		if cols := seriesToInput(lbls, tenantLabelForTest, 0, 365); cols.rowTTLDays != 365 {
			t.Errorf("row_ttl_days = %d, want 365 (global default)", cols.rowTTLDays)
		}

		if cols := seriesToInput(lbls, tenantLabelForTest, 3600, 365); cols.rowTTLDays != 365 {
			t.Errorf("row_ttl_days = %d, want 365 (sub-day TTL falls back)", cols.rowTTLDays)
		}
	})
}

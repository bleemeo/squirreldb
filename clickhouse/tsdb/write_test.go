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
	"errors"
	"math"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

const tenantLabelForTest = "__acme_account__"

// newTestTSDB builds a minimal ClickHouseTSDB for the appender's local checks
// (Append does not touch the WAL/ClickHouse; only Commit does, which needs Redis
// and is covered by integration tests).
func newTestTSDB(t *testing.T) *ClickHouseTSDB {
	t.Helper()

	return &ClickHouseTSDB{
		metrics:    newMetrics(nil),
		logger:     zerolog.Nop(),
		seenSeries: make(map[string]seenEntry),
	}
}

// TestAppenderRejectsOutOfRangeTimestamp checks that Append rejects (does not
// clamp) timestamps outside the storable DateTime64(3) range, and accepts the
// boundaries and in-range values.
func TestAppenderRejectsOutOfRangeTimestamp(t *testing.T) {
	ctx := context.Background()
	lbls := labels.FromStrings("__name__", "cpu_used", tenantLabelForTest, "tenant-1")

	year3000 := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

	cases := []struct {
		name    string
		ts      int64
		wantErr bool
	}{
		{"now", time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC).UnixMilli(), false},
		{"at min boundary", storeMinTimeMillis, false},
		{"at max boundary", storeMaxTimeMillis, false},
		{"just below min", storeMinTimeMillis - 1, true},
		{"just above max", storeMaxTimeMillis + 1, true},
		{"year 3000", year3000, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestTSDB(t)
			app := c.AppenderWithOptions(ctx, types.AppenderOptions{WriteTTL: 86400})

			_, err := app.Append(0, lbls, tc.ts, 1.0)

			switch {
			case tc.wantErr && err == nil:
				t.Fatalf("Append(ts=%d) = nil, want error", tc.ts)
			case tc.wantErr && !errors.Is(err, errTimestampOutOfRange):
				t.Fatalf("Append(ts=%d) error = %v, want errTimestampOutOfRange", tc.ts, err)
			case !tc.wantErr && err != nil:
				t.Fatalf("Append(ts=%d) = %v, want nil", tc.ts, err)
			}
		})
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

	t.Run("TTL rounds up; sub-day stays 1 day; only zero falls back to global", func(t *testing.T) {
		// No TTL requested: fall back to the global default.
		if cols := seriesToInput(lbls, tenantLabelForTest, 0, 365); cols.rowTTLDays != 365 {
			t.Errorf("row_ttl_days(0) = %d, want 365 (global default)", cols.rowTTLDays)
		}

		// A sub-day TTL is honored as 1 day, NOT silently replaced by the global.
		if cols := seriesToInput(lbls, tenantLabelForTest, 3600, 365); cols.rowTTLDays != 1 {
			t.Errorf("row_ttl_days(1h) = %d, want 1", cols.rowTTLDays)
		}

		// 32h rounds up to 2 days (one-day granularity, never truncated to 1).
		if cols := seriesToInput(lbls, tenantLabelForTest, 32*3600, 365); cols.rowTTLDays != 2 {
			t.Errorf("row_ttl_days(32h) = %d, want 2", cols.rowTTLDays)
		}
	})
}

func TestRowTTLDays(t *testing.T) {
	const global = 365

	cases := []struct {
		name       string
		ttlSeconds int64
		want       uint16
	}{
		{"zero falls back to global", 0, global},
		{"negative falls back to global", -10, global},
		{"one hour rounds up to 1 day", 3600, 1},
		{"just under a day rounds up to 1", 86399, 1},
		{"exactly one day stays 1", 86400, 1},
		{"25h rounds up to 2", 25 * 3600, 2},
		{"32h rounds up to 2", 32 * 3600, 2},
		{"exactly two days stays 2", 2 * 86400, 2},
		{"seven days", 7 * 86400, 7},
		{"365 days", 365 * 86400, 365},
		{"overflow capped at uint16 max", int64(70000) * 86400, math.MaxUint16},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rowTTLDays(tc.ttlSeconds, global); got != tc.want {
				t.Errorf("rowTTLDays(%d) = %d, want %d", tc.ttlSeconds, got, tc.want)
			}
		})
	}
}

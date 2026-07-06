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
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

func TestSeriesToRegister(t *testing.T) {
	c := &ClickHouseTSDB{seenSeries: make(map[string]seenEntry), metrics: newMetrics(nil)}
	t0 := time.Unix(1000, 0)

	a := registration{labels: labels.FromStrings("__name__", "a")}
	b := registration{labels: labels.FromStrings("__name__", "b")}

	// New series are due for registration.
	if got := c.seriesToRegister([]registration{a, b}, t0); len(got) != 2 {
		t.Fatalf("new series due = %d, want 2", len(got))
	}

	// After registering a, only b (never registered) is still due.
	c.markRegistered([]registration{a}, t0)

	got := c.seriesToRegister([]registration{a, b}, t0)
	if len(got) != 1 || !labels.Equal(got[0].labels, b.labels) {
		t.Fatalf("after registering a, due = %v, want [b]", got)
	}

	c.markRegistered([]registration{b}, t0)

	// Nothing due right after registration.
	if got := c.seriesToRegister([]registration{a, b}, t0); len(got) != 0 {
		t.Fatalf("just registered, due = %d, want 0", len(got))
	}

	// Both due again once the re-register interval has elapsed.
	later := t0.Add(reRegisterGap + time.Minute)
	if got := c.seriesToRegister([]registration{a, b}, later); len(got) != 2 {
		t.Fatalf("after interval, due = %d, want 2", len(got))
	}
}

func TestComputeMaxExpiration(t *testing.T) {
	storeMax := time.UnixMilli(storeMaxTimeMillis)

	cases := []struct {
		name       string
		maxTS      int64
		rowTTLDays uint16
		want       time.Time
	}{
		{
			// 2026-06-19 12:00 + 1 day TTL + 2h gap = 2026-06-20 14:00,
			// rounded up to the next day = 2026-06-21 00:00 UTC.
			name:       "rounds up to next day",
			maxTS:      time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC).UnixMilli(),
			rowTTLDays: 1,
			want:       time.Date(2026, 6, 21, 0, 0, 0, 0, time.UTC),
		},
		{
			// A point at the store max + gap + round-up exceeds the store range,
			// so the expiration is clamped down to the store max.
			name:       "clamped at store max (gap/round-up overflow)",
			maxTS:      storeMaxTimeMillis,
			rowTTLDays: 0,
			want:       storeMax,
		},
		{
			name:       "clamped at store max (far-future point + TTL)",
			maxTS:      storeMaxTimeMillis,
			rowTTLDays: 365,
			want:       storeMax,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := computeMaxExpiration(tc.maxTS, tc.rowTTLDays); !got.Equal(tc.want) {
				t.Errorf("computeMaxExpiration(%d, %d) = %s, want %s", tc.maxTS, tc.rowTTLDays, got, tc.want)
			}
		})
	}
}

func TestSweepSeenSeries(t *testing.T) {
	c := &ClickHouseTSDB{seenSeries: make(map[string]seenEntry), metrics: newMetrics(nil)}
	start := time.Unix(1000, 0)

	a := registration{labels: labels.FromStrings("__name__", "a")}
	b := registration{labels: labels.FromStrings("__name__", "b")}

	c.seriesToRegister([]registration{a, b}, start)
	c.markRegistered([]registration{a, b}, start)

	// "a" is written again just before the sweep; "b" stays idle.
	later := start.Add(seenSeriesTTL - time.Minute)
	c.seriesToRegister([]registration{a}, later)

	c.sweepSeenSeries(start.Add(seenSeriesTTL + time.Second))

	if _, ok := c.seenSeries[string(a.labels.Bytes(nil))]; !ok {
		t.Errorf("active series a was evicted")
	}

	if _, ok := c.seenSeries[string(b.labels.Bytes(nil))]; ok {
		t.Errorf("idle series b was not evicted")
	}
}

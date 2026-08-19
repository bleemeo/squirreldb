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

// reg builds a registration the way the drain path does: labels plus the
// canonical key (seriesKey) the seen-cache is keyed on.
func reg(name string) registration {
	l := labels.FromStrings("__name__", name)

	return registration{labels: l, key: seriesKey(l)}
}

func TestSeriesToRegister(t *testing.T) {
	c := &ClickHouseTSDB{seenSeries: make(map[string]seenEntry), metrics: newMetrics(nil)}
	t0 := time.Unix(1000, 0)

	a := reg("a")
	b := reg("b")

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

// TestSeriesToRegisterBacklog checks the coverage trigger: with the wall clock
// frozen (so nextRegister never elapses), a series is re-registered only when a
// batch's timestamps fall outside the indexed [min_time, max_time] range — the
// case a fast backlog hits, its point timestamps outrunning the wall clock.
func TestSeriesToRegisterBacklog(t *testing.T) {
	c := &ClickHouseTSDB{seenSeries: make(map[string]seenEntry), metrics: newMetrics(nil)}

	now := time.Unix(1000, 0) // frozen: only the coverage trigger can fire below

	const hourMs = int64(3_600_000)

	base := int64(2_000_000_000_000) // arbitrary ms

	// step runs one drain batch [minMs, maxMs] through the cache and reports whether
	// the series was (re-)registered.
	step := func(minMs, maxMs int64) bool {
		r := reg("m")
		r.minTS, r.maxTS = minMs, maxMs

		due := c.seriesToRegister([]registration{r}, now)
		c.markRegistered(due, now)

		return len(due) == 1
	}

	if !step(base, base) {
		t.Fatal("new series should register")
	}

	// Within the indexed range + reRegisterGap margin -> not re-registered.
	if step(base+hourMs, base+hourMs) {
		t.Error("points within coverage must not re-register")
	}

	// Backlog: maxTS jumps past the indexed max_time (base + reRegisterGap) while the
	// wall clock is unchanged, so nextRegister cannot fire — only the coverage trigger.
	past := base + reRegisterGap.Milliseconds() + hourMs
	if !step(past, past) {
		t.Error("points past the indexed max_time must re-register (backlog)")
	}

	// Now covered up to past + reRegisterGap -> a batch below it does not re-register.
	if step(past+hourMs, past+hourMs) {
		t.Error("points within the extended coverage must not re-register")
	}

	// Backfill older than the indexed min_time -> re-registered.
	if !step(base-hourMs, past+hourMs) {
		t.Error("points older than the indexed min_time must re-register (backfill)")
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

	a := reg("a")
	b := reg("b")

	c.seriesToRegister([]registration{a, b}, start)
	c.markRegistered([]registration{a, b}, start)

	// "a" is written again just before the sweep; "b" stays idle.
	later := start.Add(seenSeriesTTL - time.Minute)
	c.seriesToRegister([]registration{a}, later)

	c.sweepSeenSeries(start.Add(seenSeriesTTL + time.Second))

	if _, ok := c.seenSeries[a.key]; !ok {
		t.Errorf("active series a was evicted")
	}

	if _, ok := c.seenSeries[b.key]; ok {
		t.Errorf("idle series b was not evicted")
	}
}

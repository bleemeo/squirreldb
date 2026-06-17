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

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

func TestSeriesToRegister(t *testing.T) {
	c := &ClickHouseTSDB{seenSeries: make(map[uint64]seenEntry)}
	t0 := time.Unix(1000, 0)

	a := types.Timeseries{Labels: labels.FromStrings("__name__", "a")}
	b := types.Timeseries{Labels: labels.FromStrings("__name__", "b")}

	// New series are due for registration.
	if got := c.seriesToRegister([]types.Timeseries{a, b}, t0); len(got) != 2 {
		t.Fatalf("new series due = %d, want 2", len(got))
	}

	// After registering a, only b (never registered) is still due.
	c.markRegistered([]types.Timeseries{a}, t0)

	got := c.seriesToRegister([]types.Timeseries{a, b}, t0)
	if len(got) != 1 || !labels.Equal(got[0].Labels, b.Labels) {
		t.Fatalf("after registering a, due = %v, want [b]", got)
	}

	c.markRegistered([]types.Timeseries{b}, t0)

	// Nothing due right after registration.
	if got := c.seriesToRegister([]types.Timeseries{a, b}, t0); len(got) != 0 {
		t.Fatalf("just registered, due = %d, want 0", len(got))
	}

	// Both due again once the re-register interval has elapsed.
	later := t0.Add(reRegisterGap + time.Minute)
	if got := c.seriesToRegister([]types.Timeseries{a, b}, later); len(got) != 2 {
		t.Fatalf("after interval, due = %d, want 2", len(got))
	}
}

func TestSweepSeenSeries(t *testing.T) {
	c := &ClickHouseTSDB{seenSeries: make(map[uint64]seenEntry)}
	start := time.Unix(1000, 0)

	a := types.Timeseries{Labels: labels.FromStrings("__name__", "a")}
	b := types.Timeseries{Labels: labels.FromStrings("__name__", "b")}

	c.seriesToRegister([]types.Timeseries{a, b}, start)
	c.markRegistered([]types.Timeseries{a, b}, start)

	// "a" is written again just before the sweep; "b" stays idle.
	later := start.Add(seenSeriesTTL - time.Minute)
	c.seriesToRegister([]types.Timeseries{a}, later)

	c.sweepSeenSeries(start.Add(seenSeriesTTL + time.Second))

	if _, ok := c.seenSeries[a.Labels.Hash()]; !ok {
		t.Errorf("active series a was evicted")
	}

	if _, ok := c.seenSeries[b.Labels.Hash()]; ok {
		t.Errorf("idle series b was not evicted")
	}
}

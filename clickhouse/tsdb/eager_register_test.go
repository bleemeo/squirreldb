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

func TestUnseenSeriesAndMarkSeen(t *testing.T) {
	c := &ClickHouseTSDB{seenSeries: make(map[uint64]time.Time)}
	now := time.Unix(1000, 0)

	a := types.Timeseries{Labels: labels.FromStrings("__name__", "cpu_used", "item", "/")}
	b := types.Timeseries{Labels: labels.FromStrings("__name__", "cpu_used", "item", "/home")}

	// Both are new initially.
	if got := c.unseenSeries([]types.Timeseries{a, b}, now); len(got) != 2 {
		t.Fatalf("unseenSeries = %d new, want 2", len(got))
	}

	// Mark only a as seen.
	c.markSeen([]types.Timeseries{a}, now)

	got := c.unseenSeries([]types.Timeseries{a, b}, now)
	if len(got) != 1 {
		t.Fatalf("unseenSeries after marking a = %d new, want 1", len(got))
	}

	if !labels.Equal(got[0].Labels, b.Labels) {
		t.Errorf("remaining unseen = %s, want %s", got[0].Labels, b.Labels)
	}

	// After marking b too, nothing is new.
	c.markSeen([]types.Timeseries{b}, now)

	if got := c.unseenSeries([]types.Timeseries{a, b}, now); len(got) != 0 {
		t.Errorf("unseenSeries after marking both = %d new, want 0", len(got))
	}
}

func TestSweepSeenSeries(t *testing.T) {
	c := &ClickHouseTSDB{seenSeries: make(map[uint64]time.Time)}
	start := time.Unix(1000, 0)

	a := types.Timeseries{Labels: labels.FromStrings("__name__", "a")}
	b := types.Timeseries{Labels: labels.FromStrings("__name__", "b")}

	c.markSeen([]types.Timeseries{a, b}, start)

	// "a" is written again just before the sweep; "b" stays idle.
	later := start.Add(seenSeriesTTL - time.Minute)
	c.unseenSeries([]types.Timeseries{a}, later)

	// Sweep at start + TTL + 1: "b" is stale (last seen at start), "a" is fresh.
	c.sweepSeenSeries(start.Add(seenSeriesTTL + time.Second))

	if got := c.unseenSeries([]types.Timeseries{a, b}, later); len(got) != 1 {
		t.Fatalf("after sweep, unseen = %d, want 1 (b evicted)", len(got))
	} else if !labels.Equal(got[0].Labels, b.Labels) {
		t.Errorf("evicted series = %s, want %s", got[0].Labels, b.Labels)
	}
}

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

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

// TestMemWALRoundTrip drives the in-memory client through the exact walStore
// operations the write/drain paths use (Append -> Len -> Peek -> Trim), to make
// sure its Redis-list semantics match what the drainer relies on.
func TestMemWALRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	w := &walStore{client: newMemWALClient(), key: "wal"}

	mkEntry := func(ts int64) walEntry {
		return walEntry{
			Labels: labels.FromStrings("__name__", "m", "id", "x"),
			TTL:    3600,
			Points: []types.MetricPoint{{Timestamp: ts, Value: float64(ts)}},
		}
	}

	// Append 5 entries (one point each); Append returns the WAL length.
	entries := make([]walEntry, 5)
	for i := range entries {
		entries[i] = mkEntry(int64(i))
	}

	n, err := w.Append(ctx, entries)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if n != 5 {
		t.Fatalf("Append returned len %d, want 5", n)
	}

	if got, _ := w.Len(ctx); got != 5 {
		t.Fatalf("Len = %d, want 5", got)
	}

	// Peek the head; entries must come back in insertion order, decoded intact.
	peeked, err := w.Peek(ctx, 3)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}

	if len(peeked) != 3 {
		t.Fatalf("Peek returned %d entries, want 3", len(peeked))
	}

	for i, e := range peeked {
		if len(e.Points) != 1 || e.Points[0].Timestamp != int64(i) {
			t.Fatalf("Peek entry %d = %+v, want timestamp %d", i, e.Points, i)
		}
	}

	// Trim the 3 drained entries; the tail (2) must remain, still in order.
	if err := w.Trim(ctx, 3); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	if got, _ := w.Len(ctx); got != 2 {
		t.Fatalf("Len after Trim = %d, want 2", got)
	}

	rest, err := w.Peek(ctx, 100)
	if err != nil {
		t.Fatalf("Peek after Trim: %v", err)
	}

	if len(rest) != 2 || rest[0].Points[0].Timestamp != 3 || rest[1].Points[0].Timestamp != 4 {
		t.Fatalf("remaining entries = %+v, want timestamps [3 4]", rest)
	}

	// Trimming everything empties the WAL.
	if err := w.Trim(ctx, 2); err != nil {
		t.Fatalf("Trim rest: %v", err)
	}

	if got, _ := w.Len(ctx); got != 0 {
		t.Fatalf("Len after full Trim = %d, want 0", got)
	}
}

// TestMemWALLease checks the in-memory lease grants to the sole owner, refreshes
// for that owner, and (for faithful semantics) refuses a competing live owner.
func TestMemWALLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := newMemWALClient()

	held, err := c.LeaseAcquire(ctx, "lease", "node-a", time.Minute)
	if err != nil || !held {
		t.Fatalf("first acquire = (%v, %v), want (true, nil)", held, err)
	}

	// Same owner refreshes.
	if held, _ := c.LeaseAcquire(ctx, "lease", "node-a", time.Minute); !held {
		t.Fatal("owner should refresh its own lease")
	}

	// A competing owner is refused while the lease is live.
	if held, _ := c.LeaseAcquire(ctx, "lease", "node-b", time.Minute); held {
		t.Fatal("a second owner should not take a live lease")
	}

	// After release, another owner can take it.
	if err := c.LeaseRelease(ctx, "lease", "node-a"); err != nil {
		t.Fatalf("release: %v", err)
	}

	if held, _ := c.LeaseAcquire(ctx, "lease", "node-b", time.Minute); !held {
		t.Fatal("lease should be free after release")
	}
}

func TestNormalizeRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		start, stop, n int64
		wantLo, wantHi int64
	}{
		{"full via negative stop", 0, -1, 5, 0, 4},
		{"head window", 0, 2, 5, 0, 2},
		{"trim keep tail", 3, -1, 5, 3, 4},
		{"empty list", 0, -1, 0, 0, -1},
		{"stop clamped to end", 0, 100, 3, 0, 2},
		{"start past end is empty", 10, 20, 5, 10, 4}, // lo > hi -> empty
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lo, hi := normalizeRange(tc.start, tc.stop, tc.n)
			if lo != tc.wantLo || hi != tc.wantHi {
				t.Fatalf("normalizeRange(%d,%d,%d) = (%d,%d), want (%d,%d)",
					tc.start, tc.stop, tc.n, lo, hi, tc.wantLo, tc.wantHi)
			}
		})
	}
}

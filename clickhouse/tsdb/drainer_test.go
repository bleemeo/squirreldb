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
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

func newTestDrainer(w *walStore, write func(context.Context, []walEntry) error, maxPoints int) *walDrainer {
	return &walDrainer{
		wal:       w,
		leaseKey:  "lease",
		owner:     "test-owner",
		leaseTTL:  time.Minute,
		maxPoints: maxPoints,
		write:     write,
		metrics:   newMetrics(prometheus.NewRegistry()),
		logger:    zerolog.Nop(),
	}
}

func fillWAL(t *testing.T, w *walStore, n int) {
	t.Helper()

	entries := make([]walEntry, n)
	for i := range entries {
		entries[i] = walEntry{
			Labels: labels.FromStrings("__name__", "m"),
			TTL:    3600,
			Points: []types.MetricPoint{{Timestamp: int64(i), Value: float64(i)}},
		}
	}

	if _, err := w.Append(context.Background(), entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// TestFinalDrainFlushesWAL verifies a clean-shutdown drain empties the WAL and
// hands every buffered point to the ClickHouse write func, across several insert
// cycles (maxPoints < buffered count).
func TestFinalDrainFlushesWAL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	w := &walStore{client: newMemWALClient(), key: "wal"}
	fillWAL(t, w, 10)

	var written int

	d := newTestDrainer(w, func(_ context.Context, entries []walEntry) error {
		written += len(entries)

		return nil
	}, 3) // 3 points per insert -> 4 cycles for 10 points

	d.finalDrain(ctx)

	if written != 10 {
		t.Fatalf("write func received %d points, want 10", written)
	}

	if n, _ := w.Len(ctx); n != 0 {
		t.Fatalf("WAL length after finalDrain = %d, want 0 (fully flushed)", n)
	}
}

// TestFinalDrainKeepsEntriesOnWriteError verifies a failed ClickHouse write leaves
// the entries in the WAL (not trimmed), so nothing is silently dropped.
func TestFinalDrainKeepsEntriesOnWriteError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	w := &walStore{client: newMemWALClient(), key: "wal"}
	fillWAL(t, w, 5)

	d := newTestDrainer(w, func(_ context.Context, _ []walEntry) error {
		return errors.New("clickhouse unreachable")
	}, 100)

	d.finalDrain(ctx)

	if n, _ := w.Len(ctx); n != 5 {
		t.Fatalf("WAL length after failed drain = %d, want 5 (entries preserved)", n)
	}
}

// TestFinalDrainEmptyWAL is a no-op fast path: no write func call, no error.
func TestFinalDrainEmptyWAL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	w := &walStore{client: newMemWALClient(), key: "wal"}

	called := false

	d := newTestDrainer(w, func(_ context.Context, _ []walEntry) error {
		called = true

		return nil
	}, 100)

	d.finalDrain(ctx)

	if called {
		t.Fatal("write func should not be called for an empty WAL")
	}
}

// TestDrainCycleFenceSkipsTrimOnLostLease reproduces the data-loss scenario the
// trim fence guards against: our lease lapses during the ClickHouse insert, a
// takeover node re-drains and trims the same head, then its lease also lapses (so
// the lease is now FREE). The fence must NOT re-claim that free lease and trim,
// which would drop entries appended after the takeover's trim.
func TestDrainCycleFenceSkipsTrimOnLostLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mem := newMemWALClient()
	w := &walStore{client: mem, key: "wal"}
	fillWAL(t, w, 10)

	// The write func simulates, mid-insert, a takeover node draining the same 10-entry
	// head (Trim) and 5 fresh points arriving afterwards, then the lease going free.
	var d *walDrainer

	d = newTestDrainer(w, func(_ context.Context, _ []walEntry) error {
		if err := w.Trim(ctx, 10); err != nil {
			t.Fatalf("simulated takeover Trim: %v", err)
		}

		fillWAL(t, w, 5) //nolint:contextcheck

		mem.mu.Lock()
		delete(mem.leases, d.leaseKey) // lease lapsed and is now free
		mem.mu.Unlock()

		return nil
	}, 10)

	d.drainCycle(ctx)

	// The 5 post-takeover entries must survive: the fence sees a free (not held)
	// lease and skips the trim. With the old LeaseAcquire fence the free lease would
	// be re-granted and Trim(10) would delete them.
	if n, _ := w.Len(ctx); n != 5 {
		t.Fatalf("WAL length after fenced drainCycle = %d, want 5 (post-takeover entries preserved)", n)
	}
}

// TestFinalDrainReleasesLease verifies the lease is free after finalDrain, so a
// takeover node acquires it immediately instead of waiting out the TTL. Checked for
// both the drained path and the empty-WAL fast path.
func TestFinalDrainReleasesLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	assertLeaseFree := func(t *testing.T, client walClient, key string) {
		t.Helper()

		held, err := client.LeaseAcquire(ctx, key, "takeover-node", time.Minute)
		if err != nil {
			t.Fatalf("LeaseAcquire: %v", err)
		}

		if !held {
			t.Fatal("lease still held after finalDrain; a takeover must wait for the TTL")
		}
	}

	t.Run("after draining", func(t *testing.T) {
		t.Parallel()

		mem := newMemWALClient()
		w := &walStore{client: mem, key: "wal"}
		fillWAL(t, w, 4) //nolint:contextcheck

		d := newTestDrainer(w, func(_ context.Context, _ []walEntry) error { return nil }, 2)
		d.finalDrain(ctx)

		assertLeaseFree(t, mem, d.leaseKey)
	})

	t.Run("empty WAL fast path", func(t *testing.T) {
		t.Parallel()

		mem := newMemWALClient()
		w := &walStore{client: mem, key: "wal"}

		d := newTestDrainer(w, func(_ context.Context, _ []walEntry) error { return nil }, 2)
		// Simulate having been the leader (as the periodic loop would leave it).
		if _, err := mem.LeaseAcquire(ctx, d.leaseKey, d.owner, time.Minute); err != nil {
			t.Fatalf("pre-acquire: %v", err)
		}

		d.finalDrain(ctx)

		assertLeaseFree(t, mem, d.leaseKey)
	})
}

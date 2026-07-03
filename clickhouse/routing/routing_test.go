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

package routing

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/rs/zerolog"
)

// newTestConn builds a Conn with the given hosts but no background monitor, so
// pick/observe can be exercised without a real ClickHouse.
func newTestConn(hosts ...*host) *Conn {
	return &Conn{hosts: hosts, probeNow: make(chan struct{}, 1), logger: zerolog.Nop()}
}

func mkHost(addr string, reachable, ready bool) *host {
	h := &host{addr: addr}
	h.set(reachable, ready, "", zerolog.Nop())

	return h
}

func TestPick(t *testing.T) {
	t.Parallel()

	t.Run("prefers ready hosts", func(t *testing.T) {
		t.Parallel()

		ready := mkHost("ready", true, true)
		c := newTestConn(mkHost("reachable", true, false), ready)

		for range 10 {
			h, err := c.pick()
			if err != nil {
				t.Fatalf("pick: %v", err)
			}

			if h != ready {
				t.Fatalf("pick returned %q, want the ready host", h.addr)
			}
		}
	})

	t.Run("falls back to reachable when none ready", func(t *testing.T) {
		t.Parallel()

		c := newTestConn(mkHost("down", false, false), mkHost("reachable", true, false))

		h, err := c.pick()
		if err != nil {
			t.Fatalf("pick: %v", err)
		}

		if h.addr != "reachable" {
			t.Fatalf("pick returned %q, want the reachable host", h.addr)
		}
	})

	t.Run("errors when no host reachable", func(t *testing.T) {
		t.Parallel()

		c := newTestConn(mkHost("down1", false, false), mkHost("down2", false, false))

		if _, err := c.pick(); err == nil {
			t.Fatal("pick should error when no host is reachable")
		}
	})

	t.Run("error surfaces the per-host cause", func(t *testing.T) {
		t.Parallel()

		down := &host{addr: "clickhouse:9000"}
		down.set(false, false, "code: 516, AUTHENTICATION_FAILED", zerolog.Nop())
		c := newTestConn(down)

		_, err := c.pick()
		if err == nil {
			t.Fatal("pick should error when no host is reachable")
		}

		if !strings.Contains(err.Error(), "clickhouse:9000") ||
			!strings.Contains(err.Error(), "AUTHENTICATION_FAILED") {
			t.Fatalf("error should carry the host and its cause, got: %v", err)
		}
	})

	t.Run("round-robins over ready hosts", func(t *testing.T) {
		t.Parallel()

		a := mkHost("a", true, true)
		b := mkHost("b", true, true)
		c := newTestConn(a, b)

		seen := map[string]int{}

		for range 4 {
			h, _ := c.pick()
			seen[h.addr]++
		}

		if seen["a"] != 2 || seen["b"] != 2 {
			t.Fatalf("expected even round-robin, got %v", seen)
		}
	})
}

func TestObserve(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                     string
		err                      error
		wantReachable, wantReady bool
	}{
		{"nil error leaves ready", nil, true, true},
		{"context canceled leaves ready", context.Canceled, true, true},
		{"context deadline leaves ready", context.DeadlineExceeded, true, true},
		{"unknown table demotes to reachable-not-ready", &proto.Exception{Code: codeUnknownTable}, true, false},
		{"other server exception leaves ready", &proto.Exception{Code: 241}, true, true},
		{"connection error demotes to down", errors.New("dial tcp: connection refused"), false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := mkHost("h", true, true)
			c := newTestConn(h)

			c.observe(h, tc.err)

			reachable, ready := h.state()
			if reachable != tc.wantReachable || ready != tc.wantReady {
				t.Fatalf("after observe: reachable=%v ready=%v, want reachable=%v ready=%v",
					reachable, ready, tc.wantReachable, tc.wantReady)
			}
		})
	}
}

func TestResolveQueryBudget(t *testing.T) {
	t.Parallel()

	// A configured value is used as-is, raised to the minimum, without needing a host.
	got, err := (&Conn{opts: Options{MaxConcurrentQueries: 5}}).resolveQueryBudget(context.Background())
	if got != 5 || err != nil {
		t.Errorf("configured budget = (%d, %v), want (5, nil)", got, err)
	}

	got, err = (&Conn{opts: Options{MaxConcurrentQueries: 1}}).resolveQueryBudget(context.Background())
	if got != minQueryBudget || err != nil {
		t.Errorf("below-min budget = (%d, %v), want (%d, nil)", got, err, minQueryBudget)
	}

	// Auto-detect (0) with no reachable host can't query the server -> error, so the
	// caller retries rather than starting with the limiter silently disabled.
	if _, err := newTestConn().resolveQueryBudget(context.Background()); err == nil {
		t.Error("auto-detect without host should error")
	}
}

// TestQueryConcurrencyReservation verifies reads are capped at budget-1 so a write
// always finds a slot, and that a full sem blocks further acquisitions.
func TestQueryConcurrencyReservation(t *testing.T) {
	t.Parallel()

	// budget = 2 -> sem cap 2, readSem cap 1 (one read + one reserved write).
	c := &Conn{sem: make(chan struct{}, 2), readSem: make(chan struct{}, 1)}
	ctx := context.Background()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	// First read takes readSem(1/1) + sem(1/2).
	if err := c.acquireRead(ctx); err != nil {
		t.Fatalf("first acquireRead: %v", err)
	}

	// A second read blocks (readSem full) — a cancelled ctx surfaces that.
	if err := c.acquireRead(cancelled); err == nil {
		t.Error("second acquireRead should block while reads are at budget-1")
	}

	// ...but a write still gets the reserved slot (sem has 1 free).
	if err := c.acquireWrite(ctx); err != nil {
		t.Fatalf("acquireWrite should succeed while reads are maxed: %v", err)
	}

	// Now sem is full (2/2): another write blocks.
	if err := c.acquireWrite(cancelled); err == nil {
		t.Error("acquireWrite should block when sem is full")
	}

	// Releasing frees the slots for a new read.
	c.releaseWrite()
	c.releaseRead()

	if err := c.acquireRead(ctx); err != nil {
		t.Fatalf("acquireRead after release: %v", err)
	}

	c.releaseRead()
}

// TestQueryConcurrencyUnlimited checks the acquire/release helpers are no-ops when
// no limit is configured (nil semaphores).
func TestQueryConcurrencyUnlimited(t *testing.T) {
	t.Parallel()

	c := &Conn{} // sem/readSem nil

	if err := c.acquireRead(context.Background()); err != nil {
		t.Fatalf("acquireRead unlimited: %v", err)
	}

	c.releaseRead()

	if err := c.acquireWrite(context.Background()); err != nil {
		t.Fatalf("acquireWrite unlimited: %v", err)
	}

	c.releaseWrite()
}

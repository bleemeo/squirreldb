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

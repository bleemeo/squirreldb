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

package telemetrystore

import (
	"context"
	"testing"
	"time"
)

// TestCloseStopsRefreshLoop checks that Close cancels the refresh loops and
// waits for them: the loop must observe closeCh and return well before its
// (1 minute) refresh tick, so Close returns quickly. No ClickHouse is needed —
// the loop returns before any lease write.
func TestCloseStopsRefreshLoop(t *testing.T) {
	t.Parallel()

	s := &Store{closeCh: make(chan struct{}), refreshInterval: defaultRefreshInterval}

	loopCtx, loopCancel := context.WithCancel(t.Context())
	defer loopCancel()

	s.wg.Go(func() {
		s.refreshLoop(loopCtx, &nodeIDLease{owner: "owner", id: "id"})
	})

	done := make(chan struct{})

	go func() {
		_ = s.Close()

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return: refresh loop was not stopped and awaited")
	}

	// Close is idempotent.
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestPickFreeSlot(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		slots     []slot
		wantID    string
		wantFound bool
	}{
		{
			name:      "empty pool",
			slots:     nil,
			wantFound: false,
		},
		{
			name: "all held and live",
			slots: []slot{
				{id: "a", owner: "n1", expired: false},
				{id: "b", owner: "n2", expired: false},
			},
			wantFound: false,
		},
		{
			name: "expired lease is free",
			slots: []slot{
				{id: "a", owner: "n1", expired: false},
				{id: "b", owner: "n2", expired: true},
			},
			wantID:    "b",
			wantFound: true,
		},
		{
			name: "released slot (empty owner) is free",
			slots: []slot{
				{id: "a", owner: "", expired: false},
			},
			wantID:    "a",
			wantFound: true,
		},
		{
			name: "first reusable slot wins",
			slots: []slot{
				{id: "a", owner: "n1", expired: false},
				{id: "b", owner: "", expired: false},
				{id: "c", owner: "n3", expired: true},
			},
			wantID:    "b",
			wantFound: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id, found := pickFreeSlot(tc.slots)
			if found != tc.wantFound || id != tc.wantID {
				t.Fatalf("pickFreeSlot = (%q, %v), want (%q, %v)", id, found, tc.wantID, tc.wantFound)
			}
		})
	}
}

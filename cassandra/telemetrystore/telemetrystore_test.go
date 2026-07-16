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
	"testing"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/telemetry"

	"github.com/rs/zerolog/log"
)

// TestNodeID exercises the node-ID pool: unique IDs per live node, stable across
// a restart, and reusable (possibly swapped) once released.
func TestNodeID(t *testing.T) {
	t.Parallel()

	// A shared lock factory + state stands in for the shared backend; each
	// "node" gets its own store on top, like separate SquirrelDB processes.
	lockFactory := &dummy.Locks{}
	state := &dummy.States{}

	store1 := newStore(lockFactory, state)
	store2 := newStore(lockFactory, state)

	// Two nodes acquire two different IDs.
	id1, release1 := acquire(t, store1)
	id2, release2 := acquire(t, store2)

	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("each node should get a unique non-empty ID: id1=%s, id2=%s", id1, id2)
	}

	// Node 1 restarts: after releasing, a fresh store reuses the same ID.
	release1()

	store1 = newStore(lockFactory, state)

	gotID1, release1 := acquire(t, store1)
	if gotID1 != id1 {
		t.Fatalf("node 1 should reuse its ID after a restart: previous=%s, new=%s", id1, gotID1)
	}

	// Both nodes stop, node 2 restarts before node 1: the IDs may be swapped.
	release1()
	release2()

	store2 = newStore(lockFactory, state)
	store1 = newStore(lockFactory, state)

	newID2, _ := acquire(t, store2)
	newID1, _ := acquire(t, store1)

	if newID2 != id1 {
		t.Fatalf("node 2 should take node 1's previous ID: want=%s, got=%s", id1, newID2)
	}

	if newID1 != id2 {
		t.Fatalf("node 1 should take node 2's previous ID: want=%s, got=%s", id2, newID1)
	}
}

func newStore(lockFactory *dummy.Locks, state *dummy.States) *Store {
	return New(state, lockFactory, false, telemetry.StoreTiming{}, log.With().Str("component", "telemetry").Logger())
}

func acquire(t *testing.T, store *Store) (string, func()) {
	t.Helper()

	id, release, err := store.AcquireNodeID(t.Context())
	if err != nil {
		t.Fatalf("AcquireNodeID: %s", err)
	}

	return id(), release
}

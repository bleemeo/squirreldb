// Copyright 2015-2025 Bleemeo
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

package telemetry

import (
	"testing"

	"github.com/bleemeo/squirreldb/dummy"

	"github.com/rs/zerolog/log"
)

func TestTelemetryID(t *testing.T) {
	t.Parallel()

	lockFactory := &dummy.Locks{}
	state := &dummy.States{}

	// Two SquirrelDB are started, each one gets a different telemetry ID.
	telemetry1, id1 := createTelemetryAndGetID(t, lockFactory, state)
	telemetry2, id2 := createTelemetryAndGetID(t, lockFactory, state)

	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("Each SquirrelDB should get a unique non empty ID: id1=%s, id2=%s", id1, id2)
	}

	// Get SquirrelDB 1 ID again, it should stay the same.
	newID1, err := telemetry1.getTelemetryID(t.Context())
	if err != nil {
		t.Fatalf("Failed to get telemetry ID: %s", err)
	}

	if newID1 != id1 {
		t.Fatalf(
			"SquirrelDB 1 should get the same ID when getTelemetryID is called twice: "+
				"previousID=%s, newID=%s", id1, newID1,
		)
	}

	// SquirrelDB 1 is restarted, it should get the same ID it had before.
	telemetry1.stop()

	telemetry1, newID1 = createTelemetryAndGetID(t, lockFactory, state)
	if newID1 != id1 {
		t.Fatalf("SquirrelDB 1 should get the same ID after a restart: previousID=%s, newID=%s", id1, newID1)
	}

	// Both SquirrelDB are stopped, SquirrelDB 2 is restarted before SquirrelDB 1.
	// The two IDs could be swapped, SquirrelDB 2 could take the ID that SquirrelDB 1 had before.
	// This is actually what will happen in the current implementation.
	telemetry1.stop()
	telemetry2.stop()

	telemetry2, newID2 := createTelemetryAndGetID(t, lockFactory, state)
	telemetry1, newID1 = createTelemetryAndGetID(t, lockFactory, state)

	if newID2 != id1 {
		t.Fatalf(
			"SquirrelDB 2 should get the previous ID of SquirrelDB 1: "+
				"previousID2=%s, newID2=%s, previousID1=%s", id2, newID2, id1,
		)
	}

	if newID1 != id2 {
		t.Fatalf(
			"SquirrelDB 1 should get the previous ID of SquirrelDB 2: "+
				"previousID1=%s, newID1=%s, previousID2=%s", id1, newID1, id2,
		)
	}

	telemetry1.stop()
	telemetry2.stop()
}

func createTelemetryAndGetID(t *testing.T, lockFactory *dummy.Locks, state *dummy.States) (*Telemetry, string) {
	t.Helper()

	telemetry := New(Options{
		LockFactory: lockFactory,
		State:       state,
		Logger:      log.With().Str("component", "telemetry-1").Logger(),
	})

	id, err := telemetry.getTelemetryID(t.Context())
	if err != nil {
		t.Fatalf("Failed to get telemetry ID: %s", err)
	}

	return telemetry, id
}

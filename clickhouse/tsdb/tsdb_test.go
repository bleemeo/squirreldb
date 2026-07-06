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
)

// TestQuerierLifecycle checks that queriers are tracked as in-flight (so Close
// can wait for them to release the shared connection), that closing a querier
// releases its slot, and that no querier is handed out once Close has started.
func TestQuerierLifecycle(t *testing.T) {
	c := &ClickHouseTSDB{}

	// No in-flight query: the wait returns immediately.
	if !c.waitInflightQueries(context.Background()) {
		t.Fatal("waitInflightQueries returned false with no in-flight query")
	}

	q, err := c.newSQLQuerier("", 0, 1000)
	if err != nil {
		t.Fatalf("newSQLQuerier: %v", err)
	}

	// With a querier in flight, the bounded wait times out instead of blocking.
	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if c.waitInflightQueries(shortCtx) {
		t.Fatal("waitInflightQueries returned true while a querier is still in flight")
	}

	// Closing the querier releases its slot; Close must be idempotent on the slot.
	if err := q.Close(); err != nil {
		t.Fatalf("querier Close: %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("second querier Close: %v", err)
	}

	if !c.waitInflightQueries(context.Background()) {
		t.Fatal("waitInflightQueries still blocks after the querier was closed")
	}
}

// TestNewSQLQuerierRefusedAfterClose checks the close gate: once Close has marked
// the store closed, no new querier is created (which would use the connection
// that Close is about to shut).
func TestNewSQLQuerierRefusedAfterClose(t *testing.T) {
	c := &ClickHouseTSDB{}

	c.closeMu.Lock()
	c.closed = true
	c.closeMu.Unlock()

	if _, err := c.newSQLQuerier("", 0, 1000); !errors.Is(err, errClosed) {
		t.Fatalf("newSQLQuerier after close = %v, want errClosed", err)
	}
}

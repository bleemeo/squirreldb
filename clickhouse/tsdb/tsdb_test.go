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
	"strings"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// TestDropTablesRefusesNonTestDatabase checks the safety guard: DropTables must
// refuse any database other than the test one unless forced, so it can never wipe
// production tables. The guard runs before any connection, so no ClickHouse is needed.
func TestDropTablesRefusesNonTestDatabase(t *testing.T) {
	err := DropTables(context.Background(), config.ClickHouse{Database: "production"}, false)
	if err == nil {
		t.Fatal("DropTables should refuse a non-test database without forceNonTestDatabase")
	}

	// The error should name the refused database (it must not have tried to connect).
	if !strings.Contains(err.Error(), "production") {
		t.Errorf("error should name the refused database, got: %v", err)
	}
}

// TestNewTemporaryStore checks the store selection contract: an in-memory store
// only when Redis is not configured, and a hard error (never a silent in-memory
// fallback) when Redis is configured but unreachable — a fallback would make the
// labels store node-local and break multi-node takeover.
func TestNewTemporaryStore(t *testing.T) {
	logger := zerolog.Nop()

	t.Run("no redis configured returns in-memory store", func(t *testing.T) {
		opts := Options{Reg: prometheus.NewRegistry()}

		store, err := newTemporaryStore(context.Background(), opts, logger)
		if err != nil {
			t.Fatalf("newTemporaryStore returned error: %v", err)
		}

		if store == nil {
			t.Fatal("newTemporaryStore returned a nil store")
		}
	})

	t.Run("configured but unreachable redis errors without falling back", func(t *testing.T) {
		// A cancelled context makes the Redis connection attempt fail
		// deterministically (IsCluster pings through the context).
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		opts := Options{
			Reg:   prometheus.NewRegistry(),
			Redis: config.Redis{Addresses: []string{"127.0.0.1:6379"}},
		}

		store, err := newTemporaryStore(ctx, opts, logger)
		if err == nil {
			t.Fatalf("newTemporaryStore silently returned a store (%T) instead of erroring "+
				"when Redis is configured but unreachable", store)
		}
	})
}

// TestQuerierLifecycle checks that queriers are tracked as in-flight (so Close
// can wait for them to release the shared connection), that closing a querier
// releases its slot, and that no querier is handed out once Close has started.
func TestQuerierLifecycle(t *testing.T) {
	c := &ClickHouseTSDB{}

	// No in-flight query: the wait returns immediately.
	if !c.waitInflightQueries(context.Background()) {
		t.Fatal("waitInflightQueries returned false with no in-flight query")
	}

	q, err := c.newSQLQuerier(0, 1000)
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

	if _, err := c.newSQLQuerier(0, 1000); !errors.Is(err, errClosed) {
		t.Fatalf("newSQLQuerier after close = %v, want errClosed", err)
	}
}

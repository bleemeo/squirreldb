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

// Package telemetrystore implements the ClickHouse-backed telemetry.Store: the
// shared cluster ID and a pool of per-node IDs with a crash-tolerant ownership
// lease. ClickHouse is the source of authority for both.
//
// ClickHouse has no transactions, so the lease is optimistic and leans on the
// telemetry's soft contract: both tables are ReplacingMergeTree(version) (latest
// write wins on read with FINAL), the lease holds an owner + an expiry, and a
// node that races and loses a slot notices at its next RefreshNodeID (owner no
// longer matches) and re-acquires. The telemetry's staggered start makes
// simultaneous claims rare in the first place.
package telemetrystore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	// leaseTTL is how long a node-ID lease stays valid without a refresh. On a
	// crash the lease lapses after at most this long, freeing the ID.
	leaseTTL = 5 * time.Minute
	// refreshInterval is how often the background loop renews the lease. It MUST
	// be smaller than leaseTTL so a live node never lets its lease lapse.
	refreshInterval = time.Minute
	// claimAttempts bounds how many times AcquireNodeID retries when it loses a
	// slot to a concurrent claimer.
	claimAttempts = 3
	// writeTimeout bounds a background lease write (refresh or release), so a
	// stuck ClickHouse can never block a refresh loop (and thus Close) forever.
	writeTimeout = 5 * time.Second
)

// clusterIDKey is the single row key of telemetry_cluster_id.
const clusterIDKey = "cluster_id"

var errClaimContended = errors.New("could not claim a telemetry node ID")

// SchemaObjects returns the ClickHouse objects owned by the telemetry store.
// Exposed so the backend can create the whole schema up front and derive the
// expected-table set, independently of whether the store is ever instantiated.
func SchemaObjects(clusterName string) []ddl.Object {
	statements := []struct {
		table string
		ddl   string
	}{
		{"telemetry_cluster_id", clusterDDL},
		{"telemetry_node_ids", nodeIDsDDL},
	}

	objects := make([]ddl.Object, 0, len(statements))

	for _, stmt := range statements {
		engine := ddl.ReplicatedEngine(clusterName, stmt.table, "ReplacingMergeTree", "version")
		stmtDDL := strings.Replace(stmt.ddl, "{{ENGINE}}", engine, 1)
		objects = append(objects, ddl.Object{Name: stmt.table, DDL: ddl.Apply(stmtDDL, clusterName), Replicated: true})
	}

	return objects
}

const (
	clusterDDL = `
		CREATE TABLE IF NOT EXISTS telemetry_cluster_id{{ON_CLUSTER}} (
			k String,
			cluster_id String,
			version DateTime64(3) DEFAULT now64(3)
		)
		ENGINE = {{ENGINE}}
		ORDER BY k`

	nodeIDsDDL = `
		CREATE TABLE IF NOT EXISTS telemetry_node_ids{{ON_CLUSTER}} (
			telemetry_id String,
			owner String,
			lease_until DateTime64(3),
			version DateTime64(3) DEFAULT now64(3)
		)
		ENGINE = {{ENGINE}}
		ORDER BY telemetry_id`
)

// Store is the ClickHouse telemetry store. The connection is owned by the caller
// (clickhouse/backend) and shared with the other ClickHouse stores.
//
// Each acquired node ID runs a background lease-refresh goroutine. They are all
// tracked by wg and stopped by closeCh, so Close stops every refresh loop and
// waits for it to return before the shared connection may be closed.
type Store struct {
	conn        driver.Conn
	clusterName string
	logger      zerolog.Logger

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// New returns a new ClickHouse telemetry store. clusterName enables ClickHouse
// cluster mode (ON CLUSTER + Replicated with an explicit, name-based Keeper path;
// the tables must not be RENAMEd) when set. Schema creation is owned by
// clickhouse/backend (created up front for every store), so the store assumes its
// tables already exist.
func New(_ context.Context, conn driver.Conn, clusterName string, logger zerolog.Logger) (*Store, error) {
	return &Store{
		conn:        conn,
		clusterName: clusterName,
		logger:      logger,
		closeCh:     make(chan struct{}),
	}, nil
}

// Close stops every node-ID refresh loop and waits for them to return, so the
// shared connection can then be closed without a refresh racing it. It does not
// free the held IDs (their leases simply lapse); a graceful release is done by
// the release function returned from AcquireNodeID.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
	})

	s.wg.Wait()

	return nil
}

// ClusterID returns the shared cluster ID, creating it on first use. On the
// (one-time, brand-new-cluster) race where two nodes create one concurrently,
// it inserts then reads back so every node converges on the same winning ID.
func (s *Store) ClusterID(ctx context.Context) (string, error) {
	id, found, err := s.readClusterID(ctx)
	if err != nil {
		return "", err
	}

	if found {
		s.logger.Debug().Msgf("Read telemetry cluster ID %s", id)

		return id, nil
	}

	newID := uuid.New().String()

	if err := s.insertClusterID(ctx, newID); err != nil {
		return "", err
	}

	// Read back so concurrent creators converge on the single winning row.
	id, found, err = s.readClusterID(ctx)
	if err != nil {
		return "", err
	}

	if !found {
		// Should not happen right after a successful insert; fall back to ours.
		id = newID
	}

	s.logger.Debug().Msgf("Created telemetry cluster ID %s", id)

	return id, nil
}

func (s *Store) readClusterID(ctx context.Context) (string, bool, error) {
	rows, err := s.conn.Query(ctx,
		"SELECT cluster_id FROM telemetry_cluster_id FINAL WHERE k = ?", clusterIDKey)
	if err != nil {
		return "", false, fmt.Errorf("read cluster id: %w", err)
	}

	defer rows.Close()

	if !rows.Next() {
		return "", false, rows.Err()
	}

	var id string

	if err := rows.Scan(&id); err != nil {
		return "", false, fmt.Errorf("scan cluster id: %w", err)
	}

	return id, true, rows.Err()
}

func (s *Store) insertClusterID(ctx context.Context, id string) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO telemetry_cluster_id (k, cluster_id)")
	if err != nil {
		return fmt.Errorf("prepare insert cluster id: %w", err)
	}

	defer batch.Close()

	if err := batch.Append(clusterIDKey, id); err != nil {
		return fmt.Errorf("append cluster id: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert cluster id: %w", err)
	}

	return nil
}

// slot is the in-Go view of a telemetry_node_ids row, with the expiry already
// evaluated against the ClickHouse server clock.
type slot struct {
	id      string
	owner   string
	expired bool
}

// pickFreeSlot returns the first reusable slot: one whose lease has expired or
// that was explicitly released (empty owner).
func pickFreeSlot(slots []slot) (string, bool) {
	for _, sl := range slots {
		if sl.expired || sl.owner == "" {
			return sl.id, true
		}
	}

	return "", false
}

// AcquireNodeID claims a node ID and keeps its lease alive in a background loop
// until the returned release function is called (which frees the slot). The
// owner identity is internal: a fresh UUID per acquisition.
func (s *Store) AcquireNodeID(ctx context.Context) (string, func(), error) { //nolint:contextcheck
	owner := uuid.New().String()

	id, err := s.claim(ctx, owner)
	if err != nil {
		return "", nil, err
	}

	// The lease is renewed from a background loop, independent of the (possibly
	// short-lived) acquisition context. The loop is tracked by s.wg and ends on
	// either stop (this release) or s.closeCh (the whole Store closing).
	loopCtx, loopCancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.wg.Go(func() {
		defer close(done)

		s.refreshLoop(loopCtx, owner, id)
	})

	var once sync.Once

	release := func() { //nolint:contextcheck
		once.Do(func() {
			// Stop our refresh loop and wait for it to return before freeing the
			// slot, so a last refresh write can't resurrect the lease we clear.
			loopCancel()
			<-done

			rctx, rcancel := context.WithTimeout(context.Background(), writeTimeout)
			defer rcancel()

			if err := s.writeLease(rctx, id, "", time.Unix(0, 0)); err != nil {
				s.logger.Err(err).Msgf("Failed to release telemetry ID %s", id)
			}
		})
	}

	return id, release, nil
}

// claim picks a free slot (or a new ID), writes the lease, then confirms it owns
// the slot by reading it back. This makes a concurrent claim of the same free
// slot safe: the loser of the read-back retries and picks a different slot,
// instead of two nodes silently sharing one ID.
func (s *Store) claim(ctx context.Context, owner string) (string, error) {
	for range claimAttempts {
		slots, err := s.readSlots(ctx)
		if err != nil {
			return "", err
		}

		id, reused := pickFreeSlot(slots)
		if !reused {
			id = uuid.New().String()
		}

		if err := s.writeLease(ctx, id, owner, time.Now().Add(leaseTTL)); err != nil {
			return "", err
		}

		ownsIt, err := s.isOwner(ctx, id, owner)
		if err != nil {
			return "", err
		}

		if ownsIt {
			if reused {
				s.logger.Debug().Msgf("Reusing telemetry ID %s", id)
			} else {
				s.logger.Debug().Msgf("Created telemetry ID %s", id)
			}

			return id, nil
		}
	}

	return "", fmt.Errorf("%w after %d attempts", errClaimContended, claimAttempts)
}

// refreshLoop renews the lease until this acquisition is released (stop) or the
// whole Store is closed (s.closeCh). Once a slot is owned, no other node will
// take it (they only reuse free/expired slots), so this just keeps the lease
// from lapsing — no ownership re-check is needed. Each write is bounded by
// writeTimeout so a stuck ClickHouse can't block Close.
func (s *Store) refreshLoop(ctx context.Context, owner, id string) {
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := s.writeLease(ctx, id, owner, time.Now().Add(leaseTTL))

			cancel()

			if err != nil {
				s.logger.Err(err).Msgf("Failed to refresh telemetry ID %s", id)
			}
		}
	}
}

// isOwner reports whether owner currently holds the lease on id.
func (s *Store) isOwner(ctx context.Context, id, owner string) (bool, error) {
	rows, err := s.conn.Query(ctx,
		"SELECT owner FROM telemetry_node_ids FINAL WHERE telemetry_id = ?", id)
	if err != nil {
		return false, fmt.Errorf("read node id owner: %w", err)
	}

	defer rows.Close()

	currentOwner := ""
	if rows.Next() {
		if err := rows.Scan(&currentOwner); err != nil {
			return false, fmt.Errorf("scan node id owner: %w", err)
		}
	}

	return currentOwner == owner, rows.Err()
}

// readSlots returns the current pool, expiry evaluated against the server clock.
func (s *Store) readSlots(ctx context.Context) ([]slot, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT telemetry_id, owner, lease_until <= now64(3) AS expired
		FROM telemetry_node_ids FINAL`)
	if err != nil {
		return nil, fmt.Errorf("read node ids: %w", err)
	}

	defer rows.Close()

	var slots []slot

	for rows.Next() {
		var sl slot

		if err := rows.Scan(&sl.id, &sl.owner, &sl.expired); err != nil {
			return nil, fmt.Errorf("scan node id: %w", err)
		}

		slots = append(slots, sl)
	}

	return slots, rows.Err()
}

func (s *Store) writeLease(ctx context.Context, id, owner string, leaseUntil time.Time) error {
	batch, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO telemetry_node_ids (telemetry_id, owner, lease_until)")
	if err != nil {
		return fmt.Errorf("prepare write lease: %w", err)
	}

	defer batch.Close()

	if err := batch.Append(id, owner, leaseUntil); err != nil {
		return fmt.Errorf("append lease: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("write lease: %w", err)
	}

	return nil
}

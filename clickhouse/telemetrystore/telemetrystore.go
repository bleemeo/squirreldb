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
// write wins on read with FINAL), and the lease holds an owner + an expiry. Two
// safeguards keep two live nodes from durably sharing an ID:
//   - at claim time a node writes its lease then reads it back to confirm
//     ownership; in cluster mode it waits claimSettleDelay first so a concurrent
//     claim on another replica has time to replicate — the loser then sees the
//     winner and retries a different slot instead of both confirming;
//   - the refresh loop re-checks ownership on every tick and, if the slot was
//     taken over (a split that slipped past the read-back, only possible across
//     replicas in cluster mode), re-acquires a different slot and switches to it.
//
// Because the loop can switch slots, the node ID is handed to the caller as a
// getter (see AcquireNodeID); the telemetry reads it afresh at each report, so a
// healed ID propagates well before the next (24h) report. The telemetry's
// staggered start makes simultaneous claims rare in the first place.
package telemetrystore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"
	"github.com/bleemeo/squirreldb/telemetry"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	// defaultLeaseTTL is how long a node-ID lease stays valid without a refresh. On
	// a crash the lease lapses after at most this long, freeing the ID.
	defaultLeaseTTL = 5 * time.Minute
	// defaultRefreshInterval is how often the background loop renews the lease. It
	// MUST be smaller than the lease TTL so a live node never lets its lease lapse.
	defaultRefreshInterval = time.Minute
	// claimAttempts bounds how many times AcquireNodeID retries when it loses a
	// slot to a concurrent claimer.
	claimAttempts = 3
	// opTimeout bounds a single background ClickHouse round-trip (a lease read or
	// write in the refresh/claim/release paths), so a stuck ClickHouse can never
	// block a refresh loop (and thus Close) forever.
	opTimeout = 5 * time.Second
	// defaultClaimSettleDelay is how long claim waits, in cluster mode only,
	// between writing its lease and reading it back, so a concurrent claim on
	// another replica has time to replicate and the read-back reflects the real
	// winner. Single-node mode skips it (write and read-back hit the same replica).
	defaultClaimSettleDelay = 2 * time.Second
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

	// Lease timing, resolved from telemetry.StoreTiming at New (zero → default).
	refreshInterval  time.Duration
	leaseTTL         time.Duration
	claimSettleDelay time.Duration

	closeOnce sync.Once
	closeCh   chan struct{}
	wg        sync.WaitGroup
}

// New returns a new ClickHouse telemetry store. clusterName enables ClickHouse
// cluster mode (ON CLUSTER + Replicated with an explicit, name-based Keeper path;
// the tables must not be RENAMEd) when set. timing overrides the lease timing
// (zero fields keep the built-in defaults); it is a test hook, not a user option.
// Schema creation is owned by clickhouse/backend (created up front for every
// store), so the store assumes its tables already exist.
func New(
	_ context.Context, conn driver.Conn, clusterName string,
	timing telemetry.StoreTiming, logger zerolog.Logger,
) (*Store, error) {
	return &Store{
		conn:             conn,
		clusterName:      clusterName,
		logger:           logger,
		refreshInterval:  orDefault(timing.RefreshInterval, defaultRefreshInterval),
		leaseTTL:         orDefault(timing.LeaseTTL, defaultLeaseTTL),
		claimSettleDelay: orDefault(timing.ClaimSettleDelay, defaultClaimSettleDelay),
		closeCh:          make(chan struct{}),
	}, nil
}

// orDefault returns v when it is positive, otherwise def.
func orDefault(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}

	return def
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

// pickFreeSlot returns a slot to (re)claim. It first prefers a slot this owner
// already holds (whatever its expiry), then falls back to the first reusable one
// — a lapsed lease or an explicitly released slot (empty owner).
//
// Preferring an owned slot is a no-op on a first claim (the owner is a fresh UUID
// that holds nothing), but it collapses a redundant ID in two cases where an
// earlier of our own writes already made us the owner: a later claimAttempts
// retry after a spurious (replication-lag) read-back, and a refresh heal after a
// spurious "takeover". Without it we would skip our own slot (not expired, owner
// set) and allocate a second ID while still holding the first.
func pickFreeSlot(slots []slot, owner string) (string, bool) {
	for _, sl := range slots {
		if sl.owner == owner {
			return sl.id, true
		}
	}

	for _, sl := range slots {
		if sl.expired || sl.owner == "" {
			return sl.id, true
		}
	}

	return "", false
}

// nodeIDLease is the mutable, concurrency-safe holder of the ID a refresh loop
// currently owns. The loop may switch the ID when it heals a split (see
// refreshLoop), so the getter returned by AcquireNodeID reads it under the lock.
type nodeIDLease struct {
	owner string

	mu sync.Mutex
	id string
}

func (l *nodeIDLease) currentID() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.id
}

func (l *nodeIDLease) setID(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.id = id
}

// AcquireNodeID claims a node ID and keeps its lease alive in a background loop
// until the returned release function is called (which frees the slot). The
// owner identity is internal: a fresh UUID per acquisition. The returned getter
// reports the currently-owned ID, which the loop may change to heal a split (see
// refreshLoop) — callers must read it afresh rather than cache it.
func (s *Store) AcquireNodeID(ctx context.Context) (func() string, func(), error) { //nolint:contextcheck
	owner := uuid.New().String()

	id, err := s.claim(ctx, owner)
	if err != nil {
		return nil, nil, err
	}

	lease := &nodeIDLease{owner: owner, id: id}

	// The lease is renewed from a background loop, independent of the (possibly
	// short-lived) acquisition context. The loop is tracked by s.wg and ends on
	// either stop (this release) or s.closeCh (the whole Store closing).
	loopCtx, loopCancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.wg.Go(func() {
		defer close(done)

		s.refreshLoop(loopCtx, lease)
	})

	var once sync.Once

	release := func() { //nolint:contextcheck
		once.Do(func() {
			// Stop our refresh loop and wait for it to return before freeing the
			// slot, so a last refresh write can't resurrect the lease we clear.
			loopCancel()
			<-done

			rctx, rcancel := context.WithTimeout(context.Background(), opTimeout)
			defer rcancel()

			// Free whatever slot we currently hold (the loop may have switched it).
			id := lease.currentID()
			if err := s.writeLease(rctx, id, "", time.Unix(0, 0)); err != nil {
				s.logger.Err(err).Msgf("Failed to release telemetry ID %s", id)
			}
		})
	}

	return lease.currentID, release, nil
}

// claim picks a free slot (or a new ID), writes the lease, then confirms it owns
// the slot by reading it back. This makes a concurrent claim of the same free
// slot safe: the loser of the read-back retries and picks a different slot,
// instead of two nodes silently sharing one ID.
func (s *Store) claim(ctx context.Context, owner string) (string, error) {
	for range claimAttempts {
		// Each ClickHouse round-trip is individually bounded by opTimeout, so a
		// single stuck query can neither wedge the loop nor blow the caller's budget
		// (see healTimeout in refresh, sized from this per-op bound).
		readCtx, readCancel := context.WithTimeout(ctx, opTimeout)
		slots, err := s.readSlots(readCtx)

		readCancel()

		if err != nil {
			return "", err
		}

		id, reused := pickFreeSlot(slots, owner)
		if !reused {
			id = uuid.New().String()
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, opTimeout)
		err = s.writeLease(writeCtx, id, owner, time.Now().Add(s.leaseTTL))

		writeCancel()

		if err != nil {
			return "", err
		}

		// In cluster mode, let a concurrent claim on another replica replicate
		// before reading back, so the read-back reflects the real (max-version)
		// winner and the loser retries a different slot instead of both confirming.
		if s.clusterName != "" {
			select {
			case <-time.After(s.claimSettleDelay):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}

		checkCtx, checkCancel := context.WithTimeout(ctx, opTimeout)
		ownsIt, err := s.isOwner(checkCtx, id, owner)

		checkCancel()

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

// refreshLoop keeps the lease alive until this acquisition is released (loop ctx
// cancelled) or the whole Store is closed (s.closeCh). Each tick it re-checks
// ownership and either renews (still ours) or heals (taken over): see refresh.
// The loop ctx is folded together with s.closeCh so an in-flight write or heal
// is cancelled promptly on Close and can't block it.
func (s *Store) refreshLoop(ctx context.Context, lease *nodeIDLease) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-s.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	ticker := time.NewTicker(s.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refresh(ctx, lease)
		}
	}
}

// refresh renews the lease for one tick. It first reads back the current owner:
// if we still own the slot it renews, otherwise a concurrent claimer took it
// over (a split that got past the claim read-back, only reachable across
// replicas in cluster mode) and we re-acquire a different slot and switch to it,
// so the two nodes stop sharing one ID. Each step is time-bounded so a stuck
// ClickHouse can't wedge the loop (and thus Close).
func (s *Store) refresh(ctx context.Context, lease *nodeIDLease) {
	id := lease.currentID()

	checkCtx, checkCancel := context.WithTimeout(ctx, opTimeout)
	owns, err := s.isOwner(checkCtx, id, lease.owner)

	checkCancel()

	if err != nil {
		s.logger.Err(err).Msgf("Failed to verify telemetry ID %s ownership", id)

		return
	}

	if owns {
		writeCtx, writeCancel := context.WithTimeout(ctx, opTimeout)
		err := s.writeLease(writeCtx, id, lease.owner, time.Now().Add(s.leaseTTL))

		writeCancel()

		if err != nil {
			s.logger.Err(err).Msgf("Failed to refresh telemetry ID %s", id)
		}

		return
	}

	s.logger.Warn().Msgf("Telemetry ID %s was taken over by another node; re-acquiring a new one", id)

	// healTimeout must cover every claim attempt so a slow (not dead) ClickHouse
	// doesn't cancel a legitimate re-claim mid-flight. Each attempt does three
	// ClickHouse round-trips (readSlots, writeLease, isOwner), each bounded by
	// opTimeout, plus the cluster settle delay.
	const claimRoundTripsPerAttempt = 3

	healTimeout := claimAttempts * (s.claimSettleDelay + claimRoundTripsPerAttempt*opTimeout)

	healCtx, healCancel := context.WithTimeout(ctx, healTimeout)
	newID, err := s.claim(healCtx, lease.owner)

	healCancel()

	if err != nil {
		s.logger.Err(err).Msg("Failed to re-acquire a telemetry ID after losing the slot")

		return
	}

	lease.setID(newID)

	// claim re-picks the slot we still own when it can (see pickFreeSlot), so an
	// unchanged id means the takeover signal was spurious (a replication-lag read),
	// not a real cross-node split.
	if newID == id {
		s.logger.Info().Msgf("Telemetry ID %s ownership re-confirmed (takeover signal was spurious)", id)
	} else {
		s.logger.Info().Msgf("Re-acquired telemetry ID %s (previously %s)", newID, id)
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

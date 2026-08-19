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
	"time"

	"github.com/rs/zerolog"
)

// drainPollInterval is how often a node re-checks the drainer lease and the WAL
// length. It also bounds the lease-refresh cadence, so it must be well below the
// lease TTL.
const drainPollInterval = time.Second

// walDrainer is the single cluster-wide loop that writes the Redis WAL to
// ClickHouse. Every node runs one, but only the lease holder ("leader") drains;
// the others idle-poll and take over within ~the lease TTL if the leader dies.
type walDrainer struct {
	wal      *walStore
	leaseKey string
	owner    string // unique per process, identifies the lease holder
	leaseTTL time.Duration

	flushDelay time.Duration // drain at least this often...
	soft       int           // ...or as soon as the WAL reaches this many points
	maxPoints  int           // cap of points per ClickHouse insert

	// write persists a batch of WAL entries to ClickHouse (registers series +
	// inserts points). Injected so the loop stays backend-agnostic.
	write func(ctx context.Context, entries []walEntry) error

	metrics *metrics
	logger  zerolog.Logger
}

// run is the drainer loop. It returns when ctx is done, releasing the lease if held
// so another node can take over promptly.
func (d *walDrainer) run(ctx context.Context) {
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()

	var (
		leader    bool
		lastDrain time.Time
	)

	// The lease is NOT released here on exit: run only exits during Close, which
	// immediately calls finalDrain — that flushes the WAL using the still-held lease
	// and releases it at the end (releasing here would force finalDrain to re-acquire
	// and race a takeover). A crash instead of a clean Close lets the lease lapse on
	// its TTL, the normal safety net.

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		held := false

		if h, err := d.wal.client.LeaseAcquire(ctx, d.leaseKey, d.owner, d.leaseTTL); err != nil {
			d.logger.Warn().Err(err).Msg("acquire ClickHouse WAL drainer lease")
		} else {
			held = h
		}

		// Log both transitions: an error above (or another node taking the lease)
		// leaves held=false, so a silent leadership loss is logged here too.
		switch {
		case held && !leader:
			d.logger.Info().Msg("became ClickHouse WAL drainer (leader)")
			d.metrics.WALDrainerIsLeader.Set(1)
		case !held && leader:
			d.logger.Info().Msg("no longer the ClickHouse WAL drainer (lease lost or taken over by another node)")
			d.metrics.WALDrainerIsLeader.Set(0)
		}

		leader = held

		// Publish the WAL length (buffered points) every tick (every node observes the
		// same shared list), so the gauge reflects drains, not just this node's appends.
		walLen, err := d.wal.Len(ctx)
		if err != nil {
			d.logger.Warn().Err(err).Msg("read WAL length")

			continue
		}

		d.metrics.WALLength.Set(float64(walLen))

		if !leader {
			continue
		}

		// Drain on the timer, or early once the WAL grows past the soft threshold.
		// The timer check allows half a poll interval of slack: we only re-check
		// every drainPollInterval, and lastDrain is recorded just after a drain (a
		// hair past a tick), so a strict `>= flushDelay` would never be true at the
		// tick nearest flushDelay and would always round the effective period UP to
		// the next tick (a 5s delay polled at 1s drains every ~6s). The half-poll
		// slack centers the sampling error so the average period is flushDelay.
		if time.Since(lastDrain) < d.flushDelay-drainPollInterval/2 && walLen < int64(d.soft) {
			continue
		}

		d.drainCycle(ctx)

		lastDrain = time.Now()
	}
}

// drainCycle writes the WAL to ClickHouse in maxPoints-sized inserts until it is
// empty (or leadership/ctx is lost). It refreshes the lease each iteration so a
// long backlog drain doesn't let it lapse, and fences the trim on still holding the
// lease so a lost-leadership mid-insert never trims not-yet-persisted entries.
func (d *walDrainer) drainCycle(ctx context.Context) {
	for ctx.Err() == nil {
		if held, err := d.wal.client.LeaseAcquire(ctx, d.leaseKey, d.owner, d.leaseTTL); err != nil || !held {
			return
		}

		entries, err := d.wal.Peek(ctx, d.maxPoints)
		if err != nil {
			d.logger.Warn().Err(err).Msg("peek WAL")

			return
		}

		if len(entries) == 0 {
			return // WAL drained
		}

		if err := d.write(ctx, entries); err != nil {
			d.logger.Warn().Err(err).Msg("write WAL batch to ClickHouse")

			return // leave the entries in the WAL, retry next cycle
		}

		// Fence: only trim if we still hold the lease. LeaseHeld (not LeaseAcquire)
		// so a lease that lapsed during the insert — and is now free — is not silently
		// re-granted to us: another node may already have re-drained and trimmed the
		// same head, and trimming again would drop its not-yet-persisted entries. It
		// also refreshes the TTL, closing the window between this check and the Trim.
		// On a lost lease we skip the trim and let the new leader re-drain — duplicate
		// rows are deduplicated on read.
		if held, err := d.wal.client.LeaseHeld(ctx, d.leaseKey, d.owner, d.leaseTTL); err != nil || !held {
			d.logger.Warn().Msg("lost drainer lease during insert; skipping trim (entries will be re-drained)")

			return
		}

		if err := d.wal.Trim(ctx, len(entries)); err != nil {
			d.logger.Warn().Err(err).Msg("trim WAL")

			return
		}
	}
}

// finalDrain flushes the WAL to ClickHouse during a clean shutdown, after the
// periodic run loop has stopped. It reuses drainCycle, which acquires the lease
// first, so it never races a still-active leader on another node: in Redis mode a
// node that is not (or no longer) the leader no-ops and lets the real leader drain
// the shared WAL; in in-memory mode this process is the sole owner and always
// drains fully, leaving the data only in ClickHouse. It is bounded by ctx (the
// shutdown deadline).
//
// It always releases the lease before returning — including the empty-WAL fast
// path — so a takeover node acquires it immediately instead of waiting out the TTL.
func (d *walDrainer) finalDrain(ctx context.Context) {
	defer d.releaseLease() //nolint:contextcheck

	before, err := d.wal.Len(ctx)
	if err != nil {
		d.logger.Warn().Err(err).Msg("read WAL length before shutdown drain")
	} else if before == 0 {
		return // nothing buffered
	}

	if before > 0 {
		d.logger.Info().Int64("points", before).Msg("draining ClickHouse WAL before shutdown")
	}

	d.drainCycle(ctx)

	// Warn if anything remains (drain failed, lost/never held the lease, or the
	// shutdown deadline hit) — with the in-memory WAL those points are now lost.
	if after, err := d.wal.Len(ctx); err == nil && after > 0 {
		d.logger.Warn().Int64("remaining_points", after).
			Msg("ClickHouse WAL not fully drained at shutdown; buffered points remain (lost if the WAL is in-memory)")
	}
}

// releaseLease best-effort releases the lease on shutdown, bounded by its own
// timeout since ctx is already done by then.
func (d *walDrainer) releaseLease() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := d.wal.client.LeaseRelease(ctx, d.leaseKey, d.owner); err != nil {
		d.logger.Warn().Err(err).Msg("release ClickHouse WAL drainer lease")
	}
}

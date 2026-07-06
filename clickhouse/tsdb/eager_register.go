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
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

// registration is the tsdb_tags registration input for one series: its labels, the
// per-metric write TTL (seconds), and the min/max timestamp (ms) of the points that
// produced it. The TTL and range are per-series because a single drain batch mixes
// series from many writes with different TTLs and time ranges.
type registration struct {
	labels labels.Labels
	ttl    int64
	minTS  int64
	maxTS  int64
}

const (
	// reRegisterBase and reRegisterJitter set how often an active series is
	// re-registered in tsdb_tags to refresh its max_time/max_expiration. The
	// jitter (added per series) spreads re-registrations so active series don't
	// all hit ClickHouse at once. The interval is a free tradeoff: a longer one
	// means fewer tsdb_tags writes, but a staler max_time (label-query time filter)
	// and more index over-retention. It does NOT need to relate to the metric TTL
	// — correctness (the index outliving its data) comes from the reRegisterGap
	// margin baked into max_expiration, not from the interval.
	reRegisterBase   = time.Hour
	reRegisterJitter = time.Hour
	// reRegisterGap is the longest a series can go between two registrations
	// (base + max jitter). max_time and max_expiration carry it as a margin so the
	// index never excludes or expires a series before data written during the gap
	// (added before the day-rounding of max_expiration so the margin is guaranteed,
	// whatever the metric TTL — even a few seconds).
	reRegisterGap = reRegisterBase + reRegisterJitter

	// seenSeriesTTL is how long a series stays in the registration cache without
	// being written again. Active series are refreshed on every write so they
	// never expire; only idle/dead series are evicted, bounding the cache to the
	// active cardinality. A resumed series past this delay is simply re-registered.
	seenSeriesTTL = 24 * time.Hour
	// seenSeriesSweepInterval is how often expired cache entries are evicted.
	seenSeriesSweepInterval = time.Hour
)

// seenEntry tracks a series in the registration cache.
type seenEntry struct {
	lastSeen     time.Time // last write; drives eviction of idle series
	nextRegister time.Time // when to re-register, to refresh max_time/max_expiration
}

// registerSeries writes into tsdb_tags the series that are new or due for a
// refresh, so reads can resolve them and so their index row keeps a fresh
// expiration. It is called by the drainer for each batch it writes; tsdb_tags is
// written only here (no materialized view), while the point insert writes tsdb_data.
//
// min_time/max_time/max_expiration are derived from each series' points (not now),
// with a reRegisterGap margin on max_time and max_expiration so a series is never
// excluded or expired before data written until the next registration.
func (c *ClickHouseTSDB) registerSeries(ctx context.Context, regs []registration) error {
	now := time.Now()

	toRegister := c.seriesToRegister(regs, now)
	if len(toRegister) == 0 {
		return nil
	}

	defer func() {
		c.metrics.RegisterSeconds.Observe(time.Since(now).Seconds())
	}()

	batch, err := c.conn.PrepareBatch(
		ctx, "INSERT INTO tsdb_tags (metric_name, tenant, tags, min_time, max_time, max_expiration)",
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	defer batch.Close()

	globalDays := uint16(c.ttlDays()) //nolint:gosec

	for _, r := range toRegister {
		cols := seriesToInput(r.labels, c.opts.TenantLabel, r.ttl, globalDays)

		minTime := time.UnixMilli(r.minTS)
		// max_time covers the gap so label queries don't exclude the series for
		// data written until the next registration. maxTS is within the store range
		// (writes reject out-of-range timestamps), so maxTS+gap cannot overflow.
		maxTime := time.UnixMilli(r.maxTS).Add(reRegisterGap)
		maxExpiration := computeMaxExpiration(r.maxTS, cols.rowTTLDays)

		if err := batch.Append(cols.metricName, cols.tenant, cols.tags, minTime, maxTime, maxExpiration); err != nil {
			_ = batch.Abort()

			return fmt.Errorf("append row: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch to clickhouse: %w", err)
	}

	c.markRegistered(toRegister, now)
	c.metrics.RegisteredSeriesTotal.Add(float64(len(toRegister)))

	return nil
}

// seriesToRegister returns the series that are new or due for re-registration,
// and refreshes the last-seen time of every series (so active ones aren't evicted).
func (c *ClickHouseTSDB) seriesToRegister(regs []registration, now time.Time) []registration {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	var toRegister []registration

	for _, r := range regs {
		key := string(r.labels.Bytes(nil))

		entry, ok := c.seenSeries[key]
		if !ok || !now.Before(entry.nextRegister) {
			toRegister = append(toRegister, r)
		}

		entry.lastSeen = now
		c.seenSeries[key] = entry
	}

	c.metrics.SeenSeriesSize.Set(float64(len(c.seenSeries)))

	return toRegister
}

// markRegistered schedules the next re-registration of each series (after a
// successful insert), with per-series jitter to spread the load.
func (c *ClickHouseTSDB) markRegistered(regs []registration, now time.Time) {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	for _, r := range regs {
		key := string(r.labels.Bytes(nil))

		entry := c.seenSeries[key]
		entry.nextRegister = now.Add(reRegisterBase + time.Duration(rand.Int64N(int64(reRegisterJitter)))) //nolint:gosec
		c.seenSeries[key] = entry
	}
}

// runSeenSweeper periodically evicts cache entries not written for seenSeriesTTL.
func (c *ClickHouseTSDB) runSeenSweeper(ctx context.Context) {
	ticker := time.NewTicker(seenSeriesSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.sweepSeenSeries(time.Now())
		}
	}
}

// sweepSeenSeries evicts entries whose last-seen time is older than seenSeriesTTL.
func (c *ClickHouseTSDB) sweepSeenSeries(now time.Time) {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	for key, entry := range c.seenSeries {
		if now.Sub(entry.lastSeen) > seenSeriesTTL {
			delete(c.seenSeries, key)
		}
	}

	c.metrics.SeenSeriesSize.Set(float64(len(c.seenSeries)))
}

// computeMaxExpiration returns the tsdb_tags max_expiration for a series: the
// last point's expiration (point time + per-metric TTL) plus the reRegisterGap
// margin, rounded up to the next day (few distinct values -> better
// compression). The gap is added before the day-rounding so the margin holds
// regardless of the rounding granularity.
//
// The result is clamped to the store's max representable time: a far-future
// point (timestamps up to ~2299 are accepted) plus the TTL, the gap and the
// day round-up could otherwise exceed the DateTime64(3) column range. Clamping
// is safe — at worst the index row is purged slightly early, and only for
// timestamps far beyond any real data.
func computeMaxExpiration(maxTS int64, rowTTLDays uint16) time.Time {
	exp := time.UnixMilli(maxTS).
		Add(time.Duration(rowTTLDays) * 24 * time.Hour).
		Add(reRegisterGap).
		Truncate(24 * time.Hour).Add(24 * time.Hour)

	if storeMax := time.UnixMilli(storeMaxTimeMillis); exp.After(storeMax) {
		return storeMax
	}

	return exp
}

// pointsRange returns the min and max timestamp (ms) of a non-empty point slice.
func pointsRange(points []types.MetricPoint) (minTS, maxTS int64) {
	minTS = points[0].Timestamp
	maxTS = points[0].Timestamp

	for _, p := range points[1:] {
		if p.Timestamp < minTS {
			minTS = p.Timestamp
		}

		if p.Timestamp > maxTS {
			maxTS = p.Timestamp
		}
	}

	return minTS, maxTS
}

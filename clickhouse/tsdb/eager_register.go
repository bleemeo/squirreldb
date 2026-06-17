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
	"time"

	"github.com/bleemeo/squirreldb/types"
)

const (
	// seenSeriesTTL is how long a series stays in the registration cache without
	// being written again. Active series are refreshed on every write so they
	// never expire; only idle/dead series are evicted, bounding the cache to the
	// active cardinality instead of the lifetime cardinality. It must exceed the
	// largest write interval of an active series (re-registering is harmless but
	// wasteful). A resumed series past this delay is simply re-registered.
	seenSeriesTTL = 24 * time.Hour
	// seenSeriesSweepInterval is how often expired cache entries are evicted.
	seenSeriesSweepInterval = time.Hour
)

// registerNewSeries inserts series not yet seen into tsdb_tags, so reads can
// resolve them (and merge their batcher points) before the batcher flushes —
// otherwise a brand-new series would only become resolvable at its first flush.
//
// The id/tenant/tags columns are computed by tsdb_tags DEFAULTs from all_tags;
// we provide a non-null min_time/max_time (now) so the row survives the tags
// TTL (based on max_time) until the flush refines it.
func (c *ClickHouseTSDB) registerNewSeries(ctx context.Context, series []types.Timeseries) error {
	now := time.Now()

	newSeries := c.unseenSeries(series, now)
	if len(newSeries) == 0 {
		return nil
	}

	defer func() {
		c.metrics.RegisterSeconds.Observe(time.Since(now).Seconds())
	}()

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO tsdb_tags (metric_name, all_tags, min_time, max_time)")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	defer batch.Close()

	for _, ts := range newSeries {
		metricName, allTags, _ := seriesToInput(ts.Labels, 0, 0)

		if err := batch.Append(metricName, allTags, now, now); err != nil {
			_ = batch.Abort()

			return fmt.Errorf("append row: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch to clickhouse: %w", err)
	}

	c.markSeen(newSeries, now)
	c.metrics.RegisteredSeriesTotal.Add(float64(len(newSeries)))

	return nil
}

// unseenSeries returns the series not yet in the cache, while refreshing the
// last-seen time of those already there (so active series never expire).
func (c *ClickHouseTSDB) unseenSeries(series []types.Timeseries, now time.Time) []types.Timeseries {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	var newSeries []types.Timeseries

	for _, ts := range series {
		hash := ts.Labels.Hash()

		if _, ok := c.seenSeries[hash]; ok {
			c.seenSeries[hash] = now
		} else {
			newSeries = append(newSeries, ts)
		}
	}

	return newSeries
}

// markSeen records the series as registered, after a successful insert.
func (c *ClickHouseTSDB) markSeen(series []types.Timeseries, now time.Time) {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	for _, ts := range series {
		c.seenSeries[ts.Labels.Hash()] = now
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

	for hash, lastSeen := range c.seenSeries {
		if now.Sub(lastSeen) > seenSeriesTTL {
			delete(c.seenSeries, hash)
		}
	}
}

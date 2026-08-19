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
	"math"
	"time"
)

// commitBestEffort appends the batch to the WAL without ever blocking or
// returning an error, for a dual-write extra target. If the WAL is at the stop
// threshold (ClickHouse not draining) or the append fails (Redis/ClickHouse
// unavailable), the batch is dropped and accounted for later repair from the
// primary backend. It never applies blocking backpressure, so a ClickHouse
// problem can neither block nor fail the caller.
func (c *ClickHouseTSDB) commitBestEffort(ctx context.Context, pending []walEntry) {
	if c.lastWALLen.Load() >= int64(c.walStop) {
		// We were at/over the threshold. Re-read the real WAL length (a single
		// Redis LLEN, only on this exceptional path) so we resume as soon as the
		// drainer catches up instead of dropping forever on a stale cached length.
		n, err := c.wal.Len(ctx)
		if err != nil {
			c.accountDrop(pending)

			return
		}

		c.lastWALLen.Store(n)

		if n >= int64(c.walStop) {
			c.accountDrop(pending)

			return
		}
	}

	walLen, err := c.wal.Append(ctx, pending)
	if err != nil {
		c.accountDrop(pending)

		return
	}

	c.lastWALLen.Store(walLen)
	c.noteWriteSuccess()
}

// accountDrop records a dropped batch: bumps the dropped-points counter, extends
// the current drop episode and the cumulative global range (min/max of the
// dropped points' timestamps), and updates the drop gauges. The episode start is
// logged once, at the transition into a drop episode.
func (c *ClickHouseTSDB) accountDrop(pending []walEntry) {
	minTS, maxTS, n := batchTimeRange(pending)
	if n == 0 {
		return
	}

	c.metrics.WALDroppedPointsTotal.Add(float64(n))

	c.dropMu.Lock()
	defer c.dropMu.Unlock()

	first := !c.dropping.Load()
	if first {
		c.dropping.Store(true)
		c.dropEpisodeMin = minTS
		c.dropEpisodeMax = maxTS
	} else {
		c.dropEpisodeMin = min(c.dropEpisodeMin, minTS)
		c.dropEpisodeMax = max(c.dropEpisodeMax, maxTS)
	}

	// Cumulative global range, never reset: the safety-net repair range covering
	// every drop this instance has seen.
	if c.dropGlobalMin == 0 || minTS < c.dropGlobalMin {
		c.dropGlobalMin = minTS
	}

	if maxTS > c.dropGlobalMax {
		c.dropGlobalMax = maxTS
	}

	c.metrics.WALDroppedMinTimestamp.Set(float64(c.dropGlobalMin) / 1000)
	c.metrics.WALDroppedMaxTimestamp.Set(float64(c.dropGlobalMax) / 1000)

	if first {
		c.logger.Warn().
			Time("from", time.UnixMilli(minTS)).
			Time("to", time.UnixMilli(maxTS)).
			Msg("ClickHouse unavailable; dropping best-effort (dual-write) points — a repair range will be logged on recovery")
	}
}

// noteWriteSuccess closes a drop episode when a best-effort write succeeds again,
// logging the episode range (tight, minimal duplicates on repair) and the global
// range (covers all drops so far). It is a cheap atomic check on the normal path.
func (c *ClickHouseTSDB) noteWriteSuccess() {
	if !c.dropping.Load() {
		return
	}

	c.dropMu.Lock()
	defer c.dropMu.Unlock()

	if !c.dropping.Load() {
		return
	}

	c.dropping.Store(false)

	c.logger.Warn().
		Time("episode_from", time.UnixMilli(c.dropEpisodeMin)).
		Time("episode_to", time.UnixMilli(c.dropEpisodeMax)).
		Time("global_from", time.UnixMilli(c.dropGlobalMin)).
		Time("global_to", time.UnixMilli(c.dropGlobalMax)).
		Msg("ClickHouse recovered; re-run `squirreldb migrate` over the episode range (tight) " +
			"or the global range (covers every drop) to repair the dropped points from the primary")
}

// batchTimeRange returns the min and max point timestamp (ms) in the batch and
// the number of points.
func batchTimeRange(pending []walEntry) (minTS, maxTS int64, count int) {
	minTS = int64(math.MaxInt64)
	maxTS = int64(math.MinInt64)

	for _, e := range pending {
		for _, p := range e.Points {
			if p.Timestamp < minTS {
				minTS = p.Timestamp
			}

			if p.Timestamp > maxTS {
				maxTS = p.Timestamp
			}

			count++
		}
	}

	return minTS, maxTS, count
}

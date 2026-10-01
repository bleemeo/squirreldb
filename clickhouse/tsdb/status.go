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

	dto "github.com/prometheus/client_model/go"
)

// drainerStaleFactor is how many flush delays the leader may go without draining
// the WAL before the status reports the drainer as stalled.
const drainerStaleFactor = 6

// WALStatus is the state of the write-ahead log, for the admin status page.
type WALStatus struct {
	// Storage is "redis" (shared, durable) or "memory" (this node only).
	Storage string `json:"storage"`
	// Length is the number of buffered points, nil when it could not be read.
	Length *int64 `json:"length"`
	// LengthError is the error reading the length.
	LengthError string `json:"length_error,omitempty"`
	// SoftThreshold, CriticalThreshold and StopThreshold are the WAL lengths (in
	// points) above which the drainer flushes early, writes are slowed down, and
	// writes are blocked (or dropped by a best-effort extra backend).
	SoftThreshold     int `json:"soft_threshold"`
	CriticalThreshold int `json:"critical_threshold"`
	StopThreshold     int `json:"stop_threshold"`
	// Drainer is nil when this node runs no drainer (read-only or import-only).
	Drainer *DrainerStatus `json:"drainer"`
	// BestEffort is the drop accounting of the best-effort (dual-write extra)
	// writes. It stays empty when ClickHouse is the primary backend.
	BestEffort BestEffortStatus `json:"best_effort"`
}

// DrainerStatus is the state of this node's WAL drainer.
type DrainerStatus struct {
	// Leader tells whether this node holds the drainer lease (it writes the WAL to
	// ClickHouse). Exactly one node of the cluster should be the leader.
	Leader            bool    `json:"leader"`
	FlushDelaySeconds float64 `json:"flush_delay_seconds"`
	// LastDrain is when this node last emptied the WAL, nil if it never did.
	LastDrain   *time.Time `json:"last_drain"`
	LastError   string     `json:"last_error,omitempty"`
	LastErrorAt *time.Time `json:"last_error_at,omitempty"`
}

// BestEffortStatus is the drop accounting of the best-effort writes.
type BestEffortStatus struct {
	// Dropping tells whether a drop episode is ongoing (ClickHouse unavailable).
	Dropping      bool    `json:"dropping"`
	DroppedPoints float64 `json:"dropped_points"`
	// DroppedFrom and DroppedTo are the oldest and newest dropped point timestamps:
	// the range to repair with "squirreldb migrate". Nil when nothing was dropped.
	DroppedFrom *time.Time `json:"dropped_from,omitempty"`
	DroppedTo   *time.Time `json:"dropped_to,omitempty"`
}

// WALStatus returns the state of the WAL with its health. Reading the length is a
// single LLEN, the same request the drainer does every second.
func (c *ClickHouseTSDB) WALStatus(ctx context.Context) (WALStatus, types.ComponentStatus) {
	st := WALStatus{
		Storage:           "redis",
		SoftThreshold:     c.walCritical / walCriticalFactor,
		CriticalThreshold: c.walCritical,
		StopThreshold:     c.walStop,
	}

	if _, ok := c.walConn.(*memWALClient); ok {
		st.Storage = "memory"
	}

	health := types.ComponentStatus{Status: types.StatusOK}
	degrade := func(status string, reason string) {
		if types.WorstStatus(health.Status, status) != health.Status {
			health.Status, health.Reason = status, reason
		}
	}

	if n, err := c.wal.Len(ctx); err != nil {
		st.LengthError = err.Error()

		degrade(types.StatusDegraded, "cannot read the WAL length: "+err.Error())
	} else {
		st.Length = &n

		switch {
		case n >= int64(c.walStop):
			degrade(types.StatusDegraded,
				fmt.Sprintf("WAL holds %d points, above the stop threshold: writes are blocked", n))
		case n >= int64(c.walCritical):
			degrade(types.StatusDegraded,
				fmt.Sprintf("WAL holds %d points, above the critical threshold: writes are slowed down", n))
		}
	}

	if c.drainer != nil {
		ds := c.drainer.state()
		st.Drainer = &DrainerStatus{
			Leader:            ds.leader,
			FlushDelaySeconds: c.drainer.flushDelay.Seconds(),
			LastDrain:         timeOrNil(ds.lastDrained),
			LastError:         ds.lastErr,
			LastErrorAt:       timeOrNil(ds.lastErrAt),
		}

		stale := drainerStaleFactor * c.drainer.flushDelay
		if ds.leader && !ds.lastDrained.IsZero() && time.Since(ds.lastDrained) > stale {
			reason := fmt.Sprintf("this node is the WAL drainer but did not empty the WAL for %s",
				time.Since(ds.lastDrained).Truncate(time.Second))
			if ds.lastErr != "" {
				reason += " (last error: " + ds.lastErr + ")"
			}

			degrade(types.StatusDegraded, reason)
		}
	}

	st.BestEffort = c.bestEffortStatus()
	if st.BestEffort.Dropping {
		degrade(types.StatusDegraded, "ClickHouse unavailable: best-effort (dual-write) points are being dropped")
	}

	return st, health
}

func (c *ClickHouseTSDB) bestEffortStatus() BestEffortStatus {
	var st BestEffortStatus

	st.Dropping = c.dropping.Load()

	var m dto.Metric
	if err := c.metrics.WALDroppedPointsTotal.Write(&m); err == nil {
		st.DroppedPoints = m.GetCounter().GetValue()
	}

	c.dropMu.Lock()
	defer c.dropMu.Unlock()

	if c.dropGlobalMax != 0 {
		from, to := time.UnixMilli(c.dropGlobalMin), time.UnixMilli(c.dropGlobalMax)
		st.DroppedFrom, st.DroppedTo = &from, &to
	}

	return st
}

func timeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}

	return &t
}

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

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

// newStatusTestTSDB returns a TSDB with an in-memory WAL (soft threshold 2 points)
// and a drainer that is not running.
func newStatusTestTSDB() *ClickHouseTSDB {
	walConn := newMemWALClient()
	wal := &walStore{client: walConn, key: "wal"}
	m := newMetrics(prometheus.NewRegistry())

	c := &ClickHouseTSDB{
		walConn:     walConn,
		wal:         wal,
		metrics:     m,
		logger:      zerolog.Nop(),
		walCritical: 2 * walCriticalFactor,
		walStop:     2 * walStopFactor,
	}

	c.drainer = newTestDrainer(wal, func(context.Context, []walEntry) error { return nil }, 10)
	c.drainer.flushDelay = time.Second
	c.drainer.metrics = m

	return c
}

// appendPoints appends n single-point entries to the WAL.
func appendPoints(ctx context.Context, t *testing.T, w *walStore, n int) {
	t.Helper()

	entries := make([]walEntry, n)
	for i := range entries {
		entries[i] = walEntry{
			Labels: labels.FromStrings("__name__", "m"),
			Points: []types.MetricPoint{{Timestamp: int64(i)}},
		}
	}

	if _, err := w.Append(ctx, entries); err != nil {
		t.Fatal(err)
	}
}

func TestWALStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("healthy", func(t *testing.T) {
		t.Parallel()

		c := newStatusTestTSDB()
		appendPoints(ctx, t, c.wal, 1)

		st, health := c.WALStatus(ctx)
		if health.Status != types.StatusOK {
			t.Errorf("health = %+v, want ok", health)
		}

		if st.Storage != "memory" || st.Length == nil || *st.Length != 1 {
			t.Errorf("status = %+v, want an in-memory WAL of 1 point", st)
		}

		if st.SoftThreshold != 2 || st.CriticalThreshold != 6 || st.StopThreshold != 20 {
			t.Errorf("thresholds = %d/%d/%d, want 2/6/20", st.SoftThreshold, st.CriticalThreshold, st.StopThreshold)
		}

		if st.Drainer == nil || st.Drainer.Leader || st.Drainer.LastDrain != nil {
			t.Errorf("drainer = %+v, want a non-leader that never drained", st.Drainer)
		}
	})

	t.Run("above the stop threshold", func(t *testing.T) {
		t.Parallel()

		c := newStatusTestTSDB()
		appendPoints(ctx, t, c.wal, 20)

		_, health := c.WALStatus(ctx)
		if health.Status != types.StatusDegraded || !strings.Contains(health.Reason, "writes are blocked") {
			t.Errorf("health = %+v, want degraded because writes are blocked", health)
		}
	})

	t.Run("stalled drainer", func(t *testing.T) {
		t.Parallel()

		c := newStatusTestTSDB()
		c.drainer.leader.Store(true)
		c.drainer.lastDrained.Store(time.Now().Add(-time.Minute).UnixNano())
		c.drainer.noteError("write WAL batch to ClickHouse", errors.New("connection refused"))

		st, health := c.WALStatus(ctx)
		if health.Status != types.StatusDegraded || !strings.Contains(health.Reason, "connection refused") {
			t.Errorf("health = %+v, want degraded with the last drain error", health)
		}

		if st.Drainer == nil || !st.Drainer.Leader || st.Drainer.LastDrain == nil || st.Drainer.LastErrorAt == nil {
			t.Errorf("drainer = %+v, want the leader with its last drain and error", st.Drainer)
		}
	})

	t.Run("best-effort drops", func(t *testing.T) {
		t.Parallel()

		c := newStatusTestTSDB()
		c.accountDrop([]walEntry{{
			Labels: labels.FromStrings("__name__", "m"),
			Points: []types.MetricPoint{{Timestamp: 1000}, {Timestamp: 5000}},
		}})

		st, health := c.WALStatus(ctx)
		if health.Status != types.StatusDegraded || !strings.Contains(health.Reason, "dropped") {
			t.Errorf("health = %+v, want degraded because points are dropped", health)
		}

		be := st.BestEffort
		if !be.Dropping || be.DroppedPoints != 2 || be.DroppedFrom == nil || be.DroppedFrom.UnixMilli() != 1000 ||
			be.DroppedTo == nil || be.DroppedTo.UnixMilli() != 5000 {
			t.Errorf("best effort = %+v, want 2 points dropped over [1s, 5s]", be)
		}
	})

	t.Run("read-only", func(t *testing.T) {
		t.Parallel()

		c := newStatusTestTSDB()
		c.drainer = nil

		st, health := c.WALStatus(ctx)
		if health.Status != types.StatusOK || st.Drainer != nil {
			t.Errorf("status = %+v, health = %+v, want ok without drainer", st, health)
		}
	})
}

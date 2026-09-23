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
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/model/labels"
)

type fakeRow struct {
	id    uuid.UUID
	ts    time.Time
	value float64
}

// fakeRows is a minimal driver.Rows over a fixed slice, counting Close calls so
// tests can assert the cursor is released exactly once.
type fakeRows struct {
	rows       []fakeRow
	idx        int
	cur        fakeRow
	closeCalls int
}

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}

	r.cur = r.rows[r.idx]
	r.idx++

	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	*dest[0].(*uuid.UUID) = r.cur.id  //nolint:forcetypeassert
	*dest[1].(*time.Time) = r.cur.ts  //nolint:forcetypeassert
	*dest[2].(*float64) = r.cur.value //nolint:forcetypeassert

	return nil
}

func (r *fakeRows) Close() error {
	r.closeCalls++

	return nil
}

func (r *fakeRows) Err() error                       { return nil }
func (r *fakeRows) ScanStruct(any) error             { return nil }
func (r *fakeRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *fakeRows) Totals(...any) error              { return nil }
func (r *fakeRows) Columns() []string                { return nil }
func (r *fakeRows) HasData() bool                    { return r.idx < len(r.rows) }

func twoSeriesRows() (*fakeRows, map[uuid.UUID]labels.Labels) {
	id1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	rows := &fakeRows{rows: []fakeRow{
		{id: id1, ts: time.UnixMilli(1000), value: 1},
		{id: id2, ts: time.UnixMilli(2000), value: 2},
	}}

	byID := map[uuid.UUID]labels.Labels{
		id1: labels.FromStrings("__name__", "a"),
		id2: labels.FromStrings("__name__", "b"),
	}

	return rows, byID
}

// TestSeriesSetClosesCursorOnDrain checks that a fully-drained sqlSeriesSet closes
// its cursor exactly once (readRow's self-close), and that a further close() is a
// no-op. This self-close is what releases the routing read slot when Select drains
// the set (see materialize), so the property must hold.
func TestSeriesSetClosesCursorOnDrain(t *testing.T) {
	rows, byID := twoSeriesRows()
	set := &sqlSeriesSet{rows: rows, labelsByID: byID}

	for set.Next() {
	}

	if err := set.Err(); err != nil {
		t.Fatalf("set.Err: %v", err)
	}

	if rows.closeCalls != 1 {
		t.Fatalf("cursor close calls after drain = %d, want 1", rows.closeCalls)
	}

	// A redundant close (e.g. an abandoned set closed by its owner) must not double-close.
	set.close()

	if rows.closeCalls != 1 {
		t.Fatalf("cursor close calls after drain + close = %d, want 1 (double close)", rows.closeCalls)
	}
}

// TestSeriesSetCloseReleasesOpenCursor checks that close() releases a cursor that
// was only partially consumed, so an abandoned set never leaks its cursor.
func TestSeriesSetCloseReleasesOpenCursor(t *testing.T) {
	rows, byID := twoSeriesRows()
	set := &sqlSeriesSet{rows: rows, labelsByID: byID}

	if !set.Next() {
		t.Fatal("expected at least one series")
	}

	if rows.closeCalls != 0 {
		t.Fatalf("cursor closed too early: %d", rows.closeCalls)
	}

	set.close()

	if rows.closeCalls != 1 {
		t.Fatalf("cursor close calls after close = %d, want 1", rows.closeCalls)
	}
}

// fakeConn is a driver.Conn stub for Select: it answers resolveSeries (the
// tsdb_tags query) and the point read (the tsdb_data query) with canned rows and
// counts Close on the point cursor. Every other driver.Conn method is unused by
// Select and panics via the nil embedded interface if ever called.
type fakeConn struct {
	driver.Conn

	tagsRows  driver.Rows
	pointRows *fakeRows
}

func (c *fakeConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if strings.Contains(query, "tsdb_data") {
		return c.pointRows, nil
	}

	return c.tagsRows, nil
}

// fakeTagsRows answers resolveSeries: one row per series, scanned as
// (id, metric_name, tenant, tags).
type fakeTagsRows struct {
	*fakeRows

	names []string
}

func (r *fakeTagsRows) Scan(dest ...any) error {
	*dest[0].(*uuid.UUID) = r.cur.id                    //nolint:forcetypeassert
	*dest[1].(*string) = r.names[r.idx-1]               //nolint:forcetypeassert
	*dest[2].(*string) = ""                             //nolint:forcetypeassert
	*dest[3].(*map[string]string) = map[string]string{} //nolint:forcetypeassert

	return nil
}

// TestSelectMaterializesAndReleasesCursor is the regression test for the routing
// read-limiter deadlock: Select must drain and close its point cursor BEFORE
// returning, so a querier never holds a read slot across selectors. It asserts the
// cursor is closed by the time Select returns and that the returned set is a fully
// iterable in-memory copy.
func TestSelectMaterializesAndReleasesCursor(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	pointRows := &fakeRows{rows: []fakeRow{
		{id: id, ts: time.UnixMilli(1000), value: 1},
		{id: id, ts: time.UnixMilli(2000), value: 2},
	}}
	tagsRows := &fakeTagsRows{
		fakeRows: &fakeRows{rows: []fakeRow{{id: id}}},
		names:    []string{"a"},
	}

	q := &sqlQuerier{
		conn:    &fakeConn{tagsRows: tagsRows, pointRows: pointRows},
		mint:    0,
		maxt:    10000,
		metrics: newMetrics(nil),
		onClose: func() {},
	}

	set := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "a"))
	if err := set.Err(); err != nil {
		t.Fatalf("Select: %v", err)
	}

	// The point cursor must already be closed — the read slot is released before the
	// caller iterates. This is the invariant that prevents the hold-and-wait deadlock.
	if pointRows.closeCalls != 1 {
		t.Fatalf("point cursor close calls after Select = %d, want 1 (Select must drain before returning)",
			pointRows.closeCalls)
	}

	var series int

	for set.Next() {
		series++
	}

	if err := set.Err(); err != nil {
		t.Fatalf("iterate set: %v", err)
	}

	if series != 1 {
		t.Fatalf("series count = %d, want 1", series)
	}
}

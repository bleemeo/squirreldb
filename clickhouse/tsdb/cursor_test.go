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

// TestSeriesSetClosedOnAbandon checks that a querier abandoned mid-iteration
// still releases its cursor on Close (the early-abandonment leak).
func TestSeriesSetClosedOnAbandon(t *testing.T) {
	rows, byID := twoSeriesRows()
	set := &sqlSeriesSet{rows: rows, labelsByID: byID}

	q := &sqlQuerier{onClose: func() {}}
	q.trackSet(set)

	// Consume only the first series, then abandon the rest (cursor still open).
	if !set.Next() {
		t.Fatal("expected at least one series")
	}

	if rows.closeCalls != 0 {
		t.Fatalf("cursor closed too early: %d", rows.closeCalls)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("querier Close: %v", err)
	}

	if rows.closeCalls != 1 {
		t.Fatalf("cursor close calls after abandon = %d, want 1", rows.closeCalls)
	}
}

// TestSeriesSetClosedOnDrainIsIdempotent checks a fully-drained cursor is closed
// once by readRow, and the querier's Close does not close it a second time.
func TestSeriesSetClosedOnDrainIsIdempotent(t *testing.T) {
	rows, byID := twoSeriesRows()
	set := &sqlSeriesSet{rows: rows, labelsByID: byID}

	q := &sqlQuerier{onClose: func() {}}
	q.trackSet(set)

	for set.Next() {
	}

	if err := set.Err(); err != nil {
		t.Fatalf("set.Err: %v", err)
	}

	if rows.closeCalls != 1 {
		t.Fatalf("cursor close calls after drain = %d, want 1", rows.closeCalls)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("querier Close: %v", err)
	}

	if rows.closeCalls != 1 {
		t.Fatalf("cursor close calls after drain + Close = %d, want 1 (double close)", rows.closeCalls)
	}
}

// TestMultipleSelectsAllClosedOnClose checks that a querier Select-ed several
// times does not close an earlier set's cursor when a later Select runs, and
// that Close releases every set's cursor. This guards the storage.Querier
// contract: multiple SeriesSets from one querier may be alive at once.
func TestMultipleSelectsAllClosedOnClose(t *testing.T) {
	rows1, byID := twoSeriesRows()
	rows2, _ := twoSeriesRows()

	set1 := &sqlSeriesSet{rows: rows1, labelsByID: byID}
	set2 := &sqlSeriesSet{rows: rows2, labelsByID: byID}

	q := &sqlQuerier{onClose: func() {}}

	// Two Selects on the same querier, both sets still in flight.
	q.trackSet(set1)
	q.trackSet(set2)

	// Tracking the second set must NOT have closed the first.
	if rows1.closeCalls != 0 {
		t.Fatalf("first set's cursor closed by a later Select: %d", rows1.closeCalls)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("querier Close: %v", err)
	}

	if rows1.closeCalls != 1 || rows2.closeCalls != 1 {
		t.Fatalf("cursor close calls after Close = (%d, %d), want (1, 1)", rows1.closeCalls, rows2.closeCalls)
	}
}

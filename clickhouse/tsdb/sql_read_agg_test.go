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
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/storage"
)

func TestBucketGrid(t *testing.T) {
	bucketMs := aggBucket.Milliseconds()

	// The grid is anchored on the Unix epoch.
	if got := floorBucket(0); got != 0 {
		t.Errorf("floorBucket(epoch) = %d, want 0", got)
	}

	if got := ceilBucket(0); got != 0 {
		t.Errorf("ceilBucket(epoch) = %d, want 0", got)
	}

	// An aligned value is unchanged; a mid-bucket value floors down and ceils up by
	// one bucket.
	if got := floorBucket(bucketMs); got != bucketMs {
		t.Errorf("floorBucket(aligned) = %d, want %d", got, bucketMs)
	}

	mid := bucketMs + bucketMs/2
	if got := floorBucket(mid); got != bucketMs {
		t.Errorf("floorBucket(mid) = %d, want %d", got, bucketMs)
	}

	if got := ceilBucket(mid); got != 2*bucketMs {
		t.Errorf("ceilBucket(mid) = %d, want %d", got, 2*bucketMs)
	}
}

func TestAlignedChunks(t *testing.T) {
	t.Parallel()

	// start is aligned with partition (and with bucket)
	start := time.UnixMilli(floorBucket(time.Date(2024, 6, 6, 0, 0, 0, 0, time.UTC).UnixMilli()))

	cases := []struct {
		name     string
		from, to time.Time
		chunk    time.Duration
		want     [][2]int64
	}{
		{
			name:  "default chunk tiles the span",
			from:  start,
			to:    start.Add(3 * defaultRebuildChunk),
			chunk: 0,
			want: [][2]int64{
				{start.UnixMilli(), (start.Add(defaultRebuildChunk)).UnixMilli()},
				{start.Add(defaultRebuildChunk).UnixMilli(), (start.Add(2 * defaultRebuildChunk)).UnixMilli()},
				{start.Add(2 * defaultRebuildChunk).UnixMilli(), (start.Add(3 * defaultRebuildChunk)).UnixMilli()},
			},
		},
		{
			// from floors down to a bucket, to ceils up to one (each by <= one bucket).
			name:  "bounds snap to the bucket grid",
			from:  start.Add(5 * time.Minute),
			to:    start.Add(defaultRebuildChunk + 5*time.Minute),
			chunk: 0,
			want: [][2]int64{
				// floor for lower bound
				{start.UnixMilli(), start.Add(defaultRebuildChunk).UnixMilli()},
				// and ceil for upper bound
				{start.Add(defaultRebuildChunk).UnixMilli(), (start.Add(defaultRebuildChunk + aggBucket)).UnixMilli()},
			},
		},
		{
			name:  "sub-chunk windows tile the span",
			from:  start,
			to:    start.Add(defaultRebuildChunk),
			chunk: 24 * time.Hour,
			want: [][2]int64{
				{start.UnixMilli(), start.Add(24 * time.Hour).UnixMilli()},
				{start.Add(24 * time.Hour).UnixMilli(), start.Add(2 * 24 * time.Hour).UnixMilli()},
				{start.Add(2 * 24 * time.Hour).UnixMilli(), start.Add(3 * 24 * time.Hour).UnixMilli()},
				{start.Add(3 * 24 * time.Hour).UnixMilli(), start.Add(4 * 24 * time.Hour).UnixMilli()},
				{start.Add(4 * 24 * time.Hour).UnixMilli(), start.Add(5 * 24 * time.Hour).UnixMilli()},
				{start.Add(5 * 24 * time.Hour).UnixMilli(), start.Add(6 * 24 * time.Hour).UnixMilli()},
				// 7 * 24h is defaultRebuildChunk
				{start.Add(6 * 24 * time.Hour).UnixMilli(), start.Add(7 * 24 * time.Hour).UnixMilli()},
			},
		},
		{
			name: "less-than-one-partition-period",
			from: start.Add(60 * time.Minute),
			to:   start.Add(72 * time.Minute),
			want: [][2]int64{
				{start.Add(60 * time.Minute).UnixMilli(), start.Add(75 * time.Minute).UnixMilli()},
			},
		},
		{
			name: "two-partial-parts",
			from: start.Add(-15 * time.Minute),
			to:   start.Add(15 * time.Minute),
			want: [][2]int64{
				{start.Add(-15 * time.Minute).UnixMilli(), start.UnixMilli()},
				{start.UnixMilli(), start.Add(15 * time.Minute).UnixMilli()},
			},
		},
		{
			name: "first-and-last-partial-part",
			from: start.Add(-15 * time.Minute),
			to:   start.Add(15*time.Minute + aggPartition),
			want: [][2]int64{
				{start.Add(-15 * time.Minute).UnixMilli(), start.UnixMilli()},
				{start.UnixMilli(), start.Add(aggPartition).UnixMilli()},
				{start.Add(aggPartition).UnixMilli(), start.Add(aggPartition + 15*time.Minute).UnixMilli()},
			},
		},
		{
			name:  "multiple-part-small-chunk",
			from:  start,
			to:    start.Add(10 * 24 * time.Hour),
			chunk: 48 * time.Hour,
			want: [][2]int64{
				{start.UnixMilli(), start.Add(2 * 24 * time.Hour).UnixMilli()},
				{start.Add(2 * 24 * time.Hour).UnixMilli(), start.Add(4 * 24 * time.Hour).UnixMilli()},
				{start.Add(4 * 24 * time.Hour).UnixMilli(), start.Add(6 * 24 * time.Hour).UnixMilli()},
				// Careful here, one window shouldn't cross start + 7d, which is a *partition* boundary
				{start.Add(6 * 24 * time.Hour).UnixMilli(), start.Add(7 * 24 * time.Hour).UnixMilli()},
				{start.Add(7 * 24 * time.Hour).UnixMilli(), start.Add(9 * 24 * time.Hour).UnixMilli()},
				{start.Add(9 * 24 * time.Hour).UnixMilli(), start.Add(10 * 24 * time.Hour).UnixMilli()},
			},
		},
		{
			name:  "one-part-small-chunk",
			from:  start.Add(3 * 24 * time.Hour),
			to:    start.Add(6 * 24 * time.Hour),
			chunk: 24 * time.Hour,
			want: [][2]int64{
				{start.Add(3 * 24 * time.Hour).UnixMilli(), start.Add(4 * 24 * time.Hour).UnixMilli()},
				{start.Add(4 * 24 * time.Hour).UnixMilli(), start.Add(5 * 24 * time.Hour).UnixMilli()},
				{start.Add(5 * 24 * time.Hour).UnixMilli(), start.Add(6 * 24 * time.Hour).UnixMilli()},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			windows := alignedChunks(tc.from, tc.to, tc.chunk)
			if diff := cmp.Diff(tc.want, windows); diff != "" {
				t.Errorf("windows mismatch (-want +got)\n%s", diff)
			}

			for i, win := range windows {
				// Start & end of window fall in the same partition
				startPart := ceilPartition(win[0] + 1) // +1, remember that interval is open on left-side
				endPart := ceilPartition(win[1])

				if startPart != endPart {
					t.Errorf("window %d is start/end not in same partition: %v & %v", i, startPart, endPart)
				}

				if win[1] <= win[0] {
					t.Errorf("window %d is empty/negative: %v", i, win)
				}

				if i > 0 && win[0] != windows[i-1][1] {
					t.Errorf("window %d not contiguous with previous: %v vs %v", i, win, windows[i-1])
				}

				wantSize := tc.chunk
				if wantSize == 0 {
					wantSize = defaultRebuildChunk
				}

				if win[1]-win[0] > wantSize.Milliseconds() {
					t.Errorf("window %d size = %d ms, want less than %d ms", i, win[1]-win[0], wantSize.Milliseconds())
				}
			}
		})
	}
}

func TestAggValueExpr(t *testing.T) {
	cases := []struct {
		fn       string
		wantExpr string
		wantOK   bool
	}{
		{"min_over_time", "min(min)", true},
		{"max_over_time", "max(max)", true},
		{"sum_over_time", "sum(sum)", true},
		{"avg_over_time", "sum(sum) / sum(count)", true},
		{"last_over_time", "argMinMerge(last)", true},
		{"stddev_over_time", "max(max)", true},
		// Counters and trends reconstruct the bucket's last cumulative value.
		{"rate", "max(max)", true},
		{"increase", "max(max)", true},
		// count_over_time counts points, not values: the downsampled series has far
		// fewer points than raw, so it must read raw.
		{"count_over_time", "", false},
		// present/absent_over_time depend only on presence, preserved by the bucketed
		// series, so they are served from the aggregate.
		{"present_over_time", "max(max)", true},
		{"absent_over_time", "max(max)", true},
		// Distribution / order functions can't be served by min/max/sum/count.
		{"quantile_over_time", "", false},
		{"resets", "", false},
		{"changes", "", false},
		// Fallback is aggregated with average
		{"holt_winters", "sum(sum) / sum(count)", true},
		{"new_unknown_function", "sum(sum) / sum(count)", true},
		// We don't test bare selector and the instant aggregation operators, since they shouldn't be
		// routed to aggValueExpr (since only Range function are).
	}

	for _, tc := range cases {
		t.Run(tc.fn, func(t *testing.T) {
			expr, ok := aggValueExpr(tc.fn)
			if expr != tc.wantExpr || ok != tc.wantOK {
				t.Errorf("aggValueExpr(%q) = (%q, %v), want (%q, %v)", tc.fn, expr, ok, tc.wantExpr, tc.wantOK)
			}
		})
	}
}

func TestAggSelection(t *testing.T) {
	bigStep := aggMinStep.Milliseconds()       // reaches the routing threshold
	smallStep := aggMinStep.Milliseconds() - 1 // just below

	cases := []struct {
		name     string
		q        *sqlQuerier
		hints    *storage.SelectHints
		wantMode aggMode
		wantExpr string
	}{
		{
			name:     "force raw wins over everything",
			q:        &sqlQuerier{forceRaw: true},
			hints:    &storage.SelectHints{Step: bigStep, Range: (2 * time.Hour).Milliseconds(), Func: "avg_over_time"},
			wantMode: aggModeRaw,
		},
		{
			name:     "range function at the threshold routes to the aggregate",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: bigStep, Range: (2 * time.Hour).Milliseconds(), Func: "max_over_time"},
			wantMode: aggModeRange,
			wantExpr: "max(max)",
		},
		{
			name:     "step below the threshold reads raw",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: smallStep, Range: (2 * time.Hour).Milliseconds(), Func: "max_over_time"},
			wantMode: aggModeRaw,
		},
		{
			name:     "range narrower than two buckets reads raw",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: bigStep, Range: (20 * time.Second).Milliseconds(), Func: "rate"},
			wantMode: aggModeRaw,
		},
		{
			name:     "range of at least two buckets routes to the aggregate",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: bigStep, Range: (2 * time.Hour).Milliseconds(), Func: "rate"},
			wantMode: aggModeRange,
			wantExpr: "max(max)",
		},
		{
			// An instant query (Step == 0) over a large [range] must use the aggregate,
			// not read the whole raw window — the case force-pre-aggregated used to cover.
			name:     "instant query with a large range routes to the aggregate",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: 0, Range: (740 * time.Hour).Milliseconds(), Func: "avg_over_time"},
			wantMode: aggModeRange,
			wantExpr: "sum(sum) / sum(count)",
		},
		{
			name:     "instant query with a sub-two-bucket range reads raw",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: 0, Range: (20 * time.Second).Milliseconds(), Func: "avg_over_time"},
			wantMode: aggModeRaw,
		},
		{
			name:     "instant query bare selector reads raw",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: 0, Range: 0, Func: ""},
			wantMode: aggModeRaw,
		},
		{
			name:     "bare instant selector (range 0) snaps to the eval grid",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: bigStep, Range: 0, Func: ""},
			wantMode: aggModeInstantSnap,
		},
		{
			name:     "instant aggregation operator (range 0) snaps to the eval grid",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: bigStep, Range: 0, Func: "sum"},
			wantMode: aggModeInstantSnap,
		},
		{
			name:     "nil hints reads raw",
			q:        &sqlQuerier{},
			hints:    nil,
			wantMode: aggModeRaw,
		},
		{
			name:     "large step but non-aggregatable range function reads raw",
			q:        &sqlQuerier{},
			hints:    &storage.SelectHints{Step: bigStep, Range: (2 * time.Hour).Milliseconds(), Func: "changes"},
			wantMode: aggModeRaw,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := tc.q.aggSelection(tc.hints)
			if plan.mode != tc.wantMode {
				t.Fatalf("aggSelection mode = %v, want %v", plan.mode, tc.wantMode)
			}

			if plan.mode == aggModeRange && plan.expr != tc.wantExpr {
				t.Errorf("aggSelection expr = %q, want %q", plan.expr, tc.wantExpr)
			}
		})
	}
}

func TestAggPointsQuery(t *testing.T) {
	where := "metric_name = ?"
	whereArgs := []any{"cpu_used"}
	oneID := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001")}

	t.Run("tenant-scoped, small id set, groups by bucket", func(t *testing.T) {
		q := &sqlQuerier{labelQuerier: labelQuerier{tenant: "tenant-1", mint: 5, maxt: 9}}

		query, args := q.aggPointsQuery(oneID, "max(max)", where, whereArgs)

		for _, want := range []string{"FROM tsdb_data_agg_15m", "max(max) AS value", "id IN (?)", "GROUP BY id, bucket"} {
			if !strings.Contains(query, want) {
				t.Errorf("query missing %q, got:\n%s", want, query)
			}
		}

		want := []any{"tenant-1", oneID, clampStoreTime(5), clampStoreTime(9)}
		if diff := cmp.Diff(want, args); diff != "" {
			t.Errorf("args mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("id set over the threshold falls back to a subquery", func(t *testing.T) {
		q := &sqlQuerier{labelQuerier: labelQuerier{tenant: "tenant-1", mint: 5, maxt: 9}}
		bigIDs := make([]uuid.UUID, pointsInlineThreshold+1)

		query, args := q.aggPointsQuery(bigIDs, "max(max)", where, whereArgs)

		if !strings.Contains(query, "id IN (SELECT id FROM tsdb_tags WHERE "+where+")") {
			t.Errorf("query should use the subquery fallback, got:\n%s", query)
		}

		want := []any{"tenant-1", "cpu_used", clampStoreTime(5), clampStoreTime(9)}
		if diff := cmp.Diff(want, args); diff != "" {
			t.Errorf("args mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("unscoped query omits the tenant predicate", func(t *testing.T) {
		q := &sqlQuerier{labelQuerier: labelQuerier{tenant: "", mint: 5, maxt: 9}}

		query, args := q.aggPointsQuery(oneID, "max(max)", where, whereArgs)

		if strings.Contains(query, "tenant = ?") {
			t.Errorf("unscoped query must not filter tenant, got:\n%s", query)
		}

		want := []any{oneID, clampStoreTime(5), clampStoreTime(9)}
		if diff := cmp.Diff(want, args); diff != "" {
			t.Errorf("args mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestAggSnapQuery(t *testing.T) {
	where := "metric_name = ?"
	whereArgs := []any{"cpu_used"}
	oneID := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001")}
	bucketMs := aggBucket.Milliseconds()

	// A range query: Start .. End every Step, well inside the store bounds. Start is
	// deliberately not a multiple of Step (as a lookback-shifted hints.Start is), so
	// the two grid anchors differ.
	step := bucketMs
	start := int64(10_000_000)
	end := start + 4*bucketMs
	alignedStart := start - start%step + step

	t.Run("tenant-scoped snaps to the eval grid and matches the covering bucket", func(t *testing.T) {
		q := &sqlQuerier{labelQuerier: labelQuerier{tenant: "tenant-1"}}
		hints := &storage.SelectHints{Start: start, End: end, Step: step, Range: 0}

		query, args := q.aggSnapQuery(oneID, hints, where, whereArgs)

		for _, want := range []string{
			"argMinMerge(a.last) AS value",
			// The grid is the RIGHT (build) side of the join, big table on the left.
			"FROM tsdb_data_agg_15m AS a INNER JOIN (",
			"AS grid ON a.bucket = grid.bucket",
			"toStartOfInterval(ts, INTERVAL 15 MINUTE)",
			// Both grids, deduplicated: the Start-anchored one and the step-aligned one.
			"arrayDistinct(arrayConcat(range(toUInt64(?), toUInt64(?), toUInt64(?)), " +
				"range(toUInt64(?), toUInt64(?), toUInt64(?))))",
			"a.id IN (?)",
			"GROUP BY a.id, grid.ts",
		} {
			if !strings.Contains(query, want) {
				t.Errorf("query missing %q, got:\n%s", want, query)
			}
		}

		// Both grid bounds (Start then the aligned start, each to End+1 by Step), tenant,
		// ids, then the coarse bucket filter (Start - one bucket, End), all clamped where
		// they hit the store bounds.
		want := []any{
			start, end + 1, step,
			alignedStart, end + 1, step,
			"tenant-1", oneID,
			clampStoreTime(start - bucketMs), clampStoreTime(end),
		}
		if diff := cmp.Diff(want, args); diff != "" {
			t.Errorf("args mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a step-aligned Start yields two identical grids", func(t *testing.T) {
		q := &sqlQuerier{labelQuerier: labelQuerier{tenant: ""}}
		hints := &storage.SelectHints{Start: alignedStart, End: end, Step: step, Range: 0}

		_, args := q.aggSnapQuery(oneID, hints, where, whereArgs)

		want := []any{
			alignedStart, end + 1, step,
			alignedStart, end + 1, step,
			oneID,
			clampStoreTime(alignedStart - bucketMs), clampStoreTime(end),
		}
		if diff := cmp.Diff(want, args); diff != "" {
			t.Errorf("args mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("id set over the threshold falls back to a subquery", func(t *testing.T) {
		q := &sqlQuerier{labelQuerier: labelQuerier{tenant: "tenant-1"}}
		hints := &storage.SelectHints{Start: start, End: end, Step: step, Range: 0}
		bigIDs := make([]uuid.UUID, pointsInlineThreshold+1)

		query, args := q.aggSnapQuery(bigIDs, hints, where, whereArgs)

		if !strings.Contains(query, "a.id IN (SELECT id FROM tsdb_tags WHERE "+where+")") {
			t.Errorf("query should use the subquery fallback, got:\n%s", query)
		}

		want := []any{
			start, end + 1, step,
			alignedStart, end + 1, step,
			"tenant-1", "cpu_used",
			clampStoreTime(start - bucketMs), clampStoreTime(end),
		}
		if diff := cmp.Diff(want, args); diff != "" {
			t.Errorf("args mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("unscoped query omits the tenant predicate", func(t *testing.T) {
		q := &sqlQuerier{labelQuerier: labelQuerier{tenant: ""}}
		hints := &storage.SelectHints{Start: start, End: end, Step: step, Range: 0}

		query, args := q.aggSnapQuery(oneID, hints, where, whereArgs)

		if strings.Contains(query, "a.tenant = ?") {
			t.Errorf("unscoped query must not filter tenant, got:\n%s", query)
		}

		want := []any{
			start, end + 1, step,
			alignedStart, end + 1, step,
			oneID,
			clampStoreTime(start - bucketMs), clampStoreTime(end),
		}
		if diff := cmp.Diff(want, args); diff != "" {
			t.Errorf("args mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestAlignUp(t *testing.T) {
	cases := []struct {
		v, step, want int64
	}{
		{v: 0, step: 100, want: 0},
		{v: 100, step: 100, want: 100},
		{v: 1, step: 100, want: 100},
		{v: 99, step: 100, want: 100},
		{v: 101, step: 100, want: 200},
		// Pre-1970 hints (storeMinTimeMillis is negative): still at/after v.
		{v: -1, step: 100, want: 0},
		{v: -100, step: 100, want: -100},
		{v: -101, step: 100, want: -100},
	}

	for _, c := range cases {
		if got := alignUp(c.v, c.step); got != c.want {
			t.Errorf("alignUp(%d, %d) = %d, want %d", c.v, c.step, got, c.want)
		}
	}
}

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

package batch

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/dummy/temporarystore"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	metricIDTest1 = types.MetricID(1)
	metricIDTest2 = types.MetricID(2)
)

// recordingWriter accumulates all Write() calls; Data can be reset to nil between assertions.
type recordingWriter struct {
	mu   sync.Mutex
	Data []types.MetricData
}

func (w *recordingWriter) Write(_ context.Context, metrics []types.MetricData) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.Data = append(w.Data, metrics...)

	return nil
}

func newMemoryStore(initialData []types.MetricData) *temporarystore.Store {
	store := temporarystore.New(prometheus.NewRegistry(), log.With().Str("component", "temporary_store").Logger())
	_, _ = store.Append(context.Background(), initialData)

	return store
}

func newMemoryStoreOffset(initialData []types.MetricData, offsets []int) *temporarystore.Store {
	store := temporarystore.New(prometheus.NewRegistry(), log.With().Str("component", "temporary_store").Logger())
	_, _ = store.GetSetPointsAndOffset(context.Background(), initialData, offsets)

	return store
}

func dumpMemoryStore(ctx context.Context, store TemporaryStore) []types.MetricData {
	idsMap, _ := store.GetAllKnownMetrics(ctx)

	ids := make([]types.MetricID, 0, len(idsMap))

	for id := range idsMap {
		ids = append(ids, id)
	}

	results, _, _ := store.ReadPointsAndOffset(ctx, ids)

	return results
}

func dataEqual(orderMatter bool, a, b []types.MetricData) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}

	if orderMatter {
		return reflect.DeepEqual(a, b)
	}

	aCopy := make([]types.MetricData, len(a))
	copy(aCopy, a)

	bCopy := make([]types.MetricData, len(b))
	copy(bCopy, b)

	sort.Slice(aCopy, func(i, j int) bool { return aCopy[i].ID < aCopy[j].ID })
	sort.Slice(bCopy, func(i, j int) bool { return bCopy[i].ID < bCopy[j].ID })

	return reflect.DeepEqual(aCopy, bCopy)
}

func newBatch(memoryStore TemporaryStore) *Batch {
	return New(
		prometheus.NewRegistry(),
		300*time.Second,
		memoryStore,
		log.With().Str("component", "batch").Logger(),
	)
}

func generatePoint(fromTS, toTS, step int) []types.MetricPoint { //nolint:unparam
	points := make([]types.MetricPoint, 0)

	for ts := fromTS; ts <= toTS; ts += step {
		points = append(points, types.MetricPoint{Timestamp: int64(ts) * 1000})
	}

	return points
}

func Test_flushTimestamp(t *testing.T) {
	const MetricIDTest1 = 1

	type args struct {
		now       time.Time
		id        types.MetricID
		batchSize time.Duration
	}

	tests := []struct {
		want time.Time
		name string
		args args
	}{
		{
			name: "id_1",
			args: args{
				id:        MetricIDTest1,
				now:       time.Unix(0, 0),
				batchSize: 50 * time.Second,
			},
			want: time.Unix(49, 0),
		},
		{
			name: "id_1707",
			args: args{
				id:        1707,
				now:       time.Unix(0, 0),
				batchSize: 50 * time.Second,
			},
			want: time.Unix(43, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := flushTimestamp(tt.args.id, tt.args.now, tt.args.batchSize); got != tt.want {
				t.Errorf("flushTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_randomDuration(t *testing.T) {
	target := 50 * time.Millisecond
	tMin := 40 * time.Millisecond
	tMax := 60 * time.Millisecond

	for range 100 {
		got := randomDuration(target)
		if got < tMin {
			t.Errorf("randomDuration() = %v, want >= %v", got, tMin)
		}

		if tMax < got {
			t.Errorf("randomDuration() = %v, want <= %v", got, tMax)
		}
	}
}

func TestReadTemporary(t *testing.T) {
	tests := []struct {
		memoryStore   TemporaryStore
		name          string
		ids           []types.MetricID
		want          []types.MetricData
		fromTimestamp int64
		toTimestamp   int64
	}{
		{
			name: "points_filtered_by_range",
			memoryStore: newMemoryStore([]types.MetricData{
				{
					ID: metricIDTest1,
					Points: []types.MetricPoint{
						{Timestamp: 50000, Value: 60},
						{Timestamp: 60000, Value: 70},
					},
				},
				{
					ID: metricIDTest2,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 300},
						{Timestamp: 120000, Value: 350},
					},
				},
			}),
			ids:           []types.MetricID{metricIDTest1, metricIDTest2},
			fromTimestamp: 0,
			toTimestamp:   200000,
			want: []types.MetricData{
				{ID: metricIDTest1, Points: []types.MetricPoint{{Timestamp: 50000, Value: 60}, {Timestamp: 60000, Value: 70}}},
				{ID: metricIDTest2, Points: []types.MetricPoint{{Timestamp: 100000, Value: 300}, {Timestamp: 120000, Value: 350}}},
			},
		},
		{
			name: "range_excludes_some_points",
			memoryStore: newMemoryStore([]types.MetricData{
				{
					ID: metricIDTest1,
					Points: []types.MetricPoint{
						{Timestamp: 0, Value: 10},
						{Timestamp: 10000, Value: 20},
						{Timestamp: 20000, Value: 30},
						{Timestamp: 30000, Value: 40},
						{Timestamp: 40000, Value: 50},
					},
				},
			}),
			ids:           []types.MetricID{metricIDTest1},
			fromTimestamp: 1000,
			toTimestamp:   110000,
			want: []types.MetricData{
				{ID: metricIDTest1, Points: []types.MetricPoint{
					{Timestamp: 10000, Value: 20},
					{Timestamp: 20000, Value: 30},
					{Timestamp: 30000, Value: 40},
					{Timestamp: 40000, Value: 50},
				}},
			},
		},
		{
			name:          "empty_store_returns_no_data",
			memoryStore:   newMemoryStore(nil),
			ids:           []types.MetricID{metricIDTest1, metricIDTest2},
			fromTimestamp: 0,
			toTimestamp:   200000,
			want:          nil,
		},
		{
			name: "no_points_in_range_returns_no_data",
			memoryStore: newMemoryStore([]types.MetricData{
				{
					ID:     metricIDTest1,
					Points: []types.MetricPoint{{Timestamp: 0, Value: 10}, {Timestamp: 10000, Value: 20}},
				},
			}),
			ids:           []types.MetricID{metricIDTest1},
			fromTimestamp: 20000,
			toTimestamp:   110000,
			want:          nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBatch(tt.memoryStore)

			var got []types.MetricData

			for _, id := range tt.ids {
				data, err := b.ReadTemporary(t.Context(), id, tt.fromTimestamp, tt.toTimestamp)
				if errors.Is(err, ErrNoData) {
					continue
				}

				if err != nil {
					t.Fatalf("ReadTemporary(%d): %v", id, err)
				}

				if len(data.Points) > 0 {
					got = append(got, data)
				}
			}

			if !dataEqual(true, got, tt.want) {
				t.Errorf("ReadTemporary() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatch_flush(t *testing.T) {
	tests := []struct {
		memoryStore     TemporaryStore
		name            string
		states          map[types.MetricID]stateData
		wantWriter      []types.MetricData
		wantMemoryStore []types.MetricData
		ids             []types.MetricID
		now             time.Time
		batchSize       time.Duration
		shutdown        bool
	}{
		{
			name:      "sorts_before_writing_to_tsdb",
			batchSize: 300 * time.Second,
			states:    map[types.MetricID]stateData{},
			memoryStore: newMemoryStore([]types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 120000, Value: 44}, // out of order
						{Timestamp: 140000, Value: 46},
					},
				},
			}),
			ids: []types.MetricID{metricIDTest1},
			now: time.Unix(150, 0),
			wantWriter: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 120000, Value: 44},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 140000, Value: 46},
					},
				},
			},
			// Memory store retains original order (flush does not reorder in-memory data)
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 120000, Value: 44},
						{Timestamp: 140000, Value: 46},
					},
				},
			},
		},
		{
			name:      "deduplicates_and_sorts_before_writing",
			batchSize: 300 * time.Second,
			states:    map[types.MetricID]stateData{},
			memoryStore: newMemoryStore([]types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 140000, Value: 46},
						{Timestamp: 140000, Value: 46}, // dup
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 120000, Value: 44},
						{Timestamp: 100000, Value: 42}, // dup
					},
				},
			}),
			ids: []types.MetricID{metricIDTest1},
			now: time.Unix(150, 0),
			wantWriter: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 120000, Value: 44},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 140000, Value: 46},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 140000, Value: 46},
						{Timestamp: 140000, Value: 46},
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 120000, Value: 44},
						{Timestamp: 100000, Value: 42},
					},
				},
			},
		},
		{
			name:      "trims_memory_store_to_last_batchsize",
			batchSize: 300 * time.Second,
			states:    map[types.MetricID]stateData{},
			memoryStore: newMemoryStore([]types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 120000, Value: 44},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 140000, Value: 46},
					},
				},
			}),
			ids: []types.MetricID{metricIDTest1},
			now: time.Unix(300+130, 0), // cutoff at t=130s → keep points >= 130000ms
			wantWriter: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 120000, Value: 44},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 140000, Value: 46},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 130000, Value: 45},
						{Timestamp: 140000, Value: 46},
					},
				},
			},
		},
		{
			name:      "respects_write_offset_skips_already_flushed_points",
			batchSize: 300 * time.Second,
			states:    map[types.MetricID]stateData{},
			memoryStore: newMemoryStoreOffset(
				[]types.MetricData{
					{
						ID:         metricIDTest1,
						TimeToLive: 42,
						Points: []types.MetricPoint{
							{Timestamp: 100000, Value: 42},
							{Timestamp: 110000, Value: 43},
							{Timestamp: 120000, Value: 44},
							{Timestamp: 130000, Value: 45},
							{Timestamp: 140000, Value: 46},
						},
					},
				},
				[]int{2}, // first 2 points already written
			),
			ids: []types.MetricID{metricIDTest1},
			now: time.Unix(125, 0),
			wantWriter: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 120000, Value: 44},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 140000, Value: 46},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42},
						{Timestamp: 110000, Value: 43},
						{Timestamp: 120000, Value: 44},
						{Timestamp: 130000, Value: 45},
						{Timestamp: 140000, Value: 46},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := &recordingWriter{}
			b := &Batch{
				batchSize:   tt.batchSize,
				states:      tt.states,
				memoryStore: tt.memoryStore,
				metrics:     newMetrics(prometheus.NewRegistry()),
				logger:      log.With().Logger(),
			}

			if err := b.flush(t.Context(), writer, tt.ids, tt.now, tt.shutdown); err != nil {
				t.Fatalf("flush: %v", err)
			}

			if !reflect.DeepEqual(writer.Data, tt.wantWriter) {
				t.Errorf("writer got = %v, want = %v", writer.Data, tt.wantWriter)
			}

			// Order-insensitive: dumpMemoryStore returns metrics in map-iteration
			// order (GetAllKnownMetrics), which is non-deterministic.
			gotMemoryStore := dumpMemoryStore(t.Context(), tt.memoryStore)
			if !dataEqual(false, gotMemoryStore, tt.wantMemoryStore) {
				t.Errorf("memory store got = %v, want = %v", gotMemoryStore, tt.wantMemoryStore)
			}
		})
	}
}

type batchingTSDB struct {
	writer  *recordingWriter
	batcher *Batch
}

// Test_takeover verifies multi-instance ownership and takeover logic using two Batch
// instances sharing the same TemporaryStore (simulating two SquirrelDB nodes).
func Test_takeover(t *testing.T) {
	batchSize := 100 * time.Second
	memoryStore := temporarystore.New(prometheus.NewRegistry(), log.With().Str("component", "temporary_store").Logger())
	logger1 := log.With().Str("component", "batch1").Logger()
	logger2 := log.With().Str("component", "batch2").Logger()
	writer1 := &batchingTSDB{
		batcher: New(prometheus.NewRegistry(), batchSize, memoryStore, logger1),
		writer:  &recordingWriter{},
	}
	writer2 := &batchingTSDB{
		batcher: New(prometheus.NewRegistry(), batchSize, memoryStore, logger2),
		writer:  &recordingWriter{},
	}
	ctx := t.Context()

	// batch1 owns metric1, batch2 owns metric2; batch1 also receives points for metric2
	for _, step := range []struct {
		wr      *batchingTSDB
		metrics []types.MetricData
		now     time.Time
	}{
		{
			wr:      writer1,
			metrics: []types.MetricData{{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 10000}}}},
			now:     time.Unix(10, 0),
		},
		{
			wr:      writer2,
			metrics: []types.MetricData{{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 10000}}}},
			now:     time.Unix(12, 0),
		},
		{
			wr: writer1,
			metrics: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 20000}}},
				{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 21000}}},
			},
			now: time.Unix(20, 0),
		},
	} {
		if err := step.wr.batcher.write(ctx, step.wr.writer, step.metrics, step.now); err != nil {
			t.Fatal(err)
		}
	}

	// Verify initial state: nothing written yet, correct ownership
	if len(writer1.writer.Data) != 0 {
		t.Errorf("initial writer1: got %v, want empty", writer1.writer.Data)
	}

	if len(writer2.writer.Data) != 0 {
		t.Errorf("initial writer2: got %v, want empty", writer2.writer.Data)
	}

	wantState1 := map[types.MetricID]stateData{
		metricIDTest1: {flushDeadline: flushTimestamp(metricIDTest1, time.Unix(10, 0), batchSize)},
	}
	wantState2 := map[types.MetricID]stateData{
		metricIDTest2: {flushDeadline: flushTimestamp(metricIDTest2, time.Unix(10, 0), batchSize)},
	}

	if !reflect.DeepEqual(writer1.batcher.states, wantState1) {
		t.Errorf("initial state1 = %v, want %v", writer1.batcher.states, wantState1)
	}

	if !reflect.DeepEqual(writer2.batcher.states, wantState2) {
		t.Errorf("initial state2 = %v, want %v", writer2.batcher.states, wantState2)
	}

	wantMemoryStore := []types.MetricData{
		{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 10000}, {Timestamp: 20000}}},
		{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 10000}, {Timestamp: 21000}}},
	}

	if !dataEqual(false, dumpMemoryStore(ctx, memoryStore), wantMemoryStore) {
		t.Errorf("initial memory store = %v, want %v", dumpMemoryStore(ctx, memoryStore), wantMemoryStore)
	}

	writer1.writer.Data = nil
	writer2.writer.Data = nil

	// Phase 1: deadline not overdue yet — batch2 flushes only its own metric2
	now := time.Unix(0, 0).Add(overdueThreshold)
	writer2.batcher.checkTakeover(ctx, now, writer2.writer)

	if err := writer2.batcher.check(ctx, now, writer2.writer, false, false); err != nil {
		t.Fatal(err)
	}

	wantWriter2 := []types.MetricData{
		{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 10000}, {Timestamp: 21000}}},
	}
	wantState2 = map[types.MetricID]stateData{}
	wantMemoryStore = []types.MetricData{
		{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 10000}, {Timestamp: 20000}}},
	}

	if len(writer1.writer.Data) != 0 {
		t.Errorf("phase1 writer1: got %v, want empty", writer1.writer.Data)
	}

	if !reflect.DeepEqual(writer2.writer.Data, wantWriter2) {
		t.Errorf("phase1 writer2 = %v, want %v", writer2.writer.Data, wantWriter2)
	}

	if !reflect.DeepEqual(writer1.batcher.states, wantState1) {
		t.Errorf("phase1 state1 = %v, want %v", writer1.batcher.states, wantState1)
	}

	if !reflect.DeepEqual(writer2.batcher.states, wantState2) {
		t.Errorf("phase1 state2 = %v, want %v", writer2.batcher.states, wantState2)
	}

	if !dataEqual(true, dumpMemoryStore(ctx, memoryStore), wantMemoryStore) {
		t.Errorf("phase1 memory store = %v, want %v", dumpMemoryStore(ctx, memoryStore), wantMemoryStore)
	}

	writer1.writer.Data = nil
	writer2.writer.Data = nil

	// Phase 2: deadline is now overdue — batch2 takes over metric1 and flushes it
	now = time.Unix(100, 0).Add(overdueThreshold)
	writer2.batcher.checkTakeover(ctx, now, writer2.writer)

	if err := writer2.batcher.check(ctx, now, writer2.writer, false, false); err != nil {
		t.Fatal(err)
	}

	wantWriter2 = []types.MetricData{
		{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 10000}, {Timestamp: 20000}}},
	}
	wantMemoryStore = []types.MetricData{}

	if len(writer1.writer.Data) != 0 {
		t.Errorf("phase2 writer1: got %v, want empty", writer1.writer.Data)
	}

	if !reflect.DeepEqual(writer2.writer.Data, wantWriter2) {
		t.Errorf("phase2 writer2 = %v, want %v", writer2.writer.Data, wantWriter2)
	}

	if !reflect.DeepEqual(writer1.batcher.states, wantState1) {
		t.Errorf("phase2 state1 = %v, want %v", writer1.batcher.states, wantState1)
	}

	if !dataEqual(true, dumpMemoryStore(ctx, memoryStore), wantMemoryStore) {
		t.Errorf("phase2 memory store = %v, want %v", dumpMemoryStore(ctx, memoryStore), wantMemoryStore)
	}

	writer1.writer.Data = nil
	writer2.writer.Data = nil

	// Phase 3: batch1 runs check and detects that metric1 was taken over — it clears its state
	if err := writer1.batcher.check(ctx, now, writer1.writer, false, false); err != nil {
		t.Fatal(err)
	}

	wantState1 = map[types.MetricID]stateData{}

	if len(writer1.writer.Data) != 0 {
		t.Errorf("phase3 writer1: got %v, want empty", writer1.writer.Data)
	}

	if len(writer2.writer.Data) != 0 {
		t.Errorf("phase3 writer2: got %v, want empty", writer2.writer.Data)
	}

	if !reflect.DeepEqual(writer1.batcher.states, wantState1) {
		t.Errorf("phase3 state1 = %v, want %v", writer1.batcher.states, wantState1)
	}

	if !dataEqual(true, dumpMemoryStore(ctx, memoryStore), wantMemoryStore) {
		t.Errorf("phase3 memory store = %v, want %v", dumpMemoryStore(ctx, memoryStore), wantMemoryStore)
	}
}

type fakeClock struct {
	l   sync.Mutex
	now time.Time
}

func (c *fakeClock) SetTime(t time.Time) {
	c.l.Lock()
	defer c.l.Unlock()

	c.now = t
}

func (c *fakeClock) GetTime() time.Time {
	c.l.Lock()
	defer c.l.Unlock()

	return c.now
}

// TestBatch_write tests write() + check() behavior across two SquirrelDB instances sharing
// the same TemporaryStore. Steps are sequential and accumulate state across sub-tests.
// Cases that trigger backlog flush (owner or non-owner threshold exceeded) are handled by
// startFlushHandler goroutines so write() can complete synchronously without a running Run().
func TestBatch_write(t *testing.T) { //nolint:maintidx
	batchSize := 100 * time.Second
	memoryStore := temporarystore.New(prometheus.NewRegistry(), log.With().Str("component", "temporary_store").Logger())
	ctx := t.Context()
	clock := new(fakeClock)
	clock.SetTime(time.Now())

	newBatch := func(writer *recordingWriter, logger zerolog.Logger) *batchingTSDB {
		return &batchingTSDB{
			batcher: New(prometheus.NewRegistry(), batchSize, memoryStore, logger),
			writer:  writer,
		}
	}

	renewBatch := func(b *batchingTSDB) *batchingTSDB {
		return newBatch(b.writer, b.batcher.logger)
	}

	batch1 := newBatch(&recordingWriter{}, log.With().Str("component", "batch1").Logger())
	batch2 := newBatch(&recordingWriter{}, log.With().Str("component", "batch2").Logger())

	tests := []struct {
		nowCheck2       time.Time
		nowWrite1       time.Time
		nowWrite2       time.Time
		nowCheck1       time.Time
		wantState1      map[types.MetricID]stateData
		wantState2      map[types.MetricID]stateData
		name            string
		write2          []types.MetricData
		wantWriter1     []types.MetricData
		wantWriter2     []types.MetricData
		wantMemoryStore []types.MetricData
		write1          []types.MetricData
		shutdown1       bool
		shutdown2       bool
	}{
		{
			name:      "single-initial-fill",
			nowWrite1: time.Unix(200, 0),
			write1: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points:     []types.MetricPoint{{Timestamp: 10000}, {Timestamp: 20000}},
				},
			},
			nowCheck1:   time.Unix(210, 0),
			wantWriter1: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points:     []types.MetricPoint{{Timestamp: 10000}, {Timestamp: 20000}},
				},
			},
			wantState1: map[types.MetricID]stateData{
				metricIDTest1: {flushDeadline: time.Unix(299, 0)},
			},
		},
		{
			name:      "single-more-points",
			nowWrite1: time.Unix(220, 0),
			nowCheck1: time.Unix(297, 0),
			write1: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 4200, Points: []types.MetricPoint{{Timestamp: 220000}}},
				{ID: metricIDTest2, TimeToLive: 1337, Points: []types.MetricPoint{{Timestamp: 221000}}},
			},
			wantWriter1: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points:     []types.MetricPoint{{Timestamp: 10000}, {Timestamp: 20000}, {Timestamp: 220000}},
				},
				{ID: metricIDTest2, TimeToLive: 1337, Points: []types.MetricPoint{{Timestamp: 221000}}},
			},
			wantState1: map[types.MetricID]stateData{
				metricIDTest1: {flushDeadline: time.Unix(299, 0)},
				metricIDTest2: {flushDeadline: flushTimestamp(metricIDTest2, time.Unix(220, 0), batchSize)},
			},
		},
		{
			name:      "single-deadline-reached",
			nowWrite1: time.Unix(302, 0),
			write1: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 4200, Points: []types.MetricPoint{{Timestamp: 210000}}},
			},
			wantWriter1: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 10000},
						{Timestamp: 20000},
						{Timestamp: 220000},
						{Timestamp: 210000},
					},
				},
				{ID: metricIDTest2, TimeToLive: 1337, Points: []types.MetricPoint{{Timestamp: 221000}}},
			},
		},
		{
			name:      "single-deadline-check",
			nowCheck1: time.Unix(301, 12),
			wantWriter1: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 4200, Points: []types.MetricPoint{
					{Timestamp: 10000}, {Timestamp: 20000}, {Timestamp: 210000}, {Timestamp: 220000},
				}},
				{ID: metricIDTest2, TimeToLive: 1337, Points: []types.MetricPoint{{Timestamp: 221000}}},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points:     []types.MetricPoint{{Timestamp: 220000}, {Timestamp: 210000}},
				},
				{ID: metricIDTest2, TimeToLive: 1337, Points: []types.MetricPoint{{Timestamp: 221000}}},
			},
			wantState1: map[types.MetricID]stateData{
				metricIDTest1: {flushDeadline: flushTimestamp(metricIDTest1, time.Unix(300, 0), batchSize)},
				metricIDTest2: {flushDeadline: flushTimestamp(metricIDTest2, time.Unix(300, 0), batchSize)},
			},
		},
		{
			name:      "single-shutdown",
			nowWrite1: time.Unix(303, 0),
			write1: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points:     []types.MetricPoint{{Timestamp: 230000}, {Timestamp: 199000}},
				},
			},
			nowCheck1: time.Unix(311, 0),
			shutdown1: true,
			wantWriter1: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points:     []types.MetricPoint{{Timestamp: 199000}, {Timestamp: 230000}},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points:     []types.MetricPoint{{Timestamp: 220000}, {Timestamp: 230000}},
				},
				{ID: metricIDTest2, TimeToLive: 1337, Points: []types.MetricPoint{{Timestamp: 221000}}},
			},
		},
		{
			name:        "restart-take-transfert-ownership",
			nowCheck1:   time.Unix(304, 0),
			wantWriter1: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 4200,
					Points:     []types.MetricPoint{{Timestamp: 220000}, {Timestamp: 230000}},
				},
				{ID: metricIDTest2, TimeToLive: 1337, Points: []types.MetricPoint{{Timestamp: 221000}}},
			},
			wantState1: map[types.MetricID]stateData{
				metricIDTest1: {flushDeadline: flushTimestamp(metricIDTest1, time.Unix(300, 0), batchSize)},
				metricIDTest2: {flushDeadline: flushTimestamp(metricIDTest2, time.Unix(300, 0), batchSize)},
			},
		},
		{
			// Writes > batchSize points → triggers synchronous owner backlog flush via channel.
			name:      "single-bulk",
			nowWrite1: time.Unix(400, 0),
			write1: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 4200, Points: generatePoint(229, 400, 1)},
			},
			wantWriter1: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 4200, Points: generatePoint(229, 400, 1)},
			},
			wantMemoryStore: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 4200, Points: generatePoint(300, 400, 1)},
				{ID: metricIDTest2, TimeToLive: 1337, Points: []types.MetricPoint{{Timestamp: 221000}}},
			},
		},
		{
			name:        "cleanup",
			nowCheck1:   time.Unix(900, 0),
			wantWriter1: []types.MetricData{},
			wantState1:  map[types.MetricID]stateData{},
		},
		{
			name:      "fill-2",
			nowWrite1: time.Unix(1000, 42),
			nowWrite2: time.Unix(1000, 42),
			write1: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 1000000}}},
			},
			write2: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points:     []types.MetricPoint{{Timestamp: 1000000}, {Timestamp: 1001000}},
				},
				{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 1000000}}},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points:     []types.MetricPoint{{Timestamp: 1000000}, {Timestamp: 1000000}, {Timestamp: 1001000}},
				},
				{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 1000000}}},
			},
			wantState1: map[types.MetricID]stateData{
				metricIDTest1: {flushDeadline: flushTimestamp(metricIDTest1, time.Unix(1000, 0), batchSize)},
			},
			wantState2: map[types.MetricID]stateData{
				metricIDTest2: {flushDeadline: flushTimestamp(metricIDTest2, time.Unix(1000, 0), batchSize)},
			},
		},
		{
			name:      "deadline-two-squirreldb",
			nowCheck1: time.Unix(1100, 0),
			nowCheck2: time.Unix(1100, 0),
			wantWriter1: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points:     []types.MetricPoint{{Timestamp: 1000000}, {Timestamp: 1001000}},
				},
			},
			wantWriter2: []types.MetricData{
				{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 1000000}}},
			},
			wantState1: map[types.MetricID]stateData{
				metricIDTest1: {flushDeadline: flushTimestamp(metricIDTest1, time.Unix(1100, 0), batchSize)},
			},
			wantState2: map[types.MetricID]stateData{
				metricIDTest2: {flushDeadline: flushTimestamp(metricIDTest2, time.Unix(1100, 0), batchSize)},
			},
		},
		{
			name:        "cleanup-2",
			nowCheck1:   time.Unix(1900, 0),
			nowCheck2:   time.Unix(1900, 0),
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantState1:  map[types.MetricID]stateData{},
			wantState2:  map[types.MetricID]stateData{},
		},
		{
			name:      "fill-2-bis",
			nowWrite1: time.Unix(2000, 42),
			nowWrite2: time.Unix(2000, 42),
			write1: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 2000000}}},
			},
			write2: []types.MetricData{
				{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 2000000}}},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 2000000}}},
				{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 2000000}}},
			},
			wantState1: map[types.MetricID]stateData{
				metricIDTest1: {flushDeadline: flushTimestamp(metricIDTest1, time.Unix(2000, 0), batchSize)},
			},
			wantState2: map[types.MetricID]stateData{
				metricIDTest2: {flushDeadline: flushTimestamp(metricIDTest2, time.Unix(2000, 0), batchSize)},
			},
		},
		{
			name:      "non-owner-write-deadline-excess",
			nowWrite2: time.Unix(2100, 42),
			nowCheck2: time.Unix(2100, 42),
			write2: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 2001000}}},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{
				{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 2000000}}},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points:     []types.MetricPoint{{Timestamp: 2000000}, {Timestamp: 2001000}},
				},
				{ID: metricIDTest2, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 2000000}}},
			},
			wantState1: map[types.MetricID]stateData{
				metricIDTest1: {flushDeadline: flushTimestamp(metricIDTest1, time.Unix(2000, 0), batchSize)},
			},
			wantState2: map[types.MetricID]stateData{
				metricIDTest2: {flushDeadline: flushTimestamp(metricIDTest2, time.Unix(2100, 0), batchSize)},
			},
		},
		{
			name:      "non-owner-write-deadline-excess2",
			nowWrite2: time.Unix(2300, 42),
			nowCheck2: time.Unix(2300, 42),
			write2: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: []types.MetricPoint{{Timestamp: 2002000}}},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         metricIDTest1,
					TimeToLive: 42,
					Points:     []types.MetricPoint{{Timestamp: 2000000}, {Timestamp: 2001000}, {Timestamp: 2002000}},
				},
			},
			wantState2: map[types.MetricID]stateData{},
		},
		{
			name:      "non-owner-write-below-second-threshold",
			nowWrite2: time.Unix(2350, 42),
			nowCheck2: time.Unix(2350, 42),
			write2: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: generatePoint(2003, 2149, 1)},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: generatePoint(2000, 2149, 1)},
			},
			wantState2: map[types.MetricID]stateData{},
		},
		{
			// Crosses the 2×batchSize threshold → triggers synchronous non-owner backlog flush.
			name:      "non-owner-write-above-second-threshold",
			nowWrite2: time.Unix(2360, 42),
			nowCheck2: time.Unix(2360, 42),
			write2: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: generatePoint(2150, 2200, 1)},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{
				{ID: metricIDTest1, TimeToLive: 42, Points: generatePoint(2000, 2200, 1)},
			},
			wantMemoryStore: []types.MetricData{},
			wantState2:      map[types.MetricID]stateData{},
		},
	}

	for _, tt := range tests {
		ok := t.Run(tt.name, func(t *testing.T) {
			if tt.write1 != nil {
				clock.SetTime(tt.nowWrite1)

				if err := batch1.batcher.write(ctx, batch1.writer, tt.write1, tt.nowWrite1); err != nil {
					t.Fatalf("batch1.write: %v", err)
				}
			}

			if tt.write2 != nil {
				clock.SetTime(tt.nowWrite2)

				if err := batch2.batcher.write(ctx, batch2.writer, tt.write2, tt.nowWrite2); err != nil {
					t.Fatalf("batch2.write: %v", err)
				}
			}

			if !tt.nowCheck1.IsZero() {
				clock.SetTime(tt.nowCheck1)

				if err := batch1.batcher.check(ctx, tt.nowCheck1, batch1.writer, tt.shutdown1, tt.shutdown1); err != nil {
					t.Fatal(err)
				}
			}

			if !tt.nowCheck2.IsZero() {
				clock.SetTime(tt.nowCheck2)

				if err := batch2.batcher.check(ctx, tt.nowCheck2, batch2.writer, tt.shutdown2, tt.shutdown2); err != nil {
					t.Fatal(err)
				}
			}

			if tt.wantWriter1 != nil {
				// Order-insensitive: a flush of several metrics writes them in
				// b.states map-iteration order, which is non-deterministic. Only the
				// set of (metric, points) written matters, not the cross-metric order.
				if !dataEqual(false, batch1.writer.Data, tt.wantWriter1) {
					t.Errorf("writer1 = %v, want = %v", batch1.writer.Data, tt.wantWriter1)
				}
			}

			if tt.wantState1 != nil {
				if !reflect.DeepEqual(batch1.batcher.states, tt.wantState1) {
					t.Errorf("state1 = %v, want = %v", batch1.batcher.states, tt.wantState1)
				}
			}

			if tt.wantWriter2 != nil {
				if !dataEqual(false, batch2.writer.Data, tt.wantWriter2) {
					t.Errorf("writer2 = %v, want = %v", batch2.writer.Data, tt.wantWriter2)
				}
			}

			if tt.wantState2 != nil {
				if !reflect.DeepEqual(batch2.batcher.states, tt.wantState2) {
					t.Errorf("state2 = %v, want = %v", batch2.batcher.states, tt.wantState2)
				}
			}

			if tt.wantMemoryStore != nil {
				got := dumpMemoryStore(ctx, memoryStore)
				if !dataEqual(false, got, tt.wantMemoryStore) {
					t.Errorf("memory store = %v, want = %v", got, tt.wantMemoryStore)
				}
			}

			if tt.shutdown1 {
				batch1 = renewBatch(batch1)
			}

			if tt.shutdown2 {
				batch2 = renewBatch(batch2)
			}

			batch1.writer.Data = nil
			batch2.writer.Data = nil
		})

		if !ok {
			break
		}
	}
}

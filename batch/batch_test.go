package batch

import (
	"context"
	"reflect"
	"sort"
	"squirreldb/dummy"
	"squirreldb/dummy/temporarystore"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const (
	MetricIDTest1 = 1
	MetricIDTest2 = 2
	MetricIDTest3 = 3
)

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

func generatePoint(fromTS int, toTS int, step int) []types.MetricPoint { //nolint:unparam
	points := make([]types.MetricPoint, 0)
	for ts := fromTS; ts <= toTS; ts += step {
		points = append(points, types.MetricPoint{
			Timestamp: int64(ts) * 1000,
		})
	}

	return points
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

	sort.Slice(aCopy, func(i, j int) bool {
		return aCopy[i].ID < aCopy[j].ID
	})

	sort.Slice(bCopy, func(i, j int) bool {
		return bCopy[i].ID < bCopy[j].ID
	})

	return reflect.DeepEqual(aCopy, bCopy)
}

func newPersistentStore(initialData []types.MetricData) *dummy.MemoryTSDB {
	db := &dummy.MemoryTSDB{
		LogRequest: true,
	}

	err := db.Write(context.Background(), initialData)
	if err != nil {
		panic(err)
	}

	return db
}

func TestBatch_read(t *testing.T) { //nolint:maintidx
	type fields struct {
		memoryStore     TemporaryStore
		states          map[types.MetricID]stateData
		persistentStore *dummy.MemoryTSDB
		batchSize       time.Duration
	}

	type args struct {
		request types.MetricRequest
	}

	tests := []struct {
		fields                 fields
		name                   string
		want                   []types.MetricData
		args                   args
		mustSkipPersitentReads bool
		wantErr                bool
	}{
		{
			name: "temporary_filled_persistent_filled",
			fields: fields{
				batchSize: 50 * time.Second,
				states:    nil,
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 50000,
								Value:     60,
							},
							{
								Timestamp: 60000,
								Value:     70,
							},
						},
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 100000,
								Value:     300,
							},
							{
								Timestamp: 120000,
								Value:     350,
							},
						},
					},
				}),
				persistentStore: newPersistentStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
						},
					},
				}),
			},
			args: args{
				request: types.MetricRequest{
					IDs: []types.MetricID{
						MetricIDTest1,
						MetricIDTest2,
					},
					FromTimestamp: 0,
					ToTimestamp:   200000,
					StepMs:        0,
					Function:      "",
				},
			},
			want: []types.MetricData{
				{
					ID: MetricIDTest1,
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10000,
							Value:     20,
						},
						{
							Timestamp: 20000,
							Value:     30,
						},
						{
							Timestamp: 30000,
							Value:     40,
						},
						{
							Timestamp: 40000,
							Value:     50,
						},
						{
							Timestamp: 50000,
							Value:     60,
						},
						{
							Timestamp: 60000,
							Value:     70,
						},
					},
				},
				{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     50,
						},
						{
							Timestamp: 20000,
							Value:     100,
						},
						{
							Timestamp: 40000,
							Value:     150,
						},
						{
							Timestamp: 60000,
							Value:     200,
						},
						{
							Timestamp: 80000,
							Value:     250,
						},
						{
							Timestamp: 100000,
							Value:     300,
						},
						{
							Timestamp: 120000,
							Value:     350,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "temporary_filled_persistent_empty",
			fields: fields{
				batchSize: 50 * time.Second,
				states:    nil,
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 50000,
								Value:     60,
							},
							{
								Timestamp: 60000,
								Value:     70,
							},
						},
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 100000,
								Value:     300,
							},
							{
								Timestamp: 120000,
								Value:     350,
							},
						},
					},
				}),
				persistentStore: newPersistentStore(nil),
			},
			args: args{
				request: types.MetricRequest{
					IDs: []types.MetricID{
						MetricIDTest1,
						MetricIDTest2,
					},
					FromTimestamp: 0,
					ToTimestamp:   200000,
					StepMs:        0,
					Function:      "",
				},
			},
			want: []types.MetricData{
				{
					ID: MetricIDTest1,
					Points: []types.MetricPoint{
						{
							Timestamp: 50000,
							Value:     60,
						},
						{
							Timestamp: 60000,
							Value:     70,
						},
					},
				},
				{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{
							Timestamp: 100000,
							Value:     300,
						},
						{
							Timestamp: 120000,
							Value:     350,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "temporary_empty_persistent_filled",
			fields: fields{
				batchSize:   50 * time.Second,
				states:      nil,
				memoryStore: newMemoryStore(nil),
				persistentStore: newPersistentStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
						},
					},
				}),
			},
			args: args{
				request: types.MetricRequest{
					IDs: []types.MetricID{
						MetricIDTest1,
						MetricIDTest2,
					},
					FromTimestamp: 0,
					ToTimestamp:   200000,
					StepMs:        0,
					Function:      "",
				},
			},
			want: []types.MetricData{
				{
					ID: MetricIDTest1,
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10000,
							Value:     20,
						},
						{
							Timestamp: 20000,
							Value:     30,
						},
						{
							Timestamp: 30000,
							Value:     40,
						},
						{
							Timestamp: 40000,
							Value:     50,
						},
					},
				},
				{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     50,
						},
						{
							Timestamp: 20000,
							Value:     100,
						},
						{
							Timestamp: 40000,
							Value:     150,
						},
						{
							Timestamp: 60000,
							Value:     200,
						},
						{
							Timestamp: 80000,
							Value:     250,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "temporary_empty_persistent_empty",
			fields: fields{
				batchSize:       50 * time.Second,
				states:          nil,
				memoryStore:     newMemoryStore(nil),
				persistentStore: newPersistentStore(nil),
			},
			args: args{
				request: types.MetricRequest{
					IDs: []types.MetricID{
						MetricIDTest1,
						MetricIDTest2,
					},
					FromTimestamp: 0,
					ToTimestamp:   200000,
					StepMs:        0,
					Function:      "",
				},
			},
			want:    []types.MetricData{},
			wantErr: false,
		},
		{
			name: "temporary_has_all_points",
			fields: fields{
				batchSize: 50 * time.Second,
				states:    nil,
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
					},
				}),
				persistentStore: newPersistentStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
					},
				}),
			},
			args: args{
				request: types.MetricRequest{
					IDs: []types.MetricID{
						MetricIDTest1,
					},
					FromTimestamp: 0,
					ToTimestamp:   100000,
					StepMs:        0,
					Function:      "",
				},
			},
			want: []types.MetricData{
				{
					ID: MetricIDTest1,
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10000,
							Value:     20,
						},
						{
							Timestamp: 20000,
							Value:     30,
						},
						{
							Timestamp: 30000,
							Value:     40,
						},
						{
							Timestamp: 40000,
							Value:     50,
						},
					},
				},
			},
			mustSkipPersitentReads: true,
			wantErr:                false,
		},
		{
			name: "temporary_has_all_points_offset",
			fields: fields{
				batchSize: 50 * time.Second,
				states:    nil,
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
					},
				}),
				persistentStore: newPersistentStore(nil), // content does matter
			},
			args: args{
				request: types.MetricRequest{
					IDs: []types.MetricID{
						MetricIDTest1,
					},
					FromTimestamp: 1000,
					ToTimestamp:   110000,
					StepMs:        0,
					Function:      "",
				},
			},
			want: []types.MetricData{
				{
					ID: MetricIDTest1,
					Points: []types.MetricPoint{
						{
							Timestamp: 10000,
							Value:     20,
						},
						{
							Timestamp: 20000,
							Value:     30,
						},
						{
							Timestamp: 30000,
							Value:     40,
						},
						{
							Timestamp: 40000,
							Value:     50,
						},
					},
				},
			},
			mustSkipPersitentReads: true,
			wantErr:                false,
		},
		{
			name: "temporary_has_all_points_metric_disapear",
			fields: fields{
				batchSize: 50 * time.Second,
				states:    nil,
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
						},
					},
				}),
				persistentStore: newPersistentStore(nil), // content does matter
			},
			args: args{
				request: types.MetricRequest{
					IDs: []types.MetricID{
						MetricIDTest1,
					},
					FromTimestamp: 20000,
					ToTimestamp:   110000,
					StepMs:        0,
					Function:      "",
				},
			},
			want:                   []types.MetricData{},
			mustSkipPersitentReads: true,
			wantErr:                false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				batchSize:   tt.fields.batchSize,
				states:      tt.fields.states,
				memoryStore: tt.fields.memoryStore,
				reader:      tt.fields.persistentStore,
				writer:      tt.fields.persistentStore,
				metrics:     newMetrics(prometheus.NewRegistry()),
			}
			got, err := b.ReadIter(context.Background(), tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadIter() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			gotList, err := types.MetricIterToList(got, 0)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadIter() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !dataEqual(true, gotList, tt.want) {
				t.Errorf("read() got = %v, want %v", gotList, tt.want)
			}

			if tt.mustSkipPersitentReads && len(tt.fields.persistentStore.Reads) > 0 {
				t.Errorf("Had reads = %v, want none", tt.fields.persistentStore.Reads)
			}
		})
	}
}

func TestBatch_readTemporary(t *testing.T) {
	type fields struct {
		memoryStore TemporaryStore
		reader      types.MetricReader
		writer      types.MetricWriter
		states      map[types.MetricID]stateData
		batchSize   time.Duration
	}

	type args struct {
		ids           []types.MetricID
		fromTimestamp int64
		toTimestamp   int64
	}

	tests := []struct {
		fields  fields
		name    string
		want    []types.MetricData
		args    args
		wantErr bool
	}{
		{
			name: "temporary_filled",
			fields: fields{
				batchSize: 0,
				states:    nil,
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 50000,
								Value:     60,
							},
							{
								Timestamp: 60000,
								Value:     70,
							},
						},
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 100000,
								Value:     300,
							},
							{
								Timestamp: 120000,
								Value:     350,
							},
						},
					},
				}),
				reader: nil,
				writer: nil,
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
					MetricIDTest2,
				},
				fromTimestamp: 0,
				toTimestamp:   200000,
			},
			want: []types.MetricData{
				{
					ID: MetricIDTest1,
					Points: []types.MetricPoint{
						{
							Timestamp: 50000,
							Value:     60,
						},
						{
							Timestamp: 60000,
							Value:     70,
						},
					},
				},
				{
					ID: MetricIDTest2,
					Points: []types.MetricPoint{
						{
							Timestamp: 100000,
							Value:     300,
						},
						{
							Timestamp: 120000,
							Value:     350,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "temporary_empty",
			fields: fields{
				batchSize:   0,
				states:      nil,
				memoryStore: newMemoryStore(nil),
				reader:      nil,
				writer:      nil,
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
					MetricIDTest2,
				},
				fromTimestamp: 0,
				toTimestamp:   200000,
			},
			want:    []types.MetricData{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				batchSize:   tt.fields.batchSize,
				states:      tt.fields.states,
				memoryStore: tt.fields.memoryStore,
				reader:      tt.fields.reader,
				writer:      tt.fields.writer,
				metrics:     newMetrics(prometheus.NewRegistry()),
			}
			got, err := b.readTemporary(context.Background(), tt.args.ids, tt.args.fromTimestamp, tt.args.toTimestamp)
			if (err != nil) != tt.wantErr {
				t.Errorf("readTemporary() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
			if !dataEqual(true, got, tt.want) {
				t.Errorf("readTemporary() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_flushTimestamp(t *testing.T) {
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

func TestBatch_flush(t *testing.T) { //nolint:maintidx
	type fields struct {
		memoryStore TemporaryStore
		states      map[types.MetricID]stateData
		writer      *dummy.MemoryTSDB
		batchSize   time.Duration
	}

	type args struct {
		now      time.Time
		ids      []types.MetricID
		shutdown bool
	}

	tests := []struct {
		fields          fields
		name            string
		wantWriter      []types.MetricData
		wantMemoryStore []types.MetricData
		args            args
	}{
		{
			name: "tsdb-write-sorted",
			fields: fields{
				batchSize: 300 * time.Second,
				states:    map[types.MetricID]stateData{},
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID:         MetricIDTest1,
						TimeToLive: 42,
						Points: []types.MetricPoint{
							{Timestamp: 100000, Value: 42.0},
							{Timestamp: 110000, Value: 43.0},
							{Timestamp: 130000, Value: 45.0},
							{Timestamp: 120000, Value: 44.0},
							{Timestamp: 140000, Value: 46.0},
						},
					},
				}),
				writer: newPersistentStore(nil),
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
				},
				now:      time.Unix(150, 0),
				shutdown: false,
			},
			wantWriter: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 110000, Value: 43.0},
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 110000, Value: 43.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
		},
		{
			name: "tsdb-write-sorted-dedup",
			fields: fields{
				batchSize: 300 * time.Second,
				states:    map[types.MetricID]stateData{},
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID:         MetricIDTest1,
						TimeToLive: 42,
						Points: []types.MetricPoint{
							{Timestamp: 140000, Value: 46.0},
							{Timestamp: 140000, Value: 46.0},
							{Timestamp: 100000, Value: 42.0},
							{Timestamp: 110000, Value: 43.0},
							{Timestamp: 130000, Value: 45.0},
							{Timestamp: 120000, Value: 44.0},
							{Timestamp: 100000, Value: 42.0},
						},
					},
				}),
				writer: newPersistentStore(nil),
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
				},
				now:      time.Unix(150, 0),
				shutdown: false,
			},
			wantWriter: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 110000, Value: 43.0},
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 140000, Value: 46.0},
						{Timestamp: 140000, Value: 46.0},
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 110000, Value: 43.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 100000, Value: 42.0},
					},
				},
			},
		},
		{
			name: "tsdb-write-dedup",
			fields: fields{
				batchSize: 300 * time.Second,
				states:    map[types.MetricID]stateData{},
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID:         MetricIDTest1,
						TimeToLive: 42,
						Points: []types.MetricPoint{
							{Timestamp: 100000, Value: 42.0},
							{Timestamp: 100000, Value: 42.0},
							{Timestamp: 110000, Value: 43.0},
							{Timestamp: 120000, Value: 44.0},
							{Timestamp: 130000, Value: 45.0},
							{Timestamp: 140000, Value: 46.0},
							{Timestamp: 140000, Value: 46.0},
						},
					},
				}),
				writer: newPersistentStore(nil),
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
				},
				now:      time.Unix(150, 0),
				shutdown: false,
			},
			wantWriter: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 110000, Value: 43.0},
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 110000, Value: 43.0},
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 140000, Value: 46.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
		},
		{
			name: "keep-last-batchsize",
			fields: fields{
				batchSize: 300 * time.Second,
				states:    map[types.MetricID]stateData{},
				memoryStore: newMemoryStore([]types.MetricData{
					{
						ID:         MetricIDTest1,
						TimeToLive: 42,
						Points: []types.MetricPoint{
							{Timestamp: 100000, Value: 42.0},
							{Timestamp: 110000, Value: 43.0},
							{Timestamp: 120000, Value: 44.0},
							{Timestamp: 130000, Value: 45.0},
							{Timestamp: 140000, Value: 46.0},
						},
					},
				}),
				writer: newPersistentStore(nil),
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
				},
				now:      time.Unix(300+130, 0),
				shutdown: false,
			},
			wantWriter: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 110000, Value: 43.0},
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
		},
		{
			name: "tsdb-write-after-offset",
			fields: fields{
				batchSize: 300 * time.Second,
				states:    map[types.MetricID]stateData{},
				memoryStore: newMemoryStoreOffset(
					[]types.MetricData{
						{
							ID:         MetricIDTest1,
							TimeToLive: 42,
							Points: []types.MetricPoint{
								{Timestamp: 100000, Value: 42.0},
								{Timestamp: 110000, Value: 43.0},
								{Timestamp: 120000, Value: 44.0},
								{Timestamp: 130000, Value: 45.0},
								{Timestamp: 140000, Value: 46.0},
							},
						},
					},
					[]int{2},
				),
				writer: newPersistentStore(nil),
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
				},
				now:      time.Unix(125, 0),
				shutdown: false,
			},
			wantWriter: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 100000, Value: 42.0},
						{Timestamp: 110000, Value: 43.0},
						{Timestamp: 120000, Value: 44.0},
						{Timestamp: 130000, Value: 45.0},
						{Timestamp: 140000, Value: 46.0},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				batchSize:   tt.fields.batchSize,
				states:      tt.fields.states,
				memoryStore: tt.fields.memoryStore,
				writer:      tt.fields.writer,
				metrics:     newMetrics(prometheus.NewRegistry()),
			}
			b.flush(context.Background(), tt.args.ids, tt.args.now, tt.args.shutdown)

			if !reflect.DeepEqual(tt.fields.writer.DumpData(), tt.wantWriter) {
				t.Errorf("writer = %v, want = %v", tt.fields.writer.DumpData(), tt.wantWriter)
			}

			gotMemoryStore := dumpMemoryStore(context.Background(), tt.fields.memoryStore)
			if !dataEqual(true, gotMemoryStore, tt.wantMemoryStore) {
				t.Errorf("memory store = %v, want = %v", gotMemoryStore, tt.wantMemoryStore)
			}
		})
	}
}

// TestBatch_write test behavior of two SquirrelDB sharing the same
// memoryStore (e.g. Redis).
func TestBatch_write(t *testing.T) { //nolint:maintidx
	batchSize := 100 * time.Second
	memoryStore := temporarystore.New(prometheus.NewRegistry(), log.With().Str("component", "temporary_store").Logger())
	writer1 := newPersistentStore(nil)
	writer2 := newPersistentStore(nil)
	logger1 := log.With().Str("component", "batch1").Logger()
	batch1 := New(prometheus.NewRegistry(), batchSize, memoryStore, nil, writer1, logger1)
	logger2 := log.With().Str("component", "batch2").Logger()
	batch2 := New(prometheus.NewRegistry(), batchSize, memoryStore, nil, writer2, logger2)

	// tests case will reuse batch1 & batch2.
	// When write1 is not nil, sent it to batch1. Then after does the same with
	// write2 and batch2.
	// If the want* is nil, it means don't test.
	// Write are done at nowWriteN timestamp.
	// If nowCheck is not nil, do the check call (not flush owned metrics)
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
		shutdown2       bool
		shutdown1       bool
	}{
		{
			name:      "single-initial-fill",
			nowWrite1: time.Unix(200, 0),
			write1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 10000},
						{Timestamp: 20000},
					},
				},
			},
			nowCheck1:   time.Unix(210, 0),
			wantWriter1: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 10000},
						{Timestamp: 20000},
					},
				},
			},
			wantState1: map[types.MetricID]stateData{
				MetricIDTest1: {
					flushDeadline: time.Unix(299, 0), // 200 + 100 - 1. flushTimestamp
				},
			},
		},
		{
			name:      "single-more-points",
			nowWrite1: time.Unix(220, 0),
			nowCheck1: time.Unix(297, 0),
			write1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 220000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 1337,
					Points: []types.MetricPoint{
						{Timestamp: 221000},
					},
				},
			},
			wantWriter1: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 10000},
						{Timestamp: 20000},
						{Timestamp: 220000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 1337,
					Points: []types.MetricPoint{
						{Timestamp: 221000},
					},
				},
			},
			wantState1: map[types.MetricID]stateData{
				MetricIDTest1: {
					flushDeadline: time.Unix(299, 0),
				},
				MetricIDTest2: {
					flushDeadline: flushTimestamp(
						MetricIDTest2,
						time.Unix(220, 0),
						batchSize,
					),
				},
			},
		},
		{
			name:      "single-deadline-reached",
			nowWrite1: time.Unix(302, 0),
			write1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 210000},
					},
				},
			},
			wantWriter1: []types.MetricData{}, // not yet write, it's check that do the flush
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 10000},
						{Timestamp: 20000},
						{Timestamp: 220000},
						{Timestamp: 210000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 1337,
					Points: []types.MetricPoint{
						{Timestamp: 221000},
					},
				},
			},
		},
		{
			name:      "single-deadline-check",
			nowCheck1: time.Unix(301, 12),
			wantWriter1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 10000},
						{Timestamp: 20000},
						{Timestamp: 210000},
						{Timestamp: 220000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 1337,
					Points: []types.MetricPoint{
						{Timestamp: 221000},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 220000},
						{Timestamp: 210000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 1337,
					Points: []types.MetricPoint{
						{Timestamp: 221000},
					},
				},
			},
			wantState1: map[types.MetricID]stateData{
				MetricIDTest1: {
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(300, 0),
						batchSize,
					),
				},
				MetricIDTest2: {
					flushDeadline: flushTimestamp(
						MetricIDTest2,
						time.Unix(300, 0),
						batchSize,
					),
				},
			},
		},
		{
			name:      "single-shutdown",
			nowWrite1: time.Unix(303, 0),
			write1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 230000},
						{Timestamp: 199000},
					},
				},
			},
			nowCheck1: time.Unix(311, 0),
			shutdown1: true,
			wantWriter1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 199000},
						{Timestamp: 230000},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 220000},
						{Timestamp: 230000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 1337,
					Points: []types.MetricPoint{
						{Timestamp: 221000},
					},
				},
			},
		},
		{
			name:        "restart-take-transfert-ownership",
			nowCheck1:   time.Unix(304, 0),
			wantWriter1: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points: []types.MetricPoint{
						{Timestamp: 220000},
						{Timestamp: 230000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 1337,
					Points: []types.MetricPoint{
						{Timestamp: 221000},
					},
				},
			},
			wantState1: map[types.MetricID]stateData{
				MetricIDTest1: {
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(300, 0),
						batchSize,
					),
				},
				MetricIDTest2: {
					flushDeadline: flushTimestamp(
						MetricIDTest2,
						time.Unix(300, 0),
						batchSize,
					),
				},
			},
		},
		{
			name:      "single-bulk",
			nowWrite1: time.Unix(400, 0),
			write1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points:     generatePoint(229, 400, 1),
				},
			},
			wantWriter1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points:     generatePoint(229, 400, 1),
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 4200,
					Points:     generatePoint(300, 400, 1),
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 1337,
					Points: []types.MetricPoint{
						{Timestamp: 221000},
					},
				},
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
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 1000000},
					},
				},
			},
			write2: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 1000000},
						{Timestamp: 1001000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 1000000},
					},
				},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 1000000},
						{Timestamp: 1000000},
						{Timestamp: 1001000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 1000000},
					},
				},
			},
			wantState1: map[types.MetricID]stateData{
				MetricIDTest1: {
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(1000, 0),
						batchSize,
					),
				},
			},
			wantState2: map[types.MetricID]stateData{
				MetricIDTest2: {
					flushDeadline: flushTimestamp(
						MetricIDTest2,
						time.Unix(1000, 0),
						batchSize,
					),
				},
			},
		},
		{
			name:      "deadline-two-squirreldb",
			nowCheck1: time.Unix(1100, 0),
			nowCheck2: time.Unix(1100, 0),
			wantWriter1: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 1000000},
						{Timestamp: 1001000},
					},
				},
			},
			wantWriter2: []types.MetricData{
				{
					ID:         MetricIDTest2,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 1000000},
					},
				},
			},
			wantState1: map[types.MetricID]stateData{
				MetricIDTest1: {
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(1100, 0),
						batchSize,
					),
				},
			},
			wantState2: map[types.MetricID]stateData{
				MetricIDTest2: {
					flushDeadline: flushTimestamp(
						MetricIDTest2,
						time.Unix(1100, 0),
						batchSize,
					),
				},
			},
		},
		{
			name:        "cleanup",
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
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2000000},
					},
				},
			},
			write2: []types.MetricData{
				{
					ID:         MetricIDTest2,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2000000},
					},
				},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2000000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2000000},
					},
				},
			},
			wantState1: map[types.MetricID]stateData{
				MetricIDTest1: {
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(2000, 0),
						batchSize,
					),
				},
			},
			wantState2: map[types.MetricID]stateData{
				MetricIDTest2: {
					flushDeadline: flushTimestamp(
						MetricIDTest2,
						time.Unix(2000, 0),
						batchSize,
					),
				},
			},
		},
		{
			name:      "non-owner-write-deadline-excess",
			nowWrite2: time.Unix(2100, 42),
			nowCheck2: time.Unix(2100, 42),
			write2: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2001000},
					},
				},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{
				{
					ID:         MetricIDTest2,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2000000},
					},
				},
			},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2000000},
						{Timestamp: 2001000},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2000000},
					},
				},
			},
			wantState1: map[types.MetricID]stateData{
				MetricIDTest1: {
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(2000, 0),
						batchSize,
					),
				},
			},
			wantState2: map[types.MetricID]stateData{
				MetricIDTest2: {
					flushDeadline: flushTimestamp(
						MetricIDTest2,
						time.Unix(2100, 0),
						batchSize,
					),
				},
			},
		},
		{
			name:      "non-owner-write-deadline-excess2",
			nowWrite2: time.Unix(2300, 42),
			nowCheck2: time.Unix(2300, 42),
			write2: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2002000},
					},
				},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points: []types.MetricPoint{
						{Timestamp: 2000000},
						{Timestamp: 2001000},
						{Timestamp: 2002000},
					},
				},
			},
			wantState2: map[types.MetricID]stateData{},
		},
		{
			name:      "non-owner-write-deadline-excess-150-points",
			nowWrite2: time.Unix(2350, 42),
			nowCheck2: time.Unix(2350, 42),
			write2: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points:     generatePoint(2003, 2149, 1),
				},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{},
			wantMemoryStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points:     generatePoint(2000, 2149, 1),
				},
			},
			wantState2: map[types.MetricID]stateData{},
		},
		{
			name:      "non-owner-write-deadline-excess-201-points",
			nowWrite2: time.Unix(2360, 42),
			nowCheck2: time.Unix(2360, 42),
			write2: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points:     generatePoint(2150, 2200, 1),
				},
			},
			wantWriter1: []types.MetricData{},
			wantWriter2: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 42,
					Points:     generatePoint(2000, 2200, 1),
				},
			},
			wantMemoryStore: []types.MetricData{},
			wantState2:      map[types.MetricID]stateData{},
		},
	}
	ctx := context.Background()

	for _, tt := range tests {
		ok := t.Run(tt.name, func(t *testing.T) {
			if tt.write1 != nil {
				if err := batch1.write(ctx, tt.write1, tt.nowWrite1); err != nil {
					t.Errorf("batch1.write: %v", err)

					return
				}
			}

			if tt.write2 != nil {
				if err := batch2.write(ctx, tt.write2, tt.nowWrite2); err != nil {
					t.Errorf("batch2.write: %v", err)

					return
				}
			}

			if !tt.nowCheck1.IsZero() {
				if err := batch1.check(ctx, tt.nowCheck1, tt.shutdown1, tt.shutdown1); err != nil {
					t.Fatal(err)
				}
			}

			if !tt.nowCheck2.IsZero() {
				if err := batch2.check(ctx, tt.nowCheck2, tt.shutdown2, tt.shutdown2); err != nil {
					t.Fatal(err)
				}
			}

			if tt.wantWriter1 != nil {
				if !reflect.DeepEqual(writer1.DumpData(), tt.wantWriter1) {
					t.Errorf("writer1 = %v, want = %v", writer1.DumpData(), tt.wantWriter1)
				}
			}

			if tt.wantState1 != nil {
				if !reflect.DeepEqual(batch1.states, tt.wantState1) {
					t.Errorf("state1 = %v, want = %v", batch1.states, tt.wantState1)
				}
			}

			if tt.wantWriter2 != nil {
				if !dataEqual(true, writer2.DumpData(), tt.wantWriter2) {
					t.Errorf("writer2 = %v, want = %v", writer2.DumpData(), tt.wantWriter2)
				}
			}

			if tt.wantState2 != nil {
				if !reflect.DeepEqual(batch2.states, tt.wantState2) {
					t.Errorf("state2 = %v, want = %v", batch2.states, tt.wantState2)
				}
			}

			if tt.wantMemoryStore != nil {
				gotMemoryStore := dumpMemoryStore(ctx, memoryStore)
				if !dataEqual(false, gotMemoryStore, tt.wantMemoryStore) {
					t.Errorf("memory store = %v, want = %v", gotMemoryStore, tt.wantMemoryStore)
				}
			}

			if tt.shutdown1 {
				logger1 := log.With().Str("component", "batch1").Logger()
				batch1 = New(prometheus.NewRegistry(), batchSize, memoryStore, nil, writer1, logger1)
			}
			if tt.shutdown2 {
				logger2 := log.With().Str("component", "batch2").Logger()
				batch2 = New(prometheus.NewRegistry(), batchSize, memoryStore, nil, writer2, logger2)
			}

			writer1.Data = nil
			writer2.Data = nil
		})
		if !ok {
			break
		}
	}
}

func Test_randomDuration(t *testing.T) {
	target := 50 * time.Millisecond
	min := 40 * time.Millisecond
	max := 60 * time.Millisecond

	for n := 0; n < 100; n++ {
		got := randomDuration(target)
		if got < min {
			t.Errorf("randomDuration() = %v, want >= %v", got, min)
		}

		if max < got {
			t.Errorf("randomDuration() = %v, want <= %v", got, max)
		}
	}
}

func Test_takeover(t *testing.T) { //nolint:maintidx
	batchSize := 100 * time.Second
	memoryStore := temporarystore.New(prometheus.NewRegistry(), log.With().Str("component", "temporary_store").Logger())
	writer1 := newPersistentStore(nil)
	writer2 := newPersistentStore(nil)
	logger1 := log.With().Str("component", "batch1").Logger()
	batch1 := New(prometheus.NewRegistry(), batchSize, memoryStore, nil, writer1, logger1)
	logger2 := log.With().Str("component", "batch2").Logger()
	batch2 := New(prometheus.NewRegistry(), batchSize, memoryStore, nil, writer2, logger2)
	ctx := context.Background()

	err := batch1.write(
		context.Background(),
		[]types.MetricData{
			{
				ID:         MetricIDTest1,
				TimeToLive: 42,
				Points: []types.MetricPoint{
					{Timestamp: 10000},
				},
			},
		},
		time.Unix(10, 0),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = batch2.write(
		context.Background(),
		[]types.MetricData{
			{
				ID:         MetricIDTest2,
				TimeToLive: 42,
				Points: []types.MetricPoint{
					{Timestamp: 10000},
				},
			},
		},
		time.Unix(12, 0),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = batch1.write(
		context.Background(),
		[]types.MetricData{
			{
				ID:         MetricIDTest1,
				TimeToLive: 42,
				Points: []types.MetricPoint{
					{Timestamp: 20000},
				},
			},
			{
				ID:         MetricIDTest2,
				TimeToLive: 42,
				Points: []types.MetricPoint{
					{Timestamp: 21000},
				},
			},
		},
		time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}

	wantWriter1 := []types.MetricData{}
	wantWriter2 := []types.MetricData{}
	wantState1 := map[types.MetricID]stateData{
		MetricIDTest1: {
			flushDeadline: flushTimestamp(MetricIDTest1, time.Unix(10, 0), batchSize),
		},
	}
	wantState2 := map[types.MetricID]stateData{
		MetricIDTest2: {
			flushDeadline: flushTimestamp(MetricIDTest2, time.Unix(10, 0), batchSize),
		},
	}
	wantMemoryStore := []types.MetricData{
		{
			ID:         MetricIDTest1,
			TimeToLive: 42,
			Points: []types.MetricPoint{
				{Timestamp: 10000},
				{Timestamp: 20000},
			},
		},
		{
			ID:         MetricIDTest2,
			TimeToLive: 42,
			Points: []types.MetricPoint{
				{Timestamp: 10000},
				{Timestamp: 21000},
			},
		},
	}

	if !reflect.DeepEqual(writer1.DumpData(), wantWriter1) {
		t.Errorf("writer1.metrics = %v, want %v", writer1.DumpData(), wantWriter1)
	}

	if !reflect.DeepEqual(writer2.DumpData(), wantWriter2) {
		t.Errorf("writer2.metrics = %v, want %v", writer2.DumpData(), wantWriter2)
	}

	if !reflect.DeepEqual(batch1.states, wantState1) {
		t.Errorf("batch1.states = %v, want = %v", batch1.states, wantState1)
	}

	if !reflect.DeepEqual(batch2.states, wantState2) {
		t.Errorf("batch2.states = %v, want = %v", batch2.states, wantState2)
	}

	gotMemoryStore := dumpMemoryStore(ctx, memoryStore)
	if !dataEqual(false, gotMemoryStore, wantMemoryStore) {
		t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
	}

	writer1.Data = nil
	writer2.Data = nil

	// No takeover yet, but batch2 will flush its metric
	now := time.Unix(0, 0).Add(overdueThreshold)
	batch2.checkTakeover(ctx, now)

	if err := batch2.check(ctx, now, false, false); err != nil {
		t.Fatal(err)
	}

	wantWriter1 = []types.MetricData{}
	wantWriter2 = []types.MetricData{
		{
			ID:         MetricIDTest2,
			TimeToLive: 42,
			Points: []types.MetricPoint{
				{Timestamp: 10000},
				{Timestamp: 21000},
			},
		},
	}
	wantState2 = map[types.MetricID]stateData{}
	wantMemoryStore = []types.MetricData{
		{
			ID:         MetricIDTest1,
			TimeToLive: 42,
			Points: []types.MetricPoint{
				{Timestamp: 10000},
				{Timestamp: 20000},
			},
		},
	}

	if !reflect.DeepEqual(writer1.DumpData(), wantWriter1) {
		t.Errorf("writer1.metrics = %v, want %v", writer1.DumpData(), wantWriter1)
	}

	if !reflect.DeepEqual(writer2.DumpData(), wantWriter2) {
		t.Errorf("writer2.metrics = %v, want %v", writer2.DumpData(), wantWriter2)
	}

	if !reflect.DeepEqual(batch1.states, wantState1) {
		t.Errorf("batch1.states = %v, want = %v", batch1.states, wantState1)
	}

	if !reflect.DeepEqual(batch2.states, wantState2) {
		t.Errorf("batch2.states = %v, want = %v", batch2.states, wantState2)
	}

	gotMemoryStore = dumpMemoryStore(ctx, memoryStore)
	if !dataEqual(true, gotMemoryStore, wantMemoryStore) {
		t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
	}

	writer1.Data = nil
	writer2.Data = nil

	// Taking over
	now = time.Unix(100, 0).Add(overdueThreshold)
	batch2.checkTakeover(ctx, now)

	if err := batch2.check(ctx, now, false, false); err != nil {
		t.Fatal(err)
	}

	wantWriter1 = []types.MetricData{}
	wantWriter2 = []types.MetricData{
		{
			ID:         MetricIDTest1,
			TimeToLive: 42,
			Points: []types.MetricPoint{
				{Timestamp: 10000},
				{Timestamp: 20000},
			},
		},
	}
	wantMemoryStore = []types.MetricData{}

	if !reflect.DeepEqual(writer1.DumpData(), wantWriter1) {
		t.Errorf("writer1.metrics = %v, want %v", writer1.DumpData(), wantWriter1)
	}

	if !reflect.DeepEqual(writer2.DumpData(), wantWriter2) {
		t.Errorf("writer2.metrics = %v, want %v", writer2.DumpData(), wantWriter2)
	}

	if !reflect.DeepEqual(batch1.states, wantState1) {
		t.Errorf("batch1.states = %v, want = %v", batch1.states, wantState1)
	}

	if !reflect.DeepEqual(batch2.states, wantState2) {
		t.Errorf("batch2.states = %v, want = %v", batch2.states, wantState2)
	}

	gotMemoryStore = dumpMemoryStore(ctx, memoryStore)
	if !dataEqual(true, gotMemoryStore, wantMemoryStore) {
		t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
	}

	writer1.Data = nil
	writer2.Data = nil

	// batch1 will realize that a takeover happened
	if err := batch1.check(ctx, now, false, false); err != nil {
		t.Fatal(err)
	}

	wantWriter1 = []types.MetricData{}
	wantWriter2 = []types.MetricData{}
	wantState1 = map[types.MetricID]stateData{}

	if !reflect.DeepEqual(writer1.DumpData(), wantWriter1) {
		t.Errorf("writer1.metrics = %v, want %v", writer1.DumpData(), wantWriter1)
	}

	if !reflect.DeepEqual(writer2.DumpData(), wantWriter2) {
		t.Errorf("writer2.metrics = %v, want %v", writer2.DumpData(), wantWriter2)
	}

	if !reflect.DeepEqual(batch1.states, wantState1) {
		t.Errorf("batch1.states = %v, want = %v", batch1.states, wantState1)
	}

	if !reflect.DeepEqual(batch2.states, wantState2) {
		t.Errorf("batch2.states = %v, want = %v", batch2.states, wantState2)
	}

	gotMemoryStore = dumpMemoryStore(ctx, memoryStore)

	if !dataEqual(true, gotMemoryStore, wantMemoryStore) {
		t.Errorf("memory store = %#v, want = %#v", gotMemoryStore, wantMemoryStore)
	}
}

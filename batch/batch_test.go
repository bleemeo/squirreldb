package batch

import (
	"context"
	"reflect"
	"squirreldb/compare"
	"squirreldb/memorystore"
	"squirreldb/types"
	"testing"
	"time"
)

const (
	MetricIDTest1 = 1
	MetricIDTest2 = 2
)

func newMemoryStore(initialData []types.MetricData) *memorystore.Store {
	store := memorystore.New()
	store.Append(initialData)
	return store
}

func newMemoryStoreOffset(initialData []types.MetricData, offsets []int) *memorystore.Store {
	store := memorystore.New()
	store.GetSetPointsAndOffset(initialData, offsets)
	return store
}

func generatePoint(fromTS int, toTS int, step int) []types.MetricPoint {
	points := make([]types.MetricPoint, 0)
	for ts := fromTS; ts <= toTS; ts += step {
		points = append(points, types.MetricPoint{
			Timestamp: int64(ts) * 1000,
		})
	}
	return points
}

func dumpMemoryStore(store TemporaryStore) []types.MetricData {
	idsMap, _ := store.GetAllKnownMetrics()

	ids := make([]types.MetricID, 0, len(idsMap))

	for id := range idsMap {
		ids = append(ids, id)
	}

	results, _, _ := store.ReadPointsAndOffset(ids)
	return results
}

func metricsToMap(metrics []types.MetricData) map[types.MetricID]types.MetricData {
	metricsMap := make(map[types.MetricID]types.MetricData)
	for _, data := range metrics {
		metricsMap[data.ID] = data
	}

	return metricsMap
}

type mockStore struct {
	metrics map[types.MetricID]types.MetricData
}

type mockMetricReader struct {
	metrics map[types.MetricID]types.MetricData
}

type mockMetricWriter struct {
	metrics    map[types.MetricID]types.MetricData
	writeCount int
}

func (m *mockStore) Append(newMetrics, existingMetrics []types.MetricData, _ int64) error {
	for _, data := range newMetrics {
		storeData := m.metrics[data.ID]

		storeData.Points = append(storeData.Points, data.Points...)
		storeData.TimeToLive = compare.MaxInt64(storeData.TimeToLive, data.TimeToLive)

		m.metrics[data.ID] = storeData
	}

	for _, data := range existingMetrics {
		storeData := m.metrics[data.ID]

		storeData.Points = append(storeData.Points, data.Points...)
		storeData.TimeToLive = compare.MaxInt64(storeData.TimeToLive, data.TimeToLive)

		m.metrics[data.ID] = storeData
	}

	return nil
}

func (m *mockStore) Get(ids []types.MetricID) (map[types.MetricID]types.MetricData, error) {
	metrics := make(map[types.MetricID]types.MetricData)

	for _, id := range ids {
		storeData, exists := m.metrics[id]

		if exists {
			metrics[id] = storeData
		}
	}

	return metrics, nil
}

func (m *mockStore) Set(metrics []types.MetricData, _ int64) error {
	for _, data := range metrics {
		m.metrics[data.ID] = data
	}

	return nil
}

func (m *mockMetricReader) Read(request types.MetricRequest) (map[types.MetricID]types.MetricData, error) {
	metrics := make(map[types.MetricID]types.MetricData)

	for _, id := range request.IDs {
		data, exists := m.metrics[id]

		if exists {
			metrics[id] = data
		}
	}

	return metrics, nil
}

func (m *mockMetricWriter) Write(metrics []types.MetricData) error {
	m.writeCount++

	if len(metrics) == 0 {
		return nil
	}

	m.metrics = make(map[types.MetricID]types.MetricData)
	for _, data := range metrics {
		m.metrics[data.ID] = data
	}

	return nil
}

func TestBatch_read(t *testing.T) {
	type fields struct {
		batchSize   time.Duration
		states      map[types.MetricID]stateData
		memoryStore TemporaryStore
		reader      types.MetricReader
		writer      types.MetricWriter
	}
	type args struct {
		request types.MetricRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[types.MetricID]types.MetricData
		wantErr bool
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
				reader: &mockMetricReader{
					metrics: map[types.MetricID]types.MetricData{
						MetricIDTest1: {
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
						MetricIDTest2: {
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
				},
				writer: nil,
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
			want: map[types.MetricID]types.MetricData{
				MetricIDTest1: {
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
				MetricIDTest2: {
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
				reader: &mockMetricReader{
					metrics: nil,
				},
				writer: nil,
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
			want: map[types.MetricID]types.MetricData{
				MetricIDTest1: {
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
				MetricIDTest2: {
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
				reader: &mockMetricReader{
					metrics: map[types.MetricID]types.MetricData{
						MetricIDTest1: {
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
						MetricIDTest2: {
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
				},
				writer: nil,
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
			want: map[types.MetricID]types.MetricData{
				MetricIDTest1: {
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
				MetricIDTest2: {
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
				batchSize:   50 * time.Second,
				states:      nil,
				memoryStore: newMemoryStore(nil),
				reader: &mockMetricReader{
					metrics: nil,
				},
				writer: nil,
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
			want:    map[types.MetricID]types.MetricData{},
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
				reader: &mockMetricReader{
					metrics: map[types.MetricID]types.MetricData{
						MetricIDTest1: {
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
				},
				writer: nil,
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
			want: map[types.MetricID]types.MetricData{
				MetricIDTest1: {
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
			}
			got, err := b.read(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("read() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatch_readTemporary(t *testing.T) {
	type fields struct {
		batchSize   time.Duration
		states      map[types.MetricID]stateData
		memoryStore TemporaryStore
		reader      types.MetricReader
		writer      types.MetricWriter
	}
	type args struct {
		request types.MetricRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[types.MetricID]types.MetricData
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
			want: map[types.MetricID]types.MetricData{
				MetricIDTest1: {
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
				MetricIDTest2: {
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
			want:    make(map[types.MetricID]types.MetricData),
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
			}
			got, err := b.readTemporary(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("readTemporary() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readTemporary() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_flushTimestamp(t *testing.T) {
	type args struct {
		id        types.MetricID
		now       time.Time
		batchSize time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Time
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

func TestBatch_flush(t *testing.T) {
	type fields struct {
		batchSize   time.Duration
		states      map[types.MetricID]stateData
		memoryStore TemporaryStore
		writer      *mockMetricWriter
	}
	type args struct {
		ids      []types.MetricID
		now      time.Time
		shutdown bool
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantWriter      []types.MetricData
		wantMemoryStore []types.MetricData
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
				writer: &mockMetricWriter{},
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
				writer: &mockMetricWriter{},
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
				writer: &mockMetricWriter{},
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
				writer: &mockMetricWriter{},
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
				writer: &mockMetricWriter{},
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
			}
			b.flush(tt.args.ids, tt.args.now, tt.args.shutdown)

			wantWriter := metricsToMap(tt.wantWriter)
			if !reflect.DeepEqual(tt.fields.writer.metrics, wantWriter) {
				t.Errorf("writer = %v, want = %v", tt.fields.writer.metrics, wantWriter)
			}

			gotMemoryStore := metricsToMap(dumpMemoryStore(tt.fields.memoryStore))
			wantMemoryStore := metricsToMap(tt.wantMemoryStore)
			if !reflect.DeepEqual(gotMemoryStore, wantMemoryStore) {
				t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
			}
		})
	}
}

// TestBatch_write test behavior of two SquirrelDB sharing the same
// memoryStore (e.g. Redis)
func TestBatch_write(t *testing.T) {

	batchSize := 100 * time.Second
	memoryStore := memorystore.New()
	writer1 := &mockMetricWriter{
		metrics: map[types.MetricID]types.MetricData{},
	}
	writer2 := &mockMetricWriter{
		metrics: map[types.MetricID]types.MetricData{},
	}
	batch1 := New(batchSize, memoryStore, nil, writer1)
	batch2 := New(batchSize, memoryStore, nil, writer2)

	type args struct {
		ids      []types.MetricID
		now      time.Time
		shutdown bool
	}

	// tests case will reuse batch1 & batch2.
	// When write1 is not nil, sent it to batch1. Then after does the same with
	// write2 and batch2.
	// If the want* is nil, it means don't test.
	tests := []struct {
		name string

		nowWrite1 time.Time
		nowWrite2 time.Time
		write1    []types.MetricData
		write2    []types.MetricData

		nowCheck1 time.Time
		nowCheck2 time.Time
		shutdown1 bool
		shutdown2 bool

		wantWriter1     []types.MetricData
		wantWriter2     []types.MetricData
		wantMemoryStore []types.MetricData
		wantState1      map[types.MetricID]stateData
		wantState2      map[types.MetricID]stateData
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
				MetricIDTest1: stateData{
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
				MetricIDTest1: stateData{
					flushDeadline: time.Unix(299, 0),
				},
				MetricIDTest2: stateData{
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
				MetricIDTest1: stateData{
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(300, 0),
						batchSize,
					),
				},
				MetricIDTest2: stateData{
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
				MetricIDTest1: stateData{
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(300, 0),
						batchSize,
					),
				},
				MetricIDTest2: stateData{
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
				MetricIDTest1: stateData{
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(1000, 0),
						batchSize,
					),
				},
			},
			wantState2: map[types.MetricID]stateData{
				MetricIDTest2: stateData{
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
				MetricIDTest1: stateData{
					flushDeadline: flushTimestamp(
						MetricIDTest1,
						time.Unix(1100, 0),
						batchSize,
					),
				},
			},
			wantState2: map[types.MetricID]stateData{
				MetricIDTest2: stateData{
					flushDeadline: flushTimestamp(
						MetricIDTest2,
						time.Unix(1100, 0),
						batchSize,
					),
				},
			},
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		ok := t.Run(tt.name, func(t *testing.T) {
			if tt.write1 != nil {
				if err := batch1.write(tt.write1, tt.nowWrite1); err != nil {
					t.Errorf("batch1.write: %v", err)
					return
				}
			}

			if tt.write2 != nil {
				if err := batch2.write(tt.write2, tt.nowWrite2); err != nil {
					t.Errorf("batch2.write: %v", err)
					return
				}
			}

			if !tt.nowCheck1.IsZero() {
				batch1.check(ctx, tt.nowCheck1, tt.shutdown1, tt.shutdown1)
			}

			if !tt.nowCheck2.IsZero() {
				batch2.check(ctx, tt.nowCheck2, tt.shutdown2, tt.shutdown2)
			}

			if tt.wantWriter1 != nil {
				wantWriter := metricsToMap(tt.wantWriter1)
				if !reflect.DeepEqual(writer1.metrics, wantWriter) {
					t.Errorf("writer1 = %v, want = %v", writer1.metrics, wantWriter)
				}
			}

			if tt.wantState1 != nil {
				if !reflect.DeepEqual(batch1.states, tt.wantState1) {
					t.Errorf("state1 = %v, want = %v", batch1.states, tt.wantState1)
				}
			}

			if tt.wantWriter2 != nil {
				wantWriter := metricsToMap(tt.wantWriter2)
				if !reflect.DeepEqual(writer2.metrics, wantWriter) {
					t.Errorf("writer2 = %v, want = %v", writer2.metrics, wantWriter)
				}
			}

			if tt.wantState2 != nil {
				if !reflect.DeepEqual(batch2.states, tt.wantState2) {
					t.Errorf("state2 = %v, want = %v", batch2.states, tt.wantState2)
				}
			}

			if tt.wantMemoryStore != nil {
				gotMemoryStore := metricsToMap(dumpMemoryStore(memoryStore))
				wantMemoryStore := metricsToMap(tt.wantMemoryStore)
				if !reflect.DeepEqual(gotMemoryStore, wantMemoryStore) {
					t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
				}
			}

			if tt.shutdown1 {
				batch1 = New(batchSize, memoryStore, nil, writer1)
			}
			if tt.shutdown2 {
				batch2 = New(batchSize, memoryStore, nil, writer2)
			}

			writer1.metrics = map[types.MetricID]types.MetricData{}
			writer2.metrics = map[types.MetricID]types.MetricData{}
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

func Test_takeover(t *testing.T) {
	batchSize := 100 * time.Second
	memoryStore := memorystore.New()
	writer1 := &mockMetricWriter{
		metrics: map[types.MetricID]types.MetricData{},
	}
	writer2 := &mockMetricWriter{
		metrics: map[types.MetricID]types.MetricData{},
	}
	batch1 := New(batchSize, memoryStore, nil, writer1)
	batch2 := New(batchSize, memoryStore, nil, writer2)
	ctx := context.Background()

	batch1.write(
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
	batch2.write(
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
	batch1.write(
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

	wantWriter1 := metricsToMap([]types.MetricData{})
	wantWriter2 := metricsToMap([]types.MetricData{})
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
	wantMemoryStore := metricsToMap([]types.MetricData{
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
	})

	if !reflect.DeepEqual(writer1.metrics, wantWriter1) {
		t.Errorf("writer1.metrics = %v, want %v", writer1.metrics, wantWriter1)
	}
	if !reflect.DeepEqual(writer2.metrics, wantWriter2) {
		t.Errorf("writer2.metrics = %v, want %v", writer2.metrics, wantWriter2)
	}
	if !reflect.DeepEqual(batch1.states, wantState1) {
		t.Errorf("batch1.states = %v, want = %v", batch1.states, wantState1)
	}
	if !reflect.DeepEqual(batch2.states, wantState2) {
		t.Errorf("batch2.states = %v, want = %v", batch2.states, wantState2)
	}
	gotMemoryStore := metricsToMap(dumpMemoryStore(memoryStore))
	if !reflect.DeepEqual(gotMemoryStore, wantMemoryStore) {
		t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
	}

	// No takeover yet, but batch2 will flush its metric
	now := time.Unix(0, 0).Add(overdueThreshold)
	batch2.checkTakeover(ctx, now)
	batch2.check(ctx, now, false, false)

	wantWriter2 = metricsToMap([]types.MetricData{
		{
			ID:         MetricIDTest2,
			TimeToLive: 42,
			Points: []types.MetricPoint{
				{Timestamp: 10000},
				{Timestamp: 21000},
			},
		},
	})
	wantState2 = map[types.MetricID]stateData{}
	wantMemoryStore = metricsToMap([]types.MetricData{
		{
			ID:         MetricIDTest1,
			TimeToLive: 42,
			Points: []types.MetricPoint{
				{Timestamp: 10000},
				{Timestamp: 20000},
			},
		},
	})

	if !reflect.DeepEqual(writer1.metrics, wantWriter1) {
		t.Errorf("writer1.metrics = %v, want %v", writer1.metrics, wantWriter1)
	}
	if !reflect.DeepEqual(writer2.metrics, wantWriter2) {
		t.Errorf("writer2.metrics = %v, want %v", writer2.metrics, wantWriter2)
	}
	if !reflect.DeepEqual(batch1.states, wantState1) {
		t.Errorf("batch1.states = %v, want = %v", batch1.states, wantState1)
	}
	if !reflect.DeepEqual(batch2.states, wantState2) {
		t.Errorf("batch2.states = %v, want = %v", batch2.states, wantState2)
	}
	gotMemoryStore = metricsToMap(dumpMemoryStore(memoryStore))
	if !reflect.DeepEqual(gotMemoryStore, wantMemoryStore) {
		t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
	}

	// Taking over
	now = time.Unix(100, 0).Add(overdueThreshold)
	batch2.checkTakeover(ctx, now)
	batch2.check(ctx, now, false, false)

	wantWriter2 = metricsToMap([]types.MetricData{
		{
			ID:         MetricIDTest1,
			TimeToLive: 42,
			Points: []types.MetricPoint{
				{Timestamp: 10000},
				{Timestamp: 20000},
			},
		},
	})
	wantMemoryStore = metricsToMap([]types.MetricData{})

	if !reflect.DeepEqual(writer1.metrics, wantWriter1) {
		t.Errorf("writer1.metrics = %v, want %v", writer1.metrics, wantWriter1)
	}
	if !reflect.DeepEqual(writer2.metrics, wantWriter2) {
		t.Errorf("writer2.metrics = %v, want %v", writer2.metrics, wantWriter2)
	}
	if !reflect.DeepEqual(batch1.states, wantState1) {
		t.Errorf("batch1.states = %v, want = %v", batch1.states, wantState1)
	}
	if !reflect.DeepEqual(batch2.states, wantState2) {
		t.Errorf("batch2.states = %v, want = %v", batch2.states, wantState2)
	}
	gotMemoryStore = metricsToMap(dumpMemoryStore(memoryStore))
	if !reflect.DeepEqual(gotMemoryStore, wantMemoryStore) {
		t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
	}

	// batch1 will realize that a takeover happened
	batch1.check(ctx, now, false, false)
	wantState1 = map[types.MetricID]stateData{}

	if !reflect.DeepEqual(writer1.metrics, wantWriter1) {
		t.Errorf("writer1.metrics = %v, want %v", writer1.metrics, wantWriter1)
	}
	if !reflect.DeepEqual(writer2.metrics, wantWriter2) {
		t.Errorf("writer2.metrics = %v, want %v", writer2.metrics, wantWriter2)
	}
	if !reflect.DeepEqual(batch1.states, wantState1) {
		t.Errorf("batch1.states = %v, want = %v", batch1.states, wantState1)
	}
	if !reflect.DeepEqual(batch2.states, wantState2) {
		t.Errorf("batch2.states = %v, want = %v", batch2.states, wantState2)
	}
	gotMemoryStore = metricsToMap(dumpMemoryStore(memoryStore))
	if !reflect.DeepEqual(gotMemoryStore, wantMemoryStore) {
		t.Errorf("memory store = %v, want = %v", gotMemoryStore, wantMemoryStore)
	}

}

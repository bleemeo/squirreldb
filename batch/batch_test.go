package batch

import (
	"reflect"
	"squirreldb/compare"
	"squirreldb/types"
	"testing"
	"time"
)

type mockStore struct {
	metrics map[types.MetricUUID]types.MetricData
}

type mockMetricReader struct {
	metrics map[types.MetricUUID]types.MetricData
}

type mockMetricWriter struct {
	metrics map[types.MetricUUID]types.MetricData
}

func (m *mockStore) Append(newMetrics, existingMetrics map[types.MetricUUID]types.MetricData, _ int64) error {
	for uuid, data := range newMetrics {
		storeData := m.metrics[uuid]

		storeData.Points = append(storeData.Points, data.Points...)
		storeData.TimeToLive = compare.MaxInt64(storeData.TimeToLive, data.TimeToLive)

		m.metrics[uuid] = storeData
	}

	for uuid, data := range existingMetrics {
		storeData := m.metrics[uuid]

		storeData.Points = append(storeData.Points, data.Points...)
		storeData.TimeToLive = compare.MaxInt64(storeData.TimeToLive, data.TimeToLive)

		m.metrics[uuid] = storeData
	}

	return nil
}

func (m *mockStore) Get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error) {
	metrics := make(map[types.MetricUUID]types.MetricData)

	for _, uuid := range uuids {
		storeData, exists := m.metrics[uuid]

		if exists {
			metrics[uuid] = storeData
		}
	}

	return metrics, nil
}

func (m *mockStore) Set(metrics map[types.MetricUUID]types.MetricData, _ int64) error {
	for uuid, data := range metrics {
		m.metrics[uuid] = data
	}

	return nil
}

func (m *mockMetricReader) Read(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	metrics := make(map[types.MetricUUID]types.MetricData)

	for _, uuid := range request.UUIDs {
		data, exists := m.metrics[uuid]

		if exists {
			metrics[uuid] = data
		}
	}

	return metrics, nil
}

func (m *mockMetricWriter) Write(metrics map[types.MetricUUID]types.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}

	m.metrics = metrics

	return nil
}

func uuidFromStringOrNil(s string) types.MetricUUID {
	uuid, _ := types.UUIDFromString(s)

	return uuid
}

func TestBatch_check(t *testing.T) {
	type fields struct {
		batchSize   int64
		states      map[types.MetricUUID]stateData
		memoryStore Store
		reader      types.MetricReader
		writer      types.MetricWriter
	}
	type args struct {
		now   time.Time
		force bool
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantStorer map[types.MetricUUID]types.MetricData
		wantWriter map[types.MetricUUID]types.MetricData
	}{
		{
			name: "states_filled_store_filled_no_forced",
			fields: fields{
				batchSize: 50,
				states: map[types.MetricUUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          5,
						firstPointTimestamp: 0,
						lastPointTimestamp:  40,
						flushTimestamp:      50,
					},
				},
				memoryStore: &mockStore{
					metrics: map[types.MetricUUID]types.MetricData{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
								{
									Timestamp: 20,
									Value:     30,
								},
								{
									Timestamp: 30,
									Value:     40,
								},
								{
									Timestamp: 40,
									Value:     50,
								},
								{
									Timestamp: 50,
									Value:     60,
								},
								{
									Timestamp: 60,
									Value:     70,
								},
							},
							TimeToLive: 300,
						},
					},
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				now:   time.Unix(60, 0),
				force: false,
			},
			wantStorer: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
					TimeToLive: 300,
				},
			},
			wantWriter: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
					},
					TimeToLive: 300,
				},
			},
		},
		{
			name: "states_filled_store_filled_forced",
			fields: fields{
				batchSize: 50,
				states: map[types.MetricUUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          2,
						firstPointTimestamp: 50,
						lastPointTimestamp:  60,
						flushTimestamp:      100,
					},
				},
				memoryStore: &mockStore{
					metrics: map[types.MetricUUID]types.MetricData{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
								{
									Timestamp: 20,
									Value:     30,
								},
								{
									Timestamp: 30,
									Value:     40,
								},
								{
									Timestamp: 40,
									Value:     50,
								},
								{
									Timestamp: 50,
									Value:     60,
								},
								{
									Timestamp: 60,
									Value:     70,
								},
							},
							TimeToLive: 300,
						},
					},
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				now:   time.Unix(60, 0),
				force: true,
			},
			wantStorer: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
					TimeToLive: 300,
				},
			},
			wantWriter: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
					TimeToLive: 300,
				},
			},
		},
		{
			name: "states_filled_store_empty",
			fields: fields{
				batchSize: 50,
				states: map[types.MetricUUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          5,
						firstPointTimestamp: 0,
						lastPointTimestamp:  40,
						flushTimestamp:      50,
					},
				},
				memoryStore: &mockStore{
					metrics: make(map[types.MetricUUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				now:   time.Unix(60, 0),
				force: false,
			},
			wantStorer: make(map[types.MetricUUID]types.MetricData),
			wantWriter: nil,
		},
		{
			name: "states_empty_store_empty",
			fields: fields{
				batchSize: 50,
				states:    make(map[types.MetricUUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[types.MetricUUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				now:   time.Unix(60, 0),
				force: false,
			},
			wantStorer: make(map[types.MetricUUID]types.MetricData),
			wantWriter: nil,
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
			b.check(tt.args.now, tt.args.force)
			gotStorer := b.memoryStore.(*mockStore).metrics
			gotWriter := b.writer.(*mockMetricWriter).metrics
			if !reflect.DeepEqual(gotStorer, tt.wantStorer) {
				t.Errorf("check() gotStorer = %v, wantStorer %v", gotStorer, tt.wantStorer)
			}
			if !reflect.DeepEqual(gotWriter, tt.wantWriter) {
				t.Errorf("check() gotWriter = %v, wantWriter %v", gotWriter, tt.wantWriter)
			}
		})
	}
}

func TestBatch_flush(t *testing.T) {
	type fields struct {
		batchSize   int64
		states      map[types.MetricUUID]stateData
		memoryStore Store
		reader      types.MetricReader
		writer      types.MetricWriter
	}
	type args struct {
		states map[types.MetricUUID][]stateData
		now    time.Time
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		storerWant map[types.MetricUUID]types.MetricData
		writerWant map[types.MetricUUID]types.MetricData
	}{
		{
			name: "batch_states_filled_store_filled_states_filled",
			fields: fields{
				batchSize: 50,
				states: map[types.MetricUUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          2,
						firstPointTimestamp: 50,
						lastPointTimestamp:  60,
						flushTimestamp:      100,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						pointCount:          2,
						firstPointTimestamp: 100,
						lastPointTimestamp:  120,
						flushTimestamp:      150,
					},
				},
				memoryStore: &mockStore{
					metrics: map[types.MetricUUID]types.MetricData{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
								{
									Timestamp: 20,
									Value:     30,
								},
								{
									Timestamp: 30,
									Value:     40,
								},
								{
									Timestamp: 40,
									Value:     50,
								},
								{
									Timestamp: 50,
									Value:     60,
								},
								{
									Timestamp: 60,
									Value:     70,
								},
							},
							TimeToLive: 300,
						},
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20,
									Value:     100,
								},
								{
									Timestamp: 40,
									Value:     150,
								},
								{
									Timestamp: 60,
									Value:     200,
								},
								{
									Timestamp: 80,
									Value:     250,
								},
								{
									Timestamp: 100,
									Value:     300,
								},
								{
									Timestamp: 120,
									Value:     350,
								},
							},
							TimeToLive: 1200,
						},
					},
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				states: map[types.MetricUUID][]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						{
							pointCount:          5,
							firstPointTimestamp: 0,
							lastPointTimestamp:  40,
							flushTimestamp:      50,
						},
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						{
							pointCount:          3,
							firstPointTimestamp: 0,
							lastPointTimestamp:  40,
							flushTimestamp:      50,
						},
						{
							pointCount:          2,
							firstPointTimestamp: 60,
							lastPointTimestamp:  80,
							flushTimestamp:      50,
						},
					},
				},
				now: time.Unix(60, 0),
			},
			storerWant: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
					TimeToLive: 300,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 20,
							Value:     100,
						},
						{
							Timestamp: 40,
							Value:     150,
						},
						{
							Timestamp: 60,
							Value:     200,
						},
						{
							Timestamp: 80,
							Value:     250,
						},
						{
							Timestamp: 100,
							Value:     300,
						},
						{
							Timestamp: 120,
							Value:     350,
						},
					},
					TimeToLive: 1200,
				},
			},
			writerWant: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
					},
					TimeToLive: 300,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     50,
						},
						{
							Timestamp: 20,
							Value:     100,
						},
						{
							Timestamp: 40,
							Value:     150,
						},
						{
							Timestamp: 60,
							Value:     200,
						},
						{
							Timestamp: 80,
							Value:     250,
						},
					},
					TimeToLive: 1200,
				},
			},
		},
		{
			name: "batch_states_filled_store_empty_states_filled",
			fields: fields{
				batchSize: 50,
				states: map[types.MetricUUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          2,
						firstPointTimestamp: 50,
						lastPointTimestamp:  60,
						flushTimestamp:      100,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						pointCount:          2,
						firstPointTimestamp: 100,
						lastPointTimestamp:  120,
						flushTimestamp:      150,
					},
				},
				memoryStore: &mockStore{
					metrics: make(map[types.MetricUUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				states: map[types.MetricUUID][]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						{
							pointCount:          5,
							firstPointTimestamp: 0,
							lastPointTimestamp:  40,
							flushTimestamp:      50,
						},
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						{
							pointCount:          3,
							firstPointTimestamp: 0,
							lastPointTimestamp:  40,
							flushTimestamp:      50,
						},
						{
							pointCount:          2,
							firstPointTimestamp: 60,
							lastPointTimestamp:  80,
							flushTimestamp:      50,
						},
					},
				},
				now: time.Unix(60, 0),
			},
			storerWant: make(map[types.MetricUUID]types.MetricData),
			writerWant: nil,
		},
		{
			name: "batch_states_empty_store_empty_states_filled",
			fields: fields{
				batchSize: 50,
				states:    make(map[types.MetricUUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[types.MetricUUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				states: map[types.MetricUUID][]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						{
							pointCount:          5,
							firstPointTimestamp: 0,
							lastPointTimestamp:  40,
							flushTimestamp:      50,
						},
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						{
							pointCount:          3,
							firstPointTimestamp: 0,
							lastPointTimestamp:  40,
							flushTimestamp:      50,
						},
						{
							pointCount:          2,
							firstPointTimestamp: 60,
							lastPointTimestamp:  80,
							flushTimestamp:      50,
						},
					},
				},
				now: time.Unix(60, 0),
			},
			storerWant: make(map[types.MetricUUID]types.MetricData),
			writerWant: nil,
		},
		{
			name: "batch_states_empty_store_empty_states_empty",
			fields: fields{
				batchSize: 50,
				states:    make(map[types.MetricUUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[types.MetricUUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				states: nil,
				now:    time.Unix(60, 0),
			},
			storerWant: make(map[types.MetricUUID]types.MetricData),
			writerWant: nil,
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
			b.flush(tt.args.states, tt.args.now)
			storerGot := b.memoryStore.(*mockStore).metrics
			writerGot := b.writer.(*mockMetricWriter).metrics
			if !reflect.DeepEqual(storerGot, tt.storerWant) {
				t.Errorf("flush() storerGot = %v, storerWant %v", storerGot, tt.storerWant)
			}
			if !reflect.DeepEqual(writerGot, tt.writerWant) {
				t.Errorf("flush() writerGot = %v, writerWant %v", writerGot, tt.writerWant)
			}
		})
	}
}

func TestBatch_flushData(t *testing.T) {
	type fields struct {
		batchSize   int64
		states      map[types.MetricUUID]stateData
		memoryStore Store
		reader      types.MetricReader
		writer      types.MetricWriter
	}
	type args struct {
		uuid       types.MetricUUID
		data       types.MetricData
		statesData []stateData
		now        time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   types.MetricData
		want1  types.MetricData
	}{
		{
			name: "states_filled_points_filled_statesData_filled",
			fields: fields{
				batchSize: 50,
				states: map[types.MetricUUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          2,
						firstPointTimestamp: 50,
						lastPointTimestamp:  60,
						flushTimestamp:      100,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						pointCount:          2,
						firstPointTimestamp: 100,
						lastPointTimestamp:  120,
						flushTimestamp:      150,
					},
				},
				memoryStore: nil,
				reader:      nil,
				writer:      nil,
			},
			args: args{
				uuid: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				data: types.MetricData{
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
					TimeToLive: 300,
				},
				statesData: []stateData{
					{
						pointCount:          5,
						firstPointTimestamp: 0,
						lastPointTimestamp:  40,
						flushTimestamp:      50,
					},
				},
				now: time.Unix(60, 0),
			},
			want: types.MetricData{
				Points: []types.MetricPoint{
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 20,
						Value:     30,
					},
					{
						Timestamp: 30,
						Value:     40,
					},
					{
						Timestamp: 40,
						Value:     50,
					},
				},
				TimeToLive: 300,
			},
			want1: types.MetricData{
				Points: []types.MetricPoint{
					{
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 20,
						Value:     30,
					},
					{
						Timestamp: 30,
						Value:     40,
					},
					{
						Timestamp: 40,
						Value:     50,
					},
					{
						Timestamp: 50,
						Value:     60,
					},
					{
						Timestamp: 60,
						Value:     70,
					},
				},
				TimeToLive: 300,
			},
		},
		{
			name: "states_filled_points_empty_statesData_filled",
			fields: fields{
				batchSize: 50,
				states: map[types.MetricUUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          2,
						firstPointTimestamp: 50,
						lastPointTimestamp:  60,
						flushTimestamp:      100,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						pointCount:          2,
						firstPointTimestamp: 100,
						lastPointTimestamp:  120,
						flushTimestamp:      150,
					},
				},
				memoryStore: nil,
				reader:      nil,
				writer:      nil,
			},
			args: args{
				uuid: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				data: types.MetricData{
					Points:     nil,
					TimeToLive: 300,
				},
				statesData: []stateData{
					{
						pointCount:          5,
						firstPointTimestamp: 0,
						lastPointTimestamp:  40,
						flushTimestamp:      50,
					},
				},
				now: time.Unix(60, 0),
			},
			want: types.MetricData{
				Points:     nil,
				TimeToLive: 0,
			},
			want1: types.MetricData{
				Points:     nil,
				TimeToLive: 0,
			},
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
			got, got1 := b.flushData(tt.args.uuid, tt.args.data, tt.args.statesData, tt.args.now)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("flushData() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("flushData() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestBatch_read(t *testing.T) {
	type fields struct {
		batchSize   int64
		states      map[types.MetricUUID]stateData
		memoryStore Store
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
		want    map[types.MetricUUID]types.MetricData
		wantErr bool
	}{
		{
			name: "temporary_filled_persistent_filled",
			fields: fields{
				batchSize: 50,
				states:    nil,
				memoryStore: &mockStore{
					metrics: map[types.MetricUUID]types.MetricData{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 50,
									Value:     60,
								},
								{
									Timestamp: 60,
									Value:     70,
								},
							},
						},
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 100,
									Value:     300,
								},
								{
									Timestamp: 120,
									Value:     350,
								},
							},
						},
					},
				},
				reader: &mockMetricReader{
					metrics: map[types.MetricUUID]types.MetricData{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
								{
									Timestamp: 20,
									Value:     30,
								},
								{
									Timestamp: 30,
									Value:     40,
								},
								{
									Timestamp: 40,
									Value:     50,
								},
							},
						},
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20,
									Value:     100,
								},
								{
									Timestamp: 40,
									Value:     150,
								},
								{
									Timestamp: 60,
									Value:     200,
								},
								{
									Timestamp: 80,
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
					UUIDs: []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     50,
						},
						{
							Timestamp: 20,
							Value:     100,
						},
						{
							Timestamp: 40,
							Value:     150,
						},
						{
							Timestamp: 60,
							Value:     200,
						},
						{
							Timestamp: 80,
							Value:     250,
						},
						{
							Timestamp: 100,
							Value:     300,
						},
						{
							Timestamp: 120,
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
				batchSize: 50,
				states:    nil,
				memoryStore: &mockStore{
					metrics: map[types.MetricUUID]types.MetricData{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 50,
									Value:     60,
								},
								{
									Timestamp: 60,
									Value:     70,
								},
							},
						},
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 100,
									Value:     300,
								},
								{
									Timestamp: 120,
									Value:     350,
								},
							},
						},
					},
				},
				reader: &mockMetricReader{
					metrics: nil,
				},
				writer: nil,
			},
			args: args{
				request: types.MetricRequest{
					UUIDs: []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 100,
							Value:     300,
						},
						{
							Timestamp: 120,
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
				batchSize: 50,
				states:    nil,
				memoryStore: &mockStore{
					metrics: nil,
				},
				reader: &mockMetricReader{
					metrics: map[types.MetricUUID]types.MetricData{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
								{
									Timestamp: 20,
									Value:     30,
								},
								{
									Timestamp: 30,
									Value:     40,
								},
								{
									Timestamp: 40,
									Value:     50,
								},
							},
						},
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20,
									Value:     100,
								},
								{
									Timestamp: 40,
									Value:     150,
								},
								{
									Timestamp: 60,
									Value:     200,
								},
								{
									Timestamp: 80,
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
					UUIDs: []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
					},
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     50,
						},
						{
							Timestamp: 20,
							Value:     100,
						},
						{
							Timestamp: 40,
							Value:     150,
						},
						{
							Timestamp: 60,
							Value:     200,
						},
						{
							Timestamp: 80,
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
				batchSize: 50,
				states:    nil,
				memoryStore: &mockStore{
					metrics: nil,
				},
				reader: &mockMetricReader{
					metrics: nil,
				},
				writer: nil,
			},
			args: args{
				request: types.MetricRequest{
					UUIDs: []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want:    map[types.MetricUUID]types.MetricData{},
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
		batchSize   int64
		states      map[types.MetricUUID]stateData
		memoryStore Store
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
		want    map[types.MetricUUID]types.MetricData
		wantErr bool
	}{
		{
			name: "temporary_filled",
			fields: fields{
				batchSize: 0,
				states:    nil,
				memoryStore: &mockStore{
					metrics: map[types.MetricUUID]types.MetricData{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 50,
									Value:     60,
								},
								{
									Timestamp: 60,
									Value:     70,
								},
							},
						},
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
							Points: []types.MetricPoint{
								{
									Timestamp: 100,
									Value:     300,
								},
								{
									Timestamp: 120,
									Value:     350,
								},
							},
						},
					},
				},
				reader: nil,
				writer: nil,
			},
			args: args{
				request: types.MetricRequest{
					UUIDs: []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 100,
							Value:     300,
						},
						{
							Timestamp: 120,
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
				batchSize: 0,
				states:    nil,
				memoryStore: &mockStore{
					metrics: nil,
				},
				reader: nil,
				writer: nil,
			},
			args: args{
				request: types.MetricRequest{
					UUIDs: []types.MetricUUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want:    make(map[types.MetricUUID]types.MetricData),
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

func TestBatch_write(t *testing.T) {
	type fields struct {
		batchSize   int64
		states      map[types.MetricUUID]stateData
		memoryStore Store
		reader      types.MetricReader
		writer      types.MetricWriter
	}
	type args struct {
		metrics map[types.MetricUUID]types.MetricData
		now     time.Time
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantStates map[types.MetricUUID]stateData
		wantStorer map[types.MetricUUID]types.MetricData
		wantWriter map[types.MetricUUID]types.MetricData
	}{
		{
			name: "metrics_filled",
			fields: fields{
				batchSize: 50,
				states:    make(map[types.MetricUUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[types.MetricUUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				metrics: map[types.MetricUUID]types.MetricData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
							{
								Timestamp: 50,
								Value:     60,
							},
							{
								Timestamp: 60,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
							{
								Timestamp: 100,
								Value:     300,
							},
							{
								Timestamp: 120,
								Value:     350,
							},
						},
						TimeToLive: 1200,
					},
				},
				now: time.Unix(60, 0),
			},
			wantStates: map[types.MetricUUID]stateData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					pointCount:          2,
					firstPointTimestamp: 50,
					lastPointTimestamp:  60,
					flushTimestamp:      99,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					pointCount:          1,
					firstPointTimestamp: 120,
					lastPointTimestamp:  120,
					flushTimestamp:      98,
				},
			},
			wantStorer: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 50,
							Value:     60,
						},
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
						{
							Timestamp: 60,
							Value:     70,
						},
					},
					TimeToLive: 300,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 60,
							Value:     200,
						},
						{
							Timestamp: 120,
							Value:     350,
						},
						{
							Timestamp: 20,
							Value:     100,
						},
						{
							Timestamp: 40,
							Value:     150,
						},
						{
							Timestamp: 80,
							Value:     250,
						},
						{
							Timestamp: 100,
							Value:     300,
						},
					},
					TimeToLive: 1200,
				},
			},
			wantWriter: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
					},
					TimeToLive: 300,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     50,
						},
						{
							Timestamp: 60,
							Value:     200,
						},
						{
							Timestamp: 20,
							Value:     100,
						},
						{
							Timestamp: 40,
							Value:     150,
						},
						{
							Timestamp: 80,
							Value:     250,
						},
						{
							Timestamp: 100,
							Value:     300,
						},
					},
					TimeToLive: 1200,
				},
			},
		},
		{
			name: "metrics_empty",
			fields: fields{
				batchSize: 50,
				states:    make(map[types.MetricUUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[types.MetricUUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				metrics: nil,
				now:     time.Unix(0, 0),
			},
			wantStates: make(map[types.MetricUUID]stateData),
			wantStorer: make(map[types.MetricUUID]types.MetricData),
			wantWriter: nil,
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
			b.write(tt.args.metrics, tt.args.now)
			gotStates := b.states
			gotStorer := b.memoryStore.(*mockStore).metrics
			gotWriter := b.writer.(*mockMetricWriter).metrics
			if !reflect.DeepEqual(gotStates, tt.wantStates) {
				t.Errorf("write() gotStates = %v, wantStates %v", gotStates, tt.wantStates)
			}
			if !reflect.DeepEqual(gotStorer, tt.wantStorer) {
				t.Errorf("write() gotStorer = %v, wantStorer %v", gotStorer, tt.wantStorer)
			}
			if !reflect.DeepEqual(gotWriter, tt.wantWriter) {
				t.Errorf("write() gotWriter = %v, wantWriter %v", gotWriter, tt.wantWriter)
			}
		})
	}
}

func Test_flushTimestamp(t *testing.T) {
	type args struct {
		uuid      types.MetricUUID
		now       time.Time
		batchSize int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "uuid_0",
			args: args{
				uuid:      uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				now:       time.Unix(0, 0),
				batchSize: 50,
			},
			want: 49,
		},
		{
			name: "uuid_1707",
			args: args{
				uuid:      uuidFromStringOrNil("00000000-0000-0000-0000-0000000006ab"),
				now:       time.Unix(0, 0),
				batchSize: 50,
			},
			want: 43,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := flushTimestamp(tt.args.uuid, tt.args.now, tt.args.batchSize); got != tt.want {
				t.Errorf("flushTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

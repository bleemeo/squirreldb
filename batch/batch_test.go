package batch

import (
	gouuid "github.com/gofrs/uuid"
	"math/rand"
	"reflect"
	"squirreldb/compare"
	"squirreldb/types"
	"testing"
	"time"
)

type mockStore struct {
	metrics map[gouuid.UUID]types.MetricData
}

type mockMetricReader struct {
	metrics map[gouuid.UUID]types.MetricData
}

type mockMetricWriter struct {
	metrics map[gouuid.UUID]types.MetricData
}

func (m *mockStore) Append(newMetrics, existingMetrics map[gouuid.UUID]types.MetricData, _ int64) error {
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

func (m *mockStore) Get(uuids []gouuid.UUID) (map[gouuid.UUID]types.MetricData, error) {
	metrics := make(map[gouuid.UUID]types.MetricData)

	for _, uuid := range uuids {
		storeData, exists := m.metrics[uuid]

		if exists {
			metrics[uuid] = storeData
		}
	}

	return metrics, nil
}

func (m *mockStore) Set(metrics map[gouuid.UUID]types.MetricData, _ int64) error {
	for uuid, data := range metrics {
		m.metrics[uuid] = data
	}

	return nil
}

func (m *mockMetricReader) Read(request types.MetricRequest) (map[gouuid.UUID]types.MetricData, error) {
	metrics := make(map[gouuid.UUID]types.MetricData)

	for _, uuid := range request.UUIDs {
		data, exists := m.metrics[uuid]

		if exists {
			metrics[uuid] = data
		}
	}

	return metrics, nil
}

func (m *mockMetricWriter) Write(metrics map[gouuid.UUID]types.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}

	m.metrics = metrics

	return nil
}

func uuidFromStringOrNil(s string) gouuid.UUID {
	uuid, _ := gouuid.FromString(s)

	return uuid
}

func TestBatch_check(t *testing.T) {
	type fields struct {
		batchSize   int64
		states      map[gouuid.UUID]stateData
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
		wantStorer map[gouuid.UUID]types.MetricData
		wantWriter map[gouuid.UUID]types.MetricData
	}{
		{
			name: "states_filled_store_filled_no_forced",
			fields: fields{
				batchSize: 50,
				states: map[gouuid.UUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          5,
						firstPointTimestamp: 0,
						lastPointTimestamp:  40,
						flushTimestamp:      50,
					},
				},
				memoryStore: &mockStore{
					metrics: map[gouuid.UUID]types.MetricData{
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
			wantStorer: map[gouuid.UUID]types.MetricData{
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
			wantWriter: map[gouuid.UUID]types.MetricData{
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
				states: map[gouuid.UUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          2,
						firstPointTimestamp: 50,
						lastPointTimestamp:  60,
						flushTimestamp:      100,
					},
				},
				memoryStore: &mockStore{
					metrics: map[gouuid.UUID]types.MetricData{
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
			wantStorer: map[gouuid.UUID]types.MetricData{
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
			wantWriter: map[gouuid.UUID]types.MetricData{
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
				states: map[gouuid.UUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          5,
						firstPointTimestamp: 0,
						lastPointTimestamp:  40,
						flushTimestamp:      50,
					},
				},
				memoryStore: &mockStore{
					metrics: make(map[gouuid.UUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				now:   time.Unix(60, 0),
				force: false,
			},
			wantStorer: make(map[gouuid.UUID]types.MetricData),
			wantWriter: nil,
		},
		{
			name: "states_empty_store_empty",
			fields: fields{
				batchSize: 50,
				states:    make(map[gouuid.UUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[gouuid.UUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				now:   time.Unix(60, 0),
				force: false,
			},
			wantStorer: make(map[gouuid.UUID]types.MetricData),
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
		states      map[gouuid.UUID]stateData
		memoryStore Store
		reader      types.MetricReader
		writer      types.MetricWriter
	}
	type args struct {
		states map[gouuid.UUID][]stateData
		now    time.Time
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		storerWant map[gouuid.UUID]types.MetricData
		writerWant map[gouuid.UUID]types.MetricData
	}{
		{
			name: "batch_states_filled_store_filled_states_filled",
			fields: fields{
				batchSize: 50,
				states: map[gouuid.UUID]stateData{
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
					metrics: map[gouuid.UUID]types.MetricData{
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
				states: map[gouuid.UUID][]stateData{
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
			storerWant: map[gouuid.UUID]types.MetricData{
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
			writerWant: map[gouuid.UUID]types.MetricData{
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
				states: map[gouuid.UUID]stateData{
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
					metrics: make(map[gouuid.UUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				states: map[gouuid.UUID][]stateData{
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
			storerWant: make(map[gouuid.UUID]types.MetricData),
			writerWant: nil,
		},
		{
			name: "batch_states_empty_store_empty_states_filled",
			fields: fields{
				batchSize: 50,
				states:    make(map[gouuid.UUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[gouuid.UUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				states: map[gouuid.UUID][]stateData{
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
			storerWant: make(map[gouuid.UUID]types.MetricData),
			writerWant: nil,
		},
		{
			name: "batch_states_empty_store_empty_states_empty",
			fields: fields{
				batchSize: 50,
				states:    make(map[gouuid.UUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[gouuid.UUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				states: nil,
				now:    time.Unix(60, 0),
			},
			storerWant: make(map[gouuid.UUID]types.MetricData),
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
		batchSize int64
		states    map[gouuid.UUID]stateData
	}
	type args struct {
		uuid       gouuid.UUID
		data       types.MetricData
		statesData []stateData
		now        time.Time
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantWrite types.MetricData
		wantKeep  types.MetricData
	}{
		{
			name: "states_filled_points_filled_statesData_filled",
			fields: fields{
				batchSize: 50,
				states: map[gouuid.UUID]stateData{
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
			wantWrite: types.MetricData{
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
			wantKeep: types.MetricData{
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
				states: map[gouuid.UUID]stateData{
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
			wantKeep: types.MetricData{
				Points:     nil,
				TimeToLive: 0,
			},
			wantWrite: types.MetricData{
				Points:     nil,
				TimeToLive: 0,
			},
		},
		{
			name: "current_states_need_data",
			fields: fields{
				batchSize: 50,
				states: map[gouuid.UUID]stateData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						pointCount:          2,
						firstPointTimestamp: 50,
						lastPointTimestamp:  60,
						flushTimestamp:      100,
					},
				},
			},
			args: args{
				uuid: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				data: types.MetricData{
					Points: []types.MetricPoint{
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
						{
							Timestamp: 70,
							Value:     80,
						},
					},
					TimeToLive: 300,
				},
				statesData: []stateData{
					{
						pointCount:          1,
						firstPointTimestamp: 0,
						lastPointTimestamp:  49,
						flushTimestamp:      50,
					},
				},
				now: time.Unix(6000, 0),
			},
			wantWrite: types.MetricData{
				Points: []types.MetricPoint{
					{
						Timestamp: 40,
						Value:     50,
					},
				},
				TimeToLive: 300,
			},
			wantKeep: types.MetricData{
				Points: []types.MetricPoint{
					{
						Timestamp: 50,
						Value:     60,
					},
					{
						Timestamp: 60,
						Value:     70,
					},
					{
						Timestamp: 70,
						Value:     80,
					},
				},
				TimeToLive: 300,
			},
		},
		{
			name: "three_state",
			fields: fields{
				batchSize: 50,
				states:    nil,
			},
			args: args{
				data: types.MetricData{
					Points: []types.MetricPoint{
						{Timestamp: 10},
						{Timestamp: 20},
						{Timestamp: 30},
						{Timestamp: 40},
						{Timestamp: 50},
						{Timestamp: 60},
						{Timestamp: 70},
						{Timestamp: 80},
						{Timestamp: 90},
						{Timestamp: 100},
						{Timestamp: 110},
						{Timestamp: 120},
						{Timestamp: 130},
					},
					TimeToLive: 300,
				},
				statesData: []stateData{
					{
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  30,
					},
					{
						pointCount:          5,
						firstPointTimestamp: 42,
						lastPointTimestamp:  90,
					},
					{
						pointCount:          5,
						firstPointTimestamp: 80,
						lastPointTimestamp:  120,
					},
				},
				now: time.Unix(130+50, 0),
			},
			wantWrite: types.MetricData{
				Points: []types.MetricPoint{
					{Timestamp: 10},
					{Timestamp: 20},
					{Timestamp: 30},
					{Timestamp: 50},
					{Timestamp: 60},
					{Timestamp: 70},
					{Timestamp: 80},
					{Timestamp: 90},
					{Timestamp: 100},
					{Timestamp: 110},
					{Timestamp: 120},
				},
				TimeToLive: 300,
			},
			wantKeep: types.MetricData{
				Points: []types.MetricPoint{
					{Timestamp: 130},
				},
				TimeToLive: 300,
			},
		},
		{
			name: "unsorted_duplicate",
			fields: fields{
				batchSize: 50,
				states:    nil,
			},
			args: args{
				data: types.MetricData{
					Points: []types.MetricPoint{
						{Timestamp: 10},
						{Timestamp: 20},
						{Timestamp: 30},
						{Timestamp: 10},
						{Timestamp: 20},
						{Timestamp: 40},
						{Timestamp: 50},
						{Timestamp: 50},
						{Timestamp: 60},
						{Timestamp: 70},
						{Timestamp: 80},
						{Timestamp: 100},
						{Timestamp: 110},
						{Timestamp: 90},
						{Timestamp: 120},
						{Timestamp: 130},
					},
					TimeToLive: 300,
				},
				statesData: []stateData{
					{
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  30,
					},
					{
						pointCount:          5,
						firstPointTimestamp: 42,
						lastPointTimestamp:  90,
					},
					{
						pointCount:          5,
						firstPointTimestamp: 80,
						lastPointTimestamp:  120,
					},
				},
				now: time.Unix(130+50, 0),
			},
			wantWrite: types.MetricData{
				Points: []types.MetricPoint{
					{Timestamp: 10},
					{Timestamp: 20},
					{Timestamp: 30},
					{Timestamp: 50},
					{Timestamp: 60},
					{Timestamp: 70},
					{Timestamp: 80},
					{Timestamp: 90},
					{Timestamp: 100},
					{Timestamp: 110},
					{Timestamp: 120},
				},
				TimeToLive: 300,
			},
			wantKeep: types.MetricData{
				Points: []types.MetricPoint{
					{Timestamp: 130},
				},
				TimeToLive: 300,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				batchSize: tt.fields.batchSize,
				states:    tt.fields.states,
			}
			gotWrite, gotKeep := b.flushData(tt.args.uuid, tt.args.data, tt.args.statesData, tt.args.now)
			if !reflect.DeepEqual(gotWrite, tt.wantWrite) {
				t.Errorf("flushData() gotWrite = %v, want %v", gotWrite, tt.wantWrite)
			}
			if !reflect.DeepEqual(gotKeep, tt.wantKeep) {
				t.Errorf("flushData() gotKeep = %v, want %v", gotKeep, tt.wantKeep)
			}
		})
	}
}

func TestBatch_read(t *testing.T) {
	type fields struct {
		batchSize   int64
		states      map[gouuid.UUID]stateData
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
		want    map[gouuid.UUID]types.MetricData
		wantErr bool
	}{
		{
			name: "temporary_filled_persistent_filled",
			fields: fields{
				batchSize: 50,
				states:    nil,
				memoryStore: &mockStore{
					metrics: map[gouuid.UUID]types.MetricData{
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
					metrics: map[gouuid.UUID]types.MetricData{
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
					UUIDs: []gouuid.UUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want: map[gouuid.UUID]types.MetricData{
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
					metrics: map[gouuid.UUID]types.MetricData{
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
					UUIDs: []gouuid.UUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want: map[gouuid.UUID]types.MetricData{
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
					metrics: map[gouuid.UUID]types.MetricData{
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
					UUIDs: []gouuid.UUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want: map[gouuid.UUID]types.MetricData{
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
					UUIDs: []gouuid.UUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want:    map[gouuid.UUID]types.MetricData{},
			wantErr: false,
		},
		{
			name: "temporary_has_all_points",
			fields: fields{
				batchSize: 50,
				states:    nil,
				memoryStore: &mockStore{
					metrics: map[gouuid.UUID]types.MetricData{
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
					},
				},
				reader: &mockMetricReader{
					metrics: map[gouuid.UUID]types.MetricData{
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
					},
				},
				writer: nil,
			},
			args: args{
				request: types.MetricRequest{
					UUIDs: []gouuid.UUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					},
					FromTimestamp: 0,
					ToTimestamp:   100,
					Step:          0,
					Function:      "",
				},
			},
			want: map[gouuid.UUID]types.MetricData{
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
		batchSize   int64
		states      map[gouuid.UUID]stateData
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
		want    map[gouuid.UUID]types.MetricData
		wantErr bool
	}{
		{
			name: "temporary_filled",
			fields: fields{
				batchSize: 0,
				states:    nil,
				memoryStore: &mockStore{
					metrics: map[gouuid.UUID]types.MetricData{
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
					UUIDs: []gouuid.UUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want: map[gouuid.UUID]types.MetricData{
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
					UUIDs: []gouuid.UUID{
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
						uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					FromTimestamp: 0,
					ToTimestamp:   200,
					Step:          0,
					Function:      "",
				},
			},
			want:    make(map[gouuid.UUID]types.MetricData),
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
		states      map[gouuid.UUID]stateData
		memoryStore Store
		reader      types.MetricReader
		writer      types.MetricWriter
	}
	type args struct {
		metrics map[gouuid.UUID]types.MetricData
		now     time.Time
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantStates map[gouuid.UUID]stateData
		wantStorer map[gouuid.UUID]types.MetricData
		wantWriter map[gouuid.UUID]types.MetricData
	}{
		{
			name: "metrics_filled",
			fields: fields{
				batchSize: 50,
				states:    make(map[gouuid.UUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[gouuid.UUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				metrics: map[gouuid.UUID]types.MetricData{
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
			wantStates: map[gouuid.UUID]stateData{
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
			wantStorer: map[gouuid.UUID]types.MetricData{
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
			wantWriter: map[gouuid.UUID]types.MetricData{
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
				states:    make(map[gouuid.UUID]stateData),
				memoryStore: &mockStore{
					metrics: make(map[gouuid.UUID]types.MetricData),
				},
				reader: nil,
				writer: &mockMetricWriter{},
			},
			args: args{
				metrics: nil,
				now:     time.Unix(0, 0),
			},
			wantStates: make(map[gouuid.UUID]stateData),
			wantStorer: make(map[gouuid.UUID]types.MetricData),
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
		uuid      gouuid.UUID
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

func Benchmark_flushData(b *testing.B) {
	rand.Seed(42)
	tests := []struct {
		name      string
		data      types.MetricData
		states    []stateData
		now       time.Time
		batchSize int64
	}{
		{
			name: "one_state_first_5_30",
			data: types.MetricData{
				Points: types.MakePointsForTest(30),
			},
			states: []stateData{
				{
					pointCount:          5,
					firstPointTimestamp: 1568706164,
					lastPointTimestamp:  1568706164 + 5*10,
				},
			},
			now:       time.Unix(1568706164+30*10, 0),
			batchSize: 300,
		},
		{
			name: "one_state_first_30_60",
			data: types.MetricData{
				Points: types.MakePointsForTest(60),
			},
			states: []stateData{
				{
					pointCount:          30,
					firstPointTimestamp: 1568706164,
					lastPointTimestamp:  1568706164 + 30*10,
				},
			},
			now:       time.Unix(1568706164+60*10, 0),
			batchSize: 300,
		},
		{
			name: "three_state_90",
			data: types.MetricData{
				Points: types.MakePointsForTest(90),
			},
			states: []stateData{
				{
					pointCount:          30,
					firstPointTimestamp: 1568706164,
					lastPointTimestamp:  1568706164 + 30*10 - 1,
				},
				{
					pointCount:          30,
					firstPointTimestamp: 1568706164 + 30*10,
					lastPointTimestamp:  1568706164 + 60*10 - 1,
				},
				{
					pointCount:          30,
					firstPointTimestamp: 1568706164 + 60*10,
					lastPointTimestamp:  1568706164 + 90*10 - 1,
				},
			},
			now:       time.Unix(1568706164+90*10, 0),
			batchSize: 300,
		},
	}
	now := time.Now()
	for _, tt := range tests {
		batch := Batch{}
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, _ = batch.flushData(gouuid.UUID{}, tt.data, nil, now)
			}
		})
	}
}

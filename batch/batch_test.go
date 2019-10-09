package batch

import (
	"reflect"
	"squirreldb/types"
	"sync"
	"testing"
	"time"
)

type mockMetricReader struct {
	metrics map[types.MetricUUID]types.MetricData
}

type mockStorer struct {
	metrics map[types.MetricUUID]types.MetricData
}

type mockMetricWriter struct {
	got map[types.MetricUUID]types.MetricData
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

func (m *mockStorer) Append(newPoints, existingPoints map[types.MetricUUID]types.MetricData) error {
	for uuid, data := range newPoints {
		item := m.metrics[uuid]

		item.Points = append(item.Points, data.Points...)

		m.metrics[uuid] = item
	}

	for uuid, data := range existingPoints {
		item := m.metrics[uuid]

		item.Points = append(item.Points, data.Points...)

		m.metrics[uuid] = item
	}

	return nil
}

func (m *mockStorer) Get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error) {
	metrics := make(map[types.MetricUUID]types.MetricData)

	for _, uuid := range uuids {
		data, exists := m.metrics[uuid]

		if exists {
			metrics[uuid] = data
		}
	}

	return metrics, nil
}

func (m *mockStorer) Set(newPoints, existingPoints map[types.MetricUUID]types.MetricData) error {
	for uuid, data := range newPoints {
		m.metrics[uuid] = data
	}

	for uuid, data := range existingPoints {
		m.metrics[uuid] = data
	}

	return nil
}

func (m *mockMetricWriter) Write(metrics map[types.MetricUUID]types.MetricData) error {
	m.got = metrics

	return nil
}

func TestNewBatch(t *testing.T) {
	type args struct {
		temporaryStorer  Storer
		persistentReader types.MetricReader
		persistentWriter types.MetricWriter
	}
	tests := []struct {
		name string
		args args
		want *Batch
	}{
		{
			name: "new",
			args: args{
				temporaryStorer:  nil,
				persistentReader: nil,
				persistentWriter: nil,
			},
			want: &Batch{
				temporaryStorer:  nil,
				persistentReader: nil,
				persistentWriter: nil,
				states:           make(map[types.MetricUUID]state),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBatch(tt.args.temporaryStorer, tt.args.persistentReader, tt.args.persistentWriter); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatch_check(t *testing.T) {
	type fields struct {
		temporaryStorer  mockStorer
		persistentReader types.MetricReader
		persistentWriter mockMetricWriter
		states           map[types.MetricUUID]state
		mutex            sync.Mutex
	}
	type args struct {
		now       time.Time
		batchSize int64
		flushAll  bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[types.MetricUUID]types.MetricData
	}{
		{
			name: "no_deadline",
			fields: fields{
				temporaryStorer: mockStorer{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 100,
								Value:     100,
							},
						},
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  100,
						flushDeadline:       300,
					},
				},
			},
			args: args{
				now:       time.Unix(0, 0),
				batchSize: 300,
				flushAll:  false,
			},
			want: nil,
		},
		{
			name: "deadline",
			fields: fields{
				temporaryStorer: mockStorer{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 100,
								Value:     100,
							},
						},
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  100,
						flushDeadline:       300,
					},
				},
			},
			args: args{
				now:       time.Unix(600, 0),
				batchSize: 300,
				flushAll:  false,
			},
			want: map[types.MetricUUID]types.MetricData{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     0,
						},
						{
							Timestamp: 50,
							Value:     50,
						},
						{
							Timestamp: 100,
							Value:     100,
						},
					},
				},
			},
		},
		{
			name: "flush_all",
			fields: fields{
				temporaryStorer: mockStorer{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 100,
								Value:     100,
							},
						},
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  100,
						flushDeadline:       300,
					},
				},
			},
			args: args{
				now:       time.Unix(0, 0),
				batchSize: 300,
				flushAll:  true,
			},
			want: map[types.MetricUUID]types.MetricData{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     0,
						},
						{
							Timestamp: 50,
							Value:     50,
						},
						{
							Timestamp: 100,
							Value:     100,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				temporaryStorer:  &tt.fields.temporaryStorer,
				persistentReader: tt.fields.persistentReader,
				persistentWriter: &tt.fields.persistentWriter,
				states:           tt.fields.states,
				mutex:            tt.fields.mutex,
			}
			b.check(tt.args.now, tt.args.batchSize, tt.args.flushAll)
		})
	}
}

func TestBatch_flush(t *testing.T) {
	type fields struct {
		temporaryStorer  mockStorer
		persistentReader types.MetricReader
		persistentWriter mockMetricWriter
		states           map[types.MetricUUID]state
		mutex            sync.Mutex
	}
	type args struct {
		flushQueue map[types.MetricUUID][]state
		now        time.Time
		batchSize  int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[types.MetricUUID]types.MetricData
	}{
		{
			name: "no_flush",
			fields: fields{
				temporaryStorer: mockStorer{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 100,
								Value:     100,
							},
						},
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  100,
						flushDeadline:       300,
					},
				},
			},
			args: args{
				flushQueue: make(map[types.MetricUUID][]state),
			},
			want: make(map[types.MetricUUID]types.MetricData),
		},
		{
			name: "flush",
			fields: fields{
				temporaryStorer: mockStorer{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 150,
								Value:     150,
							},
							{
								Timestamp: 300,
								Value:     300,
							},
						},
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						pointCount:          1,
						firstPointTimestamp: 300,
						lastPointTimestamp:  300,
						flushDeadline:       600,
					},
				},
			},
			args: args{
				flushQueue: map[types.MetricUUID][]state{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						{
							pointCount:          2,
							firstPointTimestamp: 0,
							lastPointTimestamp:  150,
							flushDeadline:       300,
						},
					},
				},
				now:       time.Unix(300, 0),
				batchSize: 300,
			},
			want: map[types.MetricUUID]types.MetricData{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     0,
						},
						{
							Timestamp: 150,
							Value:     150,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				temporaryStorer:  &tt.fields.temporaryStorer,
				persistentReader: tt.fields.persistentReader,
				persistentWriter: &tt.fields.persistentWriter,
				states:           tt.fields.states,
				mutex:            tt.fields.mutex,
			}
			b.flush(tt.args.flushQueue, tt.args.now, tt.args.batchSize)
			if !reflect.DeepEqual(tt.fields.persistentWriter.got, tt.want) {
				t.Errorf("flush() got = %v, want %v", tt.fields.persistentWriter.got, tt.want)
			}
		})
	}
}

func TestBatch_read(t *testing.T) {
	type fields struct {
		temporaryStorer  mockStorer
		persistentReader mockMetricReader
		persistentWriter types.MetricWriter
		states           map[types.MetricUUID]state
		mutex            sync.Mutex
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
			name: "filled_temporary",
			fields: fields{
				temporaryStorer: mockStorer{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 100,
								Value:     100,
							},
							{
								Timestamp: 150,
								Value:     150,
							},
						},
					},
				}},
				persistentReader: mockMetricReader{},
				persistentWriter: nil,
				states:           nil,
			},
			args: args{request: types.MetricRequest{
				UUIDs: []types.MetricUUID{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(),
				},
				FromTimestamp: 50,
				ToTimestamp:   100,
				Step:          0,
			}},
			want: map[types.MetricUUID]types.MetricData{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					Points: []types.MetricPoint{
						{
							Timestamp: 50,
							Value:     50,
						},
						{
							Timestamp: 100,
							Value:     100,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "filled_persistent",
			fields: fields{
				temporaryStorer: mockStorer{},
				persistentReader: mockMetricReader{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 100,
								Value:     100,
							},
						},
					},
				}},
				persistentWriter: nil,
				states:           nil,
			},
			args: args{request: types.MetricRequest{
				UUIDs: []types.MetricUUID{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(),
				},
				FromTimestamp: 50,
				ToTimestamp:   100,
				Step:          0,
			}},
			want: map[types.MetricUUID]types.MetricData{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					Points: []types.MetricPoint{
						{
							Timestamp: 50,
							Value:     50,
						},
						{
							Timestamp: 100,
							Value:     100,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "filled_temporary_and_persistent",
			fields: fields{
				temporaryStorer: mockStorer{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 150,
								Value:     150,
							},
						},
					},
				}},
				persistentReader: mockMetricReader{metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 100,
								Value:     100,
							},
						},
					},
				}},
				persistentWriter: nil,
				states:           nil,
			},
			args: args{request: types.MetricRequest{
				UUIDs: []types.MetricUUID{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(),
				},
				FromTimestamp: 50,
				ToTimestamp:   100,
				Step:          0,
			}},
			want: map[types.MetricUUID]types.MetricData{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					Points: []types.MetricPoint{
						{
							Timestamp: 50,
							Value:     50,
						},
						{
							Timestamp: 100,
							Value:     100,
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
				temporaryStorer:  &tt.fields.temporaryStorer,
				persistentReader: &tt.fields.persistentReader,
				persistentWriter: tt.fields.persistentWriter,
				states:           tt.fields.states,
				mutex:            tt.fields.mutex,
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

func TestBatch_write(t *testing.T) {
	type fields struct {
		temporaryStorer  mockStorer
		persistentReader types.MetricReader
		persistentWriter mockMetricWriter
		states           map[types.MetricUUID]state
		mutex            sync.Mutex
	}
	type args struct {
		metrics   map[types.MetricUUID]types.MetricData
		now       time.Time
		batchSize int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[types.MetricUUID]state
		wantErr bool
	}{
		{
			name: "no_batch",
			fields: fields{
				temporaryStorer:  mockStorer{metrics: make(map[types.MetricUUID]types.MetricData)},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states:           make(map[types.MetricUUID]state),
			},
			args: args{
				metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 100,
								Value:     100,
							},
						},
					},
				},
				now:       time.Unix(100, 0),
				batchSize: 300,
			},
			want: map[types.MetricUUID]state{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					pointCount:          3,
					firstPointTimestamp: 0,
					lastPointTimestamp:  100,
					flushDeadline:       300,
				},
			},
			wantErr: false,
		},
		{
			name: "batch",
			fields: fields{
				temporaryStorer:  mockStorer{metrics: make(map[types.MetricUUID]types.MetricData)},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states:           make(map[types.MetricUUID]state),
			},
			args: args{
				metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 150,
								Value:     150,
							},
							{
								Timestamp: 300,
								Value:     300,
							},
						},
					},
				},
				now:       time.Unix(300, 0),
				batchSize: 300,
			},
			want: map[types.MetricUUID]state{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					pointCount:          1,
					firstPointTimestamp: 300,
					lastPointTimestamp:  300,
					flushDeadline:       600,
				},
			},
			wantErr: false,
		},
		{
			name: "unordered_points",
			fields: fields{
				temporaryStorer:  mockStorer{metrics: make(map[types.MetricUUID]types.MetricData)},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states:           make(map[types.MetricUUID]state),
			},
			args: args{
				metrics: map[types.MetricUUID]types.MetricData{
					types.MetricLabels{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}.UUID(): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
							{
								Timestamp: 100,
								Value:     100,
							},
						},
					},
				},
				now:       time.Unix(100, 0),
				batchSize: 300,
			},
			want: map[types.MetricUUID]state{
				types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(): {
					pointCount:          3,
					firstPointTimestamp: 0,
					lastPointTimestamp:  100,
					flushDeadline:       300,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				temporaryStorer:  &tt.fields.temporaryStorer,
				persistentReader: tt.fields.persistentReader,
				persistentWriter: &tt.fields.persistentWriter,
				states:           tt.fields.states,
				mutex:            tt.fields.mutex,
			}
			if err := b.write(tt.args.metrics, tt.args.now, tt.args.batchSize); (err != nil) != tt.wantErr {
				t.Errorf("write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.fields.states, tt.want) {
				t.Errorf("write() states = %v, want %v", tt.fields.states, tt.want)
			}
		})
	}
}

func Test_flushDeadline(t *testing.T) {
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
				uuid: types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}.UUID(),
				now:       time.Unix(0, 0),
				batchSize: 300,
			},
			want: 300,
		},
		{
			name: "uuid_1707",
			args: args{
				uuid: types.MetricLabels{
					"__uuid__": "00000000-0000-0000-0000-0000000006ab",
				}.UUID(),
				now:       time.Unix(0, 0),
				batchSize: 300,
			},
			want: 93,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := flushDeadline(tt.args.uuid, tt.args.now, tt.args.batchSize); got != tt.want {
				t.Errorf("flushDeadline() = %v, want %v", got, tt.want)
			}
		})
	}
}

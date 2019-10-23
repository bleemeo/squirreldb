package batch

import (
	"reflect"
	"squirreldb/compare"
	"squirreldb/types"
	"testing"
	"time"
)

type mockMetricReader struct {
	metrics types.Metrics
}

type mockStorer struct {
	metrics types.Metrics
}

type mockMetricWriter struct {
	got types.Metrics
}

func uuidify(value string) types.MetricUUID {
	uuid := types.MetricLabels{
		{
			Name:  "__bleemeo_uuid__",
			Value: value,
		},
	}.UUID()

	return uuid
}

func (m *mockMetricReader) Read(request types.MetricRequest) (types.Metrics, error) {
	metrics := make(types.Metrics)

	for _, uuid := range request.UUIDs {
		data, exists := m.metrics[uuid]

		if exists {
			metrics[uuid] = data
		}
	}

	return metrics, nil
}

func (m *mockStorer) Append(newMetrics, actualMetrics types.Metrics) error {
	for uuid, metricData := range newMetrics {
		metric := m.metrics[uuid]

		metric.Points = append(metric.Points, metricData.Points...)
		metric.TimeToLive = compare.Int64Max(metric.TimeToLive, metricData.TimeToLive)

		m.metrics[uuid] = metric
	}

	for uuid, metricData := range actualMetrics {
		metric := m.metrics[uuid]

		metric.Points = append(metric.Points, metricData.Points...)
		metric.TimeToLive = compare.Int64Max(metric.TimeToLive, metricData.TimeToLive)

		m.metrics[uuid] = metric
	}

	return nil
}

func (m *mockStorer) Get(uuids []types.MetricUUID) (types.Metrics, error) {
	metrics := make(types.Metrics)

	for _, uuid := range uuids {
		data, exists := m.metrics[uuid]

		if exists {
			metrics[uuid] = data
		}
	}

	return metrics, nil
}

func (m *mockStorer) Set(newMetrics, actualMetrics types.Metrics) error {
	for uuid, metricData := range newMetrics {
		m.metrics[uuid] = metricData
	}

	for uuid, metricData := range actualMetrics {
		m.metrics[uuid] = metricData
	}

	return nil
}

func (m *mockMetricWriter) Write(metrics types.Metrics) error {
	m.got = metrics

	return nil
}

func TestNewBatch(t *testing.T) {
	type args struct {
		batchSize        int64
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
				batchSize:        300,
				temporaryStorer:  nil,
				persistentReader: nil,
				persistentWriter: nil,
			},
			want: &Batch{
				batchSize: 300,
				store:     nil,
				reader:    nil,
				writer:    nil,
				states:    make(map[types.MetricUUID]state),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.args.batchSize, tt.args.temporaryStorer, tt.args.persistentReader, tt.args.persistentWriter); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatch_check(t *testing.T) {
	type fields struct {
		batchSize        int64
		temporaryStorer  mockStorer
		persistentReader types.MetricReader
		persistentWriter mockMetricWriter
		states           map[types.MetricUUID]state
	}
	type args struct {
		now      time.Time
		flushAll bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   types.Metrics
	}{
		{
			name: "no_deadline",
			fields: fields{
				batchSize: 300,
				temporaryStorer: mockStorer{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
						TimeToLive: 3600,
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  100,
						flushTimestamp:      300,
					},
				},
			},
			args: args{
				now:      time.Unix(0, 0),
				flushAll: false,
			},
			want: nil,
		},
		{
			name: "deadline",
			fields: fields{
				batchSize: 300,
				temporaryStorer: mockStorer{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
						TimeToLive: 3600,
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  100,
						flushTimestamp:      300,
					},
				},
			},
			args: args{
				now:      time.Unix(600, 0),
				flushAll: false,
			},
			want: types.Metrics{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
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
					TimeToLive: 3600,
				},
			},
		},
		{
			name: "flush_all",
			fields: fields{
				batchSize: 300,
				temporaryStorer: mockStorer{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
						TimeToLive: 3600,
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  100,
						flushTimestamp:      300,
					},
				},
			},
			args: args{
				now:      time.Unix(0, 0),
				flushAll: true,
			},
			want: types.Metrics{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
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
					TimeToLive: 3600,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				batchSize: tt.fields.batchSize,
				store:     &tt.fields.temporaryStorer,
				reader:    tt.fields.persistentReader,
				writer:    &tt.fields.persistentWriter,
				states:    tt.fields.states,
			}
			b.check(tt.args.now, tt.args.flushAll)
		})
	}
}

func TestBatch_flush(t *testing.T) {
	type fields struct {
		batchSize        int64
		temporaryStorer  mockStorer
		persistentReader types.MetricReader
		persistentWriter mockMetricWriter
		states           map[types.MetricUUID]state
	}
	type args struct {
		flushQueue map[types.MetricUUID][]state
		now        time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   types.Metrics
	}{
		{
			name: "no_flush",
			fields: fields{
				batchSize: 300,
				temporaryStorer: mockStorer{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
						TimeToLive: 3600,
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						pointCount:          3,
						firstPointTimestamp: 0,
						lastPointTimestamp:  100,
						flushTimestamp:      300,
					},
				},
			},
			args: args{
				flushQueue: make(map[types.MetricUUID][]state),
			},
			want: make(types.Metrics),
		},
		{
			name: "flush",
			fields: fields{
				batchSize: 300,
				temporaryStorer: mockStorer{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
						TimeToLive: 3600,
					},
				}},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states: map[types.MetricUUID]state{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						pointCount:          1,
						firstPointTimestamp: 300,
						lastPointTimestamp:  300,
						flushTimestamp:      600,
					},
				},
			},
			args: args{
				flushQueue: map[types.MetricUUID][]state{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						{
							pointCount:          2,
							firstPointTimestamp: 0,
							lastPointTimestamp:  150,
							flushTimestamp:      300,
						},
					},
				},
				now: time.Unix(300, 0),
			},
			want: types.Metrics{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
						{
							Timestamp: 0,
							Value:     0,
						},
						{
							Timestamp: 150,
							Value:     150,
						},
					},
					TimeToLive: 3600,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				batchSize: tt.fields.batchSize,
				store:     &tt.fields.temporaryStorer,
				reader:    tt.fields.persistentReader,
				writer:    &tt.fields.persistentWriter,
				states:    tt.fields.states,
			}
			b.flush(tt.args.flushQueue, tt.args.now)
			if !reflect.DeepEqual(tt.fields.persistentWriter.got, tt.want) {
				t.Errorf("flush() got = %v, want %v", tt.fields.persistentWriter.got, tt.want)
			}
		})
	}
}

func TestBatch_read(t *testing.T) {
	type fields struct {
		batchSize        int64
		temporaryStorer  mockStorer
		persistentReader mockMetricReader
		persistentWriter types.MetricWriter
		states           map[types.MetricUUID]state
	}
	type args struct {
		request types.MetricRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    types.Metrics
		wantErr bool
	}{
		{
			name: "filled_temporary",
			fields: fields{
				batchSize: 300,
				temporaryStorer: mockStorer{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
					uuidify("00000000-0000-0000-0000-000000000001"),
				},
				FromTimestamp: 50,
				ToTimestamp:   100,
				Step:          0,
			}},
			want: types.Metrics{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
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
				batchSize:       300,
				temporaryStorer: mockStorer{},
				persistentReader: mockMetricReader{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
					uuidify("00000000-0000-0000-0000-000000000001"),
				},
				FromTimestamp: 50,
				ToTimestamp:   100,
				Step:          0,
			}},
			want: types.Metrics{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
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
				batchSize: 300,
				temporaryStorer: mockStorer{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
							{
								Timestamp: 0,
								Value:     0,
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
				persistentReader: mockMetricReader{metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
							{
								Timestamp: 50,
								Value:     50,
							},
						},
						TimeToLive: 3600,
					},
				}},
				persistentWriter: nil,
				states:           nil,
			},
			args: args{request: types.MetricRequest{
				UUIDs: []types.MetricUUID{
					uuidify("00000000-0000-0000-0000-000000000001"),
				},
				FromTimestamp: 50,
				ToTimestamp:   100,
				Step:          0,
			}},
			want: types.Metrics{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
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
				batchSize: tt.fields.batchSize,
				store:     &tt.fields.temporaryStorer,
				reader:    &tt.fields.persistentReader,
				writer:    tt.fields.persistentWriter,
				states:    tt.fields.states,
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
		batchSize        int64
		temporaryStorer  mockStorer
		persistentReader types.MetricReader
		persistentWriter mockMetricWriter
		states           map[types.MetricUUID]state
	}
	type args struct {
		metrics types.Metrics
		now     time.Time
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
				batchSize:        300,
				temporaryStorer:  mockStorer{metrics: make(types.Metrics)},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states:           make(map[types.MetricUUID]state),
			},
			args: args{
				metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
						TimeToLive: 3600,
					},
				},
				now: time.Unix(100, 0),
			},
			want: map[types.MetricUUID]state{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					pointCount:          3,
					firstPointTimestamp: 0,
					lastPointTimestamp:  100,
					flushTimestamp:      299,
				},
			},
			wantErr: false,
		},
		{
			name: "batch",
			fields: fields{
				batchSize:        300,
				temporaryStorer:  mockStorer{metrics: make(types.Metrics)},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states:           make(map[types.MetricUUID]state),
			},
			args: args{
				metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
						TimeToLive: 3600,
					},
				},
				now: time.Unix(300, 0),
			},
			want: map[types.MetricUUID]state{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					pointCount:          1,
					firstPointTimestamp: 300,
					lastPointTimestamp:  300,
					flushTimestamp:      599,
				},
			},
			wantErr: false,
		},
		{
			name: "unordered_points",
			fields: fields{
				batchSize:        300,
				temporaryStorer:  mockStorer{metrics: make(types.Metrics)},
				persistentReader: nil,
				persistentWriter: mockMetricWriter{},
				states:           make(map[types.MetricUUID]state),
			},
			args: args{
				metrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
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
						TimeToLive: 3600,
					},
				},
				now: time.Unix(100, 0),
			},
			want: map[types.MetricUUID]state{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					pointCount:          3,
					firstPointTimestamp: 0,
					lastPointTimestamp:  100,
					flushTimestamp:      299,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				batchSize: tt.fields.batchSize,
				store:     &tt.fields.temporaryStorer,
				reader:    tt.fields.persistentReader,
				writer:    &tt.fields.persistentWriter,
				states:    tt.fields.states,
			}
			if err := b.write(tt.args.metrics, tt.args.now); (err != nil) != tt.wantErr {
				t.Errorf("write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.fields.states, tt.want) {
				t.Errorf("write() states = %v, want %v", tt.fields.states, tt.want)
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
				uuid:      uuidify("00000000-0000-0000-0000-000000000001"),
				now:       time.Unix(0, 0),
				batchSize: 300,
			},
			want: 299,
		},
		{
			name: "uuid_1707",
			args: args{
				uuid:      uuidify("00000000-0000-0000-0000-0000000006ab"),
				now:       time.Unix(0, 0),
				batchSize: 300,
			},
			want: 93,
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

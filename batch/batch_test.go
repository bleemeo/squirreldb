package batch

import (
	"reflect"
	"squirreldb/types"
	"sync"
	"testing"
	"time"
)

type mockMetricReader struct {
	msPoints []types.MetricPoints
}

type mockMetricStorer struct {
	keysPoints map[string][]types.Point
}

type mockMetricWriter struct {
	got []types.MetricPoints
}

func (m *mockMetricReader) Read(request types.MetricRequest) ([]types.MetricPoints, error) {
	var msPoints []types.MetricPoints
	canonical := request.CanonicalLabels()

	for _, mPoints := range m.msPoints {
		if mPoints.CanonicalLabels() == canonical {
			msPoints = append(msPoints, mPoints)
		}
	}

	return msPoints, nil
}

func (m *mockMetricStorer) Append(newPoints, existingPoints map[string][]types.Point) error {
	for key, points := range newPoints {
		item := m.keysPoints[key]

		m.keysPoints[key] = append(item, points...)
	}

	for key, points := range existingPoints {
		item := m.keysPoints[key]

		m.keysPoints[key] = append(item, points...)
	}

	return nil
}

func (m *mockMetricStorer) Get(keys []string) (map[string][]types.Point, error) {
	keysPoints := make(map[string][]types.Point)

	for key, points := range m.keysPoints {
		for i := range keys {
			if keys[i] == key {
				keysPoints[key] = points
			}
		}
	}

	return keysPoints, nil
}

func (m *mockMetricStorer) Set(newPoints, existingPoints map[string][]types.Point) error {
	for key, points := range newPoints {
		m.keysPoints[key] = points
	}

	for key, points := range existingPoints {
		m.keysPoints[key] = points
	}

	return nil
}

func (m *mockMetricWriter) Write(msPoints []types.MetricPoints) error {
	m.got = msPoints

	return nil
}

func TestNewBatch(t *testing.T) {
	type args struct {
		temporaryStorage   MetricStorer
		persistentStorageR types.MetricReader
		persistentStorageW types.MetricWriter
	}
	tests := []struct {
		name string
		args args
		want *Batch
	}{
		{
			name: "test_new",
			args: args{
				temporaryStorage:   nil,
				persistentStorageR: nil,
				persistentStorageW: nil,
			},
			want: &Batch{
				temporaryStorage:   nil,
				persistentStorageR: nil,
				persistentStorageW: nil,
				states:             make(map[string]state),
				mutex:              sync.Mutex{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBatch(tt.args.temporaryStorage, tt.args.persistentStorageR, tt.args.persistentStorageW); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatch_check(t *testing.T) {
	type fields struct {
		temporaryStorage   mockMetricStorer
		persistentStorageR types.MetricReader
		persistentStorageW mockMetricWriter
		states             map[string]state
		mutex              sync.Mutex
	}
	type args struct {
		now           time.Time
		batchDuration time.Duration
		flushAll      bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []types.MetricPoints
	}{
		{
			name: "test_nothing",
			fields: fields{
				temporaryStorage: mockMetricStorer{keysPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
				}},
				persistentStorageR: nil,
				persistentStorageW: mockMetricWriter{},
				states: map[string]state{
					"00000000-0000-0000-0000-000000000000": {
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						pointCount:     3,
						firstPointTime: time.Unix(0, 0),
						lastPointTime:  time.Unix(100, 0),
						flushDeadline:  time.Unix(300, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				now:           time.Unix(300, 0),
				batchDuration: 300 * time.Second,
				flushAll:      false,
			},
			want: nil,
		},
		{
			name: "test_something",
			fields: fields{
				temporaryStorage: mockMetricStorer{keysPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
						{
							Time:  time.Unix(300, 0),
							Value: 300,
						},
					},
				}},
				persistentStorageR: nil,
				persistentStorageW: mockMetricWriter{},
				states: map[string]state{
					"00000000-0000-0000-0000-000000000000": {
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						pointCount:     3,
						firstPointTime: time.Unix(0, 0),
						lastPointTime:  time.Unix(100, 0),
						flushDeadline:  time.Unix(300, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				now:           time.Unix(600, 0),
				batchDuration: 300 * time.Second,
				flushAll:      false,
			},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
				},
			},
		},
		{
			name: "test_flushAll",
			fields: fields{
				temporaryStorage: mockMetricStorer{keysPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
				}},
				persistentStorageR: nil,
				persistentStorageW: mockMetricWriter{},
				states: map[string]state{
					"00000000-0000-0000-0000-000000000000": {
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						pointCount:     3,
						firstPointTime: time.Unix(0, 0),
						lastPointTime:  time.Unix(100, 0),
						flushDeadline:  time.Unix(300, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				now:           time.Unix(300, 0),
				batchDuration: 300 * time.Second,
				flushAll:      true,
			},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				temporaryStorage:   &tt.fields.temporaryStorage,
				persistentStorageR: tt.fields.persistentStorageR,
				persistentStorageW: &tt.fields.persistentStorageW,
				states:             tt.fields.states,
				mutex:              tt.fields.mutex,
			}
			b.check(tt.args.now, tt.args.batchDuration, tt.args.flushAll)
			got := tt.fields.persistentStorageW.got
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("check() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatch_flush(t *testing.T) {
	type fields struct {
		temporaryStorage   mockMetricStorer
		persistentStorageR types.MetricReader
		persistentStorageW mockMetricWriter
		states             map[string]state
		mutex              sync.Mutex
	}
	type args struct {
		stateQueue    map[string][]state
		now           time.Time
		batchDuration time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []types.MetricPoints
	}{
		{
			name: "test_single_state",
			fields: fields{
				temporaryStorage: mockMetricStorer{keysPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
						{
							Time:  time.Unix(300, 0),
							Value: 300,
						},
					},
				}},
				persistentStorageR: nil,
				persistentStorageW: mockMetricWriter{},
				states: map[string]state{
					"00000000-0000-0000-0000-000000000000": {
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						pointCount:     1,
						firstPointTime: time.Unix(300, 0),
						lastPointTime:  time.Unix(300, 0),
						flushDeadline:  time.Unix(600, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				stateQueue: map[string][]state{
					"00000000-0000-0000-0000-000000000000": {
						{
							Metric: types.Metric{Labels: map[string]string{
								"__uuid__": "00000000-0000-0000-0000-000000000000",
							}},
							pointCount:     2,
							firstPointTime: time.Unix(0, 0),
							lastPointTime:  time.Unix(150, 0),
							flushDeadline:  time.Unix(300, 0),
						},
					},
				},
				now:           time.Unix(300, 0),
				batchDuration: 300 * time.Second,
			},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				temporaryStorage:   &tt.fields.temporaryStorage,
				persistentStorageR: tt.fields.persistentStorageR,
				persistentStorageW: &tt.fields.persistentStorageW,
				states:             tt.fields.states,
				mutex:              tt.fields.mutex,
			}
			b.flush(tt.args.stateQueue, tt.args.now, tt.args.batchDuration)
			got := tt.fields.persistentStorageW.got
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("flush() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatch_read(t *testing.T) {
	type fields struct {
		temporaryStorage   mockMetricStorer
		persistentStorageR mockMetricReader
		persistentStorageW types.MetricWriter
		states             map[string]state
		mutex              sync.Mutex
	}
	type args struct {
		mRequest types.MetricRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []types.MetricPoints
		wantErr bool
	}{
		{
			name: "test_filled_temporaryStorage",
			fields: fields{
				temporaryStorage: mockMetricStorer{keysPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
					},
				}},
				persistentStorageR: mockMetricReader{},
				persistentStorageW: nil,
				states:             nil,
				mutex:              sync.Mutex{},
			},
			args: args{mRequest: types.MetricRequest{
				Metric: types.Metric{Labels: map[string]string{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}},
				FromTime: time.Unix(50, 0),
				ToTime:   time.Unix(150, 0),
				Step:     0,
			}},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "test_filled_persistentStorage",
			fields: fields{
				temporaryStorage: mockMetricStorer{},
				persistentStorageR: mockMetricReader{msPoints: []types.MetricPoints{
					{
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						Points: []types.Point{
							{
								Time:  time.Unix(50, 0),
								Value: 50,
							},
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
							{
								Time:  time.Unix(150, 0),
								Value: 150,
							},
						},
					},
				}},
				persistentStorageW: nil,
				states:             nil,
				mutex:              sync.Mutex{},
			},
			args: args{mRequest: types.MetricRequest{
				Metric: types.Metric{Labels: map[string]string{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}},
				FromTime: time.Unix(50, 0),
				ToTime:   time.Unix(150, 0),
				Step:     0,
			}},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "test_filled_temporaryStorage_persistentStorage",
			fields: fields{
				temporaryStorage: mockMetricStorer{keysPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
					},
				}},
				persistentStorageR: mockMetricReader{msPoints: []types.MetricPoints{
					{
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						Points: []types.Point{
							{
								Time:  time.Unix(25, 0),
								Value: 25,
							},
							{
								Time:  time.Unix(75, 0),
								Value: 75,
							},
							{
								Time:  time.Unix(125, 0),
								Value: 125,
							},
						},
					},
				}},
				persistentStorageW: nil,
				states:             nil,
				mutex:              sync.Mutex{},
			},
			args: args{mRequest: types.MetricRequest{
				Metric: types.Metric{Labels: map[string]string{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}},
				FromTime: time.Unix(50, 0),
				ToTime:   time.Unix(150, 0),
				Step:     0,
			}},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
						{
							Time:  time.Unix(25, 0),
							Value: 25,
						},
						{
							Time:  time.Unix(75, 0),
							Value: 75,
						},
						{
							Time:  time.Unix(125, 0),
							Value: 125,
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
				temporaryStorage:   &tt.fields.temporaryStorage,
				persistentStorageR: &tt.fields.persistentStorageR,
				persistentStorageW: tt.fields.persistentStorageW,
				states:             tt.fields.states,
				mutex:              tt.fields.mutex,
			}
			got, err := b.read(tt.args.mRequest)
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
		temporaryStorage   mockMetricStorer
		persistentStorageR types.MetricReader
		persistentStorageW mockMetricWriter
		states             map[string]state
		mutex              sync.Mutex
	}
	type args struct {
		msPoints      []types.MetricPoints
		now           time.Time
		batchDuration time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]state
		wantErr bool
	}{
		{
			name: "test_no_batch",
			fields: fields{
				temporaryStorage:   mockMetricStorer{keysPoints: make(map[string][]types.Point)},
				persistentStorageR: nil,
				persistentStorageW: mockMetricWriter{},
				states:             make(map[string]state),
				mutex:              sync.Mutex{},
			},
			args: args{
				msPoints: []types.MetricPoints{
					{
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						Points: []types.Point{
							{
								Time:  time.Unix(0, 0),
								Value: 0,
							},
							{
								Time:  time.Unix(50, 0),
								Value: 50,
							},
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
						},
					},
				},
				now:           time.Unix(100, 0),
				batchDuration: 300 * time.Second,
			},
			want: map[string]state{
				"00000000-0000-0000-0000-000000000000": {
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					pointCount:     3,
					firstPointTime: time.Unix(0, 0),
					lastPointTime:  time.Unix(100, 0),
					flushDeadline:  time.Unix(300, 0),
				},
			},
			wantErr: false,
		},
		{
			name: "test_batch",
			fields: fields{
				temporaryStorage:   mockMetricStorer{keysPoints: make(map[string][]types.Point)},
				persistentStorageR: nil,
				persistentStorageW: mockMetricWriter{},
				states:             make(map[string]state),
				mutex:              sync.Mutex{},
			},
			args: args{
				msPoints: []types.MetricPoints{
					{
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						Points: []types.Point{
							{
								Time:  time.Unix(0, 0),
								Value: 0,
							},
							{
								Time:  time.Unix(150, 0),
								Value: 150,
							},
							{
								Time:  time.Unix(300, 0),
								Value: 300,
							},
						},
					},
				},
				now:           time.Unix(300, 0),
				batchDuration: 300 * time.Second,
			},
			want: map[string]state{
				"00000000-0000-0000-0000-000000000000": {
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					pointCount:     1,
					firstPointTime: time.Unix(300, 0),
					lastPointTime:  time.Unix(300, 0),
					flushDeadline:  time.Unix(600, 0),
				},
			},
			wantErr: false,
		},
		{
			name: "test_unordered_points",
			fields: fields{
				temporaryStorage:   mockMetricStorer{keysPoints: make(map[string][]types.Point)},
				persistentStorageR: nil,
				persistentStorageW: mockMetricWriter{},
				states:             make(map[string]state),
				mutex:              sync.Mutex{},
			},
			args: args{
				msPoints: []types.MetricPoints{
					{
						Metric: types.Metric{Labels: map[string]string{
							"__uuid__": "00000000-0000-0000-0000-000000000000",
						}},
						Points: []types.Point{
							{
								Time:  time.Unix(50, 0),
								Value: 50,
							},
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
							{
								Time:  time.Unix(0, 0),
								Value: 0,
							},
						},
					},
				},
				now:           time.Unix(100, 0),
				batchDuration: 300 * time.Second,
			},
			want: map[string]state{
				"00000000-0000-0000-0000-000000000000": {
					Metric: types.Metric{Labels: map[string]string{
						"__uuid__": "00000000-0000-0000-0000-000000000000",
					}},
					pointCount:     3,
					firstPointTime: time.Unix(0, 0),
					lastPointTime:  time.Unix(100, 0),
					flushDeadline:  time.Unix(300, 0),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				temporaryStorage:   &tt.fields.temporaryStorage,
				persistentStorageR: tt.fields.persistentStorageR,
				persistentStorageW: &tt.fields.persistentStorageW,
				states:             tt.fields.states,
				mutex:              tt.fields.mutex,
			}
			if err := b.write(tt.args.msPoints, tt.args.now, tt.args.batchDuration); (err != nil) != tt.wantErr {
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
		metric        types.Metric
		now           time.Time
		batchDuration time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			name: "test_uuid_0",
			args: args{
				metric: types.Metric{Labels: map[string]string{
					"__uuid__": "00000000-0000-0000-0000-000000000000",
				}},
				now:           time.Unix(0, 0),
				batchDuration: 300 * time.Second,
			},
			want: time.Unix(300, 0),
		},
		{
			name: "test_uuid_1707",
			args: args{
				metric: types.Metric{Labels: map[string]string{
					"__uuid__": "00000000-0000-0000-0000-0000000006ab",
				}},
				now:           time.Unix(0, 0),
				batchDuration: 300 * time.Second,
			},
			want: time.Unix(93, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := flushDeadline(tt.args.metric, tt.args.now, tt.args.batchDuration); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("flushDeadline() = %v, want %v", got, tt.want)
			}
		})
	}
}

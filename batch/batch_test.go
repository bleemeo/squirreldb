package batch

import (
	"hamsterdb/store"
	"hamsterdb/types"
	"reflect"
	"sync"
	"testing"
	"time"
)

type mockMetricWriter struct {
	got []types.MetricPoints
}

func (m *mockMetricWriter) Write(msPoints []types.MetricPoints) error {
	m.got = msPoints

	return nil
}

func TestNewBatch(t *testing.T) {
	type args struct {
		temporaryStorage  MetricStorer
		persistentStorage types.MetricWriter
	}
	tests := []struct {
		name string
		args args
		want *Batch
	}{
		{
			name: "new_batch",
			args: args{
				temporaryStorage:  nil,
				persistentStorage: nil,
			},
			want: &Batch{
				temporaryStorage:  nil,
				persistentStorage: nil,
				states:            make(map[string]state),
				mutex:             sync.Mutex{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBatch(tt.args.temporaryStorage, tt.args.persistentStorage); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBatch_check(t *testing.T) {
	type fields struct {
		temporaryStorage  MetricStorer
		persistentStorage mockMetricWriter
		states            map[string]state
		mutex             sync.Mutex
	}
	type args struct {
		now         time.Time
		batchLength time.Duration
		flushAll    bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []types.MetricPoints
		wantErr bool
	}{
		{
			name: "check_simple",
			fields: fields{
				temporaryStorage: &store.Store{
					Points: map[string][]types.Point{
						`__name__="testing1"`: {
							{
								Time:  time.Unix(0, 0),
								Value: 0,
							},
						},
						`__name__="testing2"`: {
							{
								Time:  time.Unix(0, 0),
								Value: 0,
							},
							{
								Time:  time.Unix(25, 0),
								Value: 25,
							},
						},
					},
				},
				persistentStorage: mockMetricWriter{},
				states: map[string]state{
					`__name__="testing1"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "testing1",
						}},
						pointCount:     1,
						firstPointTime: time.Unix(0, 0),
						lastPointTime:  time.Unix(0, 0),
						flushDeadline:  time.Unix(50, 0),
					},
					`__name__="testing2"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "testing2",
						}},
						pointCount:     2,
						firstPointTime: time.Unix(0, 0),
						lastPointTime:  time.Unix(25, 0),
						flushDeadline:  time.Unix(50, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				now:         time.Unix(100, 0),
				batchLength: 50,
				flushAll:    false,
			},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing1",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
					},
				},
				{
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing2",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(25, 0),
							Value: 25,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "check_flush_all",
			fields: fields{
				temporaryStorage: &store.Store{
					Points: map[string][]types.Point{
						`__name__="testing1"`: {
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
						},
						`__name__="testing2"`: {
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
							{
								Time:  time.Unix(125, 0),
								Value: 125,
							},
						},
					},
				},
				persistentStorage: mockMetricWriter{},
				states: map[string]state{
					`__name__="testing1"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "testing1",
						}},
						pointCount:     1,
						firstPointTime: time.Unix(100, 0),
						lastPointTime:  time.Unix(100, 0),
						flushDeadline:  time.Unix(150, 0),
					},
					`__name__="testing2"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "testing2",
						}},
						pointCount:     2,
						firstPointTime: time.Unix(100, 0),
						lastPointTime:  time.Unix(125, 0),
						flushDeadline:  time.Unix(150, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				now:         time.Unix(100, 0),
				batchLength: 50,
				flushAll:    true,
			},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing1",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
				},
				{
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing2",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(100, 0),
							Value: 100,
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
				temporaryStorage:  tt.fields.temporaryStorage,
				persistentStorage: &tt.fields.persistentStorage,
				states:            tt.fields.states,
				mutex:             tt.fields.mutex,
			}
			if err := b.check(tt.args.now, tt.args.batchLength, tt.args.flushAll); (err != nil) != tt.wantErr {
				t.Errorf("check() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.fields.persistentStorage.got, tt.want) {
				t.Errorf("write() persistentStorage.got = %v, want %v", tt.fields.persistentStorage.got, tt.want)
			}
		})
	}
}

func TestBatch_flush(t *testing.T) {
	type fields struct {
		temporaryStorage  MetricStorer
		persistentStorage mockMetricWriter
		states            map[string]state
		mutex             sync.Mutex
	}
	type args struct {
		stateQueue  map[string][]state
		now         time.Time
		batchLength time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []types.MetricPoints
		wantErr bool
	}{
		{
			name: "flush",
			fields: fields{
				temporaryStorage: &store.Store{
					Points: map[string][]types.Point{
						`__name__="testing1"`: {
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
						`__name__="testing2"`: {
							{
								Time:  time.Unix(0, 0),
								Value: 0,
							},
							{
								Time:  time.Unix(25, 0),
								Value: 25,
							},
							{
								Time:  time.Unix(50, 0),
								Value: 50,
							},
						},
					},
				},
				persistentStorage: mockMetricWriter{},
				states:            nil,
				mutex:             sync.Mutex{},
			},
			args: args{
				stateQueue: map[string][]state{
					`__name__="testing1"`: {
						{
							Metric: types.Metric{Labels: map[string]string{
								"__name__": "testing1",
							}},
							pointCount:     1,
							firstPointTime: time.Unix(0, 0),
							lastPointTime:  time.Unix(0, 0),
							flushDeadline:  time.Unix(150, 0),
						},
						{
							Metric: types.Metric{Labels: map[string]string{
								"__name__": "testing1",
							}},
							pointCount:     1,
							firstPointTime: time.Unix(50, 0),
							lastPointTime:  time.Unix(50, 0),
							flushDeadline:  time.Unix(150, 0),
						},
					},
					`__name__="testing2"`: {
						{
							Metric: types.Metric{Labels: map[string]string{
								"__name__": "testing2",
							}},
							pointCount:     2,
							firstPointTime: time.Unix(0, 0),
							lastPointTime:  time.Unix(25, 0),
							flushDeadline:  time.Unix(150, 0),
						},
					},
				},
				now:         time.Unix(100, 0),
				batchLength: 50,
			},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing1",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
					},
				},
				{
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing1",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(50, 0),
							Value: 50,
						},
					},
				},
				{
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing2",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(0, 0),
							Value: 0,
						},
						{
							Time:  time.Unix(25, 0),
							Value: 25,
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
				temporaryStorage:  tt.fields.temporaryStorage,
				persistentStorage: &tt.fields.persistentStorage,
				states:            tt.fields.states,
				mutex:             tt.fields.mutex,
			}
			if err := b.flush(tt.args.stateQueue, tt.args.now, tt.args.batchLength); (err != nil) != tt.wantErr {
				t.Errorf("flush() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.fields.persistentStorage.got, tt.want) {
				t.Errorf("flush() persistentStorage.got = %v, want %v", tt.fields.persistentStorage.got, tt.want)
			}
		})
	}
}

func TestBatch_write(t *testing.T) {
	type fields struct {
		temporaryStorage  MetricStorer
		persistentStorage types.MetricWriter
		states            map[string]state
		mutex             sync.Mutex
	}
	type args struct {
		msPoints    []types.MetricPoints
		now         time.Time
		batchLength time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]state
		wantErr bool
	}{
		{
			name: "write_no_state_simple",
			fields: fields{
				temporaryStorage:  store.NewStore(),
				persistentStorage: &mockMetricWriter{},
				states:            make(map[string]state),
				mutex:             sync.Mutex{},
			},
			args: args{
				msPoints: []types.MetricPoints{
					{
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "testing1",
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
					{
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "testing2",
						}},
						Points: []types.Point{
							{
								Time:  time.Unix(0, 0),
								Value: 0,
							},
							{
								Time:  time.Unix(25, 0),
								Value: 25,
							},
							{
								Time:  time.Unix(50, 0),
								Value: 50,
							},
						},
					},
				},
				now:         time.Unix(100, 0),
				batchLength: 50,
			},
			want: map[string]state{
				`__name__="testing1"`: {
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing1",
					}},
					pointCount:     1,
					firstPointTime: time.Unix(100, 0),
					lastPointTime:  time.Unix(100, 0),
					flushDeadline:  time.Unix(150, 0),
				},
				`__name__="testing2"`: {
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing2",
					}},
					pointCount:     1,
					firstPointTime: time.Unix(50, 0),
					lastPointTime:  time.Unix(50, 0),
					flushDeadline:  time.Unix(150, 0),
				},
			},
			wantErr: false,
		},
		{
			name: "write_",
			fields: fields{
				temporaryStorage:  store.NewStore(),
				persistentStorage: &mockMetricWriter{},
				states:            make(map[string]state),
				mutex:             sync.Mutex{},
			},
			args: args{
				msPoints: []types.MetricPoints{
					{
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "testing1",
						}},
						Points: []types.Point{
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
							{
								Time:  time.Unix(50, 0),
								Value: 50,
							},
							{
								Time:  time.Unix(25, 0),
								Value: 25,
							},
						},
					},
				},
				now:         time.Unix(100, 0),
				batchLength: 50,
			},
			want: map[string]state{
				`__name__="testing1"`: {
					Metric: types.Metric{Labels: map[string]string{
						"__name__": "testing1",
					}},
					pointCount:     2,
					firstPointTime: time.Unix(25, 0),
					lastPointTime:  time.Unix(50, 0),
					flushDeadline:  time.Unix(150, 0),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Batch{
				temporaryStorage:  tt.fields.temporaryStorage,
				persistentStorage: tt.fields.persistentStorage,
				states:            tt.fields.states,
				mutex:             tt.fields.mutex,
			}
			if err := b.write(tt.args.msPoints, tt.args.now, tt.args.batchLength); (err != nil) != tt.wantErr {
				t.Errorf("write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(b.states, tt.want) {
				t.Errorf("write() states = %v, want %v", b.states, tt.want)
			}
		})
	}
}

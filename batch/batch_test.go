package batch

import (
	"hamsterdb/store"
	"hamsterdb/types"
	"reflect"
	"sync"
	"testing"
	"time"
)

type mockPersistentStorage struct {
	got []types.MetricPoints
}

func (mock *mockPersistentStorage) Write(msPoints []types.MetricPoints) error {
	mock.got = msPoints

	return nil
}

func TestNewBatch(t *testing.T) {
	type args struct {
		temporaryStorage  Storer
		persistentStorage types.Writer
	}
	tests := []struct {
		name string
		args args
		want *batch
	}{
		{
			name: "basic",
			args: args{
				temporaryStorage:  nil,
				persistentStorage: nil,
			},
			want: &batch{
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

func TestBatch_Flush(t *testing.T) {
	type fields struct {
		temporaryStorage  Storer
		persistentStorage *mockPersistentStorage
		states            map[string]state
		mutex             sync.Mutex
	}
	type args struct {
		flushQueue      map[string][]state
		currentTime     time.Time
		batchTimeLength float64
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		newMsPoints map[string]types.MetricPoints
		want        []types.MetricPoints
		wantErr     bool
	}{
		{
			name: "basic",
			fields: fields{
				temporaryStorage:  store.NewStore(),
				persistentStorage: &mockPersistentStorage{},
				states: map[string]state{
					`__name__="testing"`: {
						pointCount:     1,
						firstPointTime: time.Unix(0, 0),
						lastPointTime:  time.Unix(100, 0),
						flushDeadline:  time.Time{},
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				flushQueue: map[string][]state{
					`__name__="testing"`: {
						{
							pointCount:     1,
							firstPointTime: time.Unix(25, 0),
							lastPointTime:  time.Unix(100, 0),
							flushDeadline:  time.Time{},
						},
						{
							pointCount:     1,
							firstPointTime: time.Unix(200, 0),
							lastPointTime:  time.Unix(275, 0),
							flushDeadline:  time.Time{},
						},
					},
				},
				currentTime:     time.Unix(1000, 0),
				batchTimeLength: 300,
			},
			newMsPoints: map[string]types.MetricPoints{
				`__name__="testing"`: {
					Metric: types.Metric{Labels: map[string]string{
						"name": "testing",
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
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
						{
							Time:  time.Unix(200, 0),
							Value: 200,
						},
						{
							Time:  time.Unix(250, 0),
							Value: 250,
						},
						{
							Time:  time.Unix(300, 0),
							Value: 300,
						},
					},
				},
			},
			want: []types.MetricPoints{
				{
					Metric: types.Metric{Labels: map[string]string{
						"name": "testing",
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
					},
				},
				{
					Metric: types.Metric{Labels: map[string]string{
						"name": "testing",
					}},
					Points: []types.Point{
						{
							Time:  time.Unix(200, 0),
							Value: 200,
						},
						{
							Time:  time.Unix(250, 0),
							Value: 250,
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := &batch{
				temporaryStorage:  tt.fields.temporaryStorage,
				persistentStorage: tt.fields.persistentStorage,
				states:            tt.fields.states,
				mutex:             tt.fields.mutex,
			}
			_ = tt.fields.temporaryStorage.Set(tt.newMsPoints, nil)
			if err := batch.flush(tt.args.flushQueue, tt.args.currentTime, tt.args.batchTimeLength); (err != nil) != tt.wantErr {
				t.Errorf("Flush() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.fields.persistentStorage.got, tt.want) {
				t.Errorf("Flush() got = %v, want %v", tt.fields.persistentStorage.got, tt.want)
			}
		})
	}
}

func TestBatch_Write(t *testing.T) {
	type fields struct {
		temporaryStorage  Storer
		persistentStorage types.Writer
		states            map[string]state
		mutex             sync.Mutex
	}
	type args struct {
		msPoints        []types.MetricPoints
		currentTime     time.Time
		batchTimeLength float64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]state
		wantErr bool
	}{
		{
			name: "basic",
			fields: fields{
				temporaryStorage:  store.NewStore(),
				persistentStorage: &mockPersistentStorage{},
				states:            make(map[string]state),
				mutex:             sync.Mutex{},
			},
			args: args{
				msPoints: []types.MetricPoints{
					{
						Metric: types.Metric{Labels: map[string]string{
							"name": "testing1",
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
					{
						Metric: types.Metric{Labels: map[string]string{
							"name": "testing2",
						}},
						Points: []types.Point{
							{
								Time:  time.Unix(0, 0),
								Value: 0,
							},
							{
								Time:  time.Unix(250, 0),
								Value: 250,
							},
							{
								Time:  time.Unix(300, 0),
								Value: 300,
							},
						},
					},
				},
				currentTime:     time.Unix(300, 0),
				batchTimeLength: 100,
			},
			want: map[string]state{
				`name="testing1"`: {
					pointCount:     1,
					firstPointTime: time.Unix(300, 0),
					lastPointTime:  time.Unix(300, 0),
					flushDeadline:  time.Unix(400, 0),
				},
				`name="testing2"`: {
					pointCount:     2,
					firstPointTime: time.Unix(250, 0),
					lastPointTime:  time.Unix(300, 0),
					flushDeadline:  time.Unix(350, 0),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := &batch{
				temporaryStorage:  tt.fields.temporaryStorage,
				persistentStorage: tt.fields.persistentStorage,
				states:            tt.fields.states,
				mutex:             tt.fields.mutex,
			}
			if err := batch.write(tt.args.msPoints, tt.args.currentTime, tt.args.batchTimeLength); (err != nil) != tt.wantErr {
				t.Errorf("write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.fields.states, tt.want) {
				t.Errorf("write() states = %v, want %v", tt.fields.states, tt.want)
			}
		})
	}
}

package store

import (
	"hamsterdb/types"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	tests := []struct {
		name string
		want *Store
	}{
		{"basic", &Store{
			msPoints: make(map[string]types.MetricPoints),
			mutex:    sync.Mutex{},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewStore(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewStore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_Append(t *testing.T) {
	type fields struct {
		msPoints map[string]types.MetricPoints
		mutex    sync.Mutex
	}
	type args struct {
		newMsPoints     map[string]types.MetricPoints
		currentMsPoints map[string]types.MetricPoints
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Store
		wantErr bool
	}{
		{ // <--- basic (start)
			"basic",
			fields{
				msPoints: map[string]types.MetricPoints{
					`__name__="current"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "current",
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
						},
					},
				},
				mutex: sync.Mutex{},
			},
			args{
				newMsPoints: map[string]types.MetricPoints{
					`__name__="new"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "new",
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
						},
					},
				},
				currentMsPoints: map[string]types.MetricPoints{
					`__name__="current"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "current",
						}},
						Points: []types.Point{
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
			},
			Store{
				msPoints: map[string]types.MetricPoints{
					`__name__="new"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "new",
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
						},
					},
					`__name__="current"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "current",
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
						},
					},
				},
				mutex: sync.Mutex{},
			},
			false,
		}, // <--- basic (end)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{
				msPoints: tt.fields.msPoints,
				mutex:    tt.fields.mutex,
			}
			if err := store.Append(tt.args.newMsPoints, tt.args.currentMsPoints); (err != nil) != tt.wantErr {
				t.Errorf("Append() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(*store, tt.want) {
				t.Errorf("Append() store = %v, want %v", *store, tt.want)
			}
		})
	}
}

func TestStore_Get(t *testing.T) {
	type fields struct {
		msPoints map[string]types.MetricPoints
		mutex    sync.Mutex
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    types.MetricPoints
		wantErr bool
	}{
		{ // <--- basic (start)
			"basic",
			fields{
				msPoints: map[string]types.MetricPoints{
					`__name__="testing"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "test",
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
				mutex: sync.Mutex{},
			},
			args{key: `__name__="testing"`},
			types.MetricPoints{
				Metric: types.Metric{Labels: map[string]string{
					"__name__": "test",
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
			false,
		}, // <-- basic (end)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{
				msPoints: tt.fields.msPoints,
				mutex:    tt.fields.mutex,
			}
			got, err := store.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_Set(t *testing.T) {
	type fields struct {
		msPoints map[string]types.MetricPoints
		mutex    sync.Mutex
	}
	type args struct {
		newMsPoints     map[string]types.MetricPoints
		currentMsPoints map[string]types.MetricPoints
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Store
		wantErr bool
	}{
		{ // <--- basic (start)
			"basic",
			fields{
				msPoints: map[string]types.MetricPoints{
					`__name__="current"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "current",
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
						},
					},
				},
				mutex: sync.Mutex{},
			},
			args{
				newMsPoints: map[string]types.MetricPoints{
					`__name__="new"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "new",
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
						},
					},
				},
				currentMsPoints: map[string]types.MetricPoints{
					`__name__="current"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "current",
						}},
						Points: []types.Point{
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
			},
			Store{
				msPoints: map[string]types.MetricPoints{
					`__name__="new"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "new",
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
						},
					},
					`__name__="current"`: {
						Metric: types.Metric{Labels: map[string]string{
							"__name__": "current",
						}},
						Points: []types.Point{
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
				mutex: sync.Mutex{},
			},
			false,
		}, // <--- basic (end)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{
				msPoints: tt.fields.msPoints,
				mutex:    tt.fields.mutex,
			}
			if err := store.Set(tt.args.newMsPoints, tt.args.currentMsPoints); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(*store, tt.want) {
				t.Errorf("Set() store = %v, want %v", *store, tt.want)
			}
		})
	}
}

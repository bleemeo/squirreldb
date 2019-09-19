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
		{
			name: "new",
			want: &Store{
				Points: make(map[string][]types.Point),
				mutex:  sync.Mutex{},
			},
		},
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
		Points map[string][]types.Point
		mutex  sync.Mutex
	}
	type args struct {
		newPoints      map[string][]types.Point
		existingPoints map[string][]types.Point
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string][]types.Point
		wantErr bool
	}{
		{
			name: "store_with_points",
			fields: fields{
				Points: map[string][]types.Point{
					`__name__="testing1"`: {
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
					`__name__="testing2"`: {
						{
							Time:  time.Unix(200, 0),
							Value: 200,
						},
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				newPoints: map[string][]types.Point{
					`__name__="testing1"`: {
						{
							Time:  time.Unix(101, 0),
							Value: 101,
						},
					},
				},
				existingPoints: map[string][]types.Point{
					`__name__="testing2"`: {
						{
							Time:  time.Unix(201, 0),
							Value: 201,
						},
					},
				},
			},
			want: map[string][]types.Point{
				`__name__="testing1"`: {
					{
						Time:  time.Unix(100, 0),
						Value: 100,
					},
					{
						Time:  time.Unix(101, 0),
						Value: 101,
					},
				},
				`__name__="testing2"`: {
					{
						Time:  time.Unix(200, 0),
						Value: 200,
					},
					{
						Time:  time.Unix(201, 0),
						Value: 201,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "store_without_points",
			fields: fields{
				Points: make(map[string][]types.Point),
				mutex:  sync.Mutex{},
			},
			args: args{
				newPoints: map[string][]types.Point{
					`__name__="testing1"`: {
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
				},
				existingPoints: map[string][]types.Point{
					`__name__="testing2"`: {
						{
							Time:  time.Unix(200, 0),
							Value: 200,
						},
					},
				},
			},
			want: map[string][]types.Point{
				`__name__="testing1"`: {
					{
						Time:  time.Unix(100, 0),
						Value: 100,
					},
				},
				`__name__="testing2"`: {
					{
						Time:  time.Unix(200, 0),
						Value: 200,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				Points: tt.fields.Points,
				mutex:  tt.fields.mutex,
			}
			if err := s.Append(tt.args.newPoints, tt.args.existingPoints); (err != nil) != tt.wantErr {
				t.Errorf("Append() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(s.Points, tt.want) {
				t.Errorf("Append() s.Points = %v, want %v", s.Points, tt.want)
			}
		})
	}
}

func TestStore_Get(t *testing.T) {
	type fields struct {
		Points map[string][]types.Point
		mutex  sync.Mutex
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []types.Point
		wantErr bool
	}{
		{
			name: "store_with_points",
			fields: fields{
				Points: map[string][]types.Point{
					`__name__="testing1"`: {
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
					`__name__="testing2"`: {
						{
							Time:  time.Unix(200, 0),
							Value: 200,
						},
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				key: `__name__="testing1"`,
			},
			want: []types.Point{
				{
					Time:  time.Unix(100, 0),
					Value: 100,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				Points: tt.fields.Points,
				mutex:  tt.fields.mutex,
			}
			got, err := s.Get(tt.args.key)
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
		Points map[string][]types.Point
		mutex  sync.Mutex
	}
	type args struct {
		newPoints      map[string][]types.Point
		existingPoints map[string][]types.Point
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string][]types.Point
		wantErr bool
	}{
		{
			name: "store_with_points",
			fields: fields{
				Points: map[string][]types.Point{
					`__name__="testing1"`: {
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
					`__name__="testing2"`: {
						{
							Time:  time.Unix(200, 0),
							Value: 200,
						},
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				newPoints: map[string][]types.Point{
					`__name__="testing1"`: {
						{
							Time:  time.Unix(101, 0),
							Value: 101,
						},
					},
				},
				existingPoints: map[string][]types.Point{
					`__name__="testing2"`: {
						{
							Time:  time.Unix(201, 0),
							Value: 201,
						},
					},
				},
			},
			want: map[string][]types.Point{
				`__name__="testing1"`: {
					{
						Time:  time.Unix(101, 0),
						Value: 101,
					},
				},
				`__name__="testing2"`: {
					{
						Time:  time.Unix(201, 0),
						Value: 201,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				Points: tt.fields.Points,
				mutex:  tt.fields.mutex,
			}
			if err := s.Set(tt.args.newPoints, tt.args.existingPoints); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(s.Points, tt.want) {
				t.Errorf("Set() s.Points = %v, want %v", s.Points, tt.want)
			}
		})
	}
}

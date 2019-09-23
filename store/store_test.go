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
				Metrics: make(map[string]Data),
				mutex:   sync.Mutex{},
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

func TestStore_append(t *testing.T) {
	type fields struct {
		Metrics map[string]Data
		mutex   sync.Mutex
	}
	type args struct {
		newPoints      map[string][]types.Point
		existingPoints map[string][]types.Point
		timeToLive     time.Duration
		now            time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]Data
		wantErr bool
	}{
		{
			name: "store_with_points",
			fields: fields{
				Metrics: map[string]Data{
					`__name__="testing1"`: {
						Points: []types.Point{
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
						},
						ExpirationDeadline: time.Unix(0, 0).Add(1 * time.Hour),
					},
					`__name__="testing2"`: {
						Points: []types.Point{
							{
								Time:  time.Unix(200, 0),
								Value: 200,
							},
						},
						ExpirationDeadline: time.Unix(0, 0),
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
				timeToLive: 3600,
				now:        time.Unix(0, 0),
			},
			want: map[string]Data{
				`__name__="testing1"`: {
					Points: []types.Point{
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
						{
							Time:  time.Unix(101, 0),
							Value: 101,
						},
					},
					ExpirationDeadline: time.Unix(3600, 0),
				},
				`__name__="testing2"`: {
					Points: []types.Point{
						{
							Time:  time.Unix(200, 0),
							Value: 200,
						},
						{
							Time:  time.Unix(201, 0),
							Value: 201,
						},
					},
					ExpirationDeadline: time.Unix(3600, 0),
				},
			},
			wantErr: false,
		},
		{
			name: "store_without_points",
			fields: fields{
				Metrics: make(map[string]Data),
				mutex:   sync.Mutex{},
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
				timeToLive: 3600,
				now:        time.Unix(0, 0),
			},
			want: map[string]Data{
				`__name__="testing1"`: {
					Points: []types.Point{
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
					},
					ExpirationDeadline: time.Unix(3600, 0),
				},
				`__name__="testing2"`: {
					Points: []types.Point{
						{
							Time:  time.Unix(200, 0),
							Value: 200,
						},
					},
					ExpirationDeadline: time.Unix(3600, 0),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				Metrics: tt.fields.Metrics,
				mutex:   tt.fields.mutex,
			}
			if err := s.append(tt.args.newPoints, tt.args.existingPoints, tt.args.now, tt.args.timeToLive); (err != nil) != tt.wantErr {
				t.Errorf("append() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(s.Metrics, tt.want) {
				t.Errorf("append() s.Metrics = %v, want %v", s.Metrics, tt.want)
			}
		})
	}
}

func TestStore_Get(t *testing.T) {
	type fields struct {
		Metrics map[string]Data
		mutex   sync.Mutex
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
				Metrics: map[string]Data{
					`__name__="testing1"`: {
						Points: []types.Point{
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
						},
					},
					`__name__="testing2"`: {
						Points: []types.Point{
							{
								Time:  time.Unix(200, 0),
								Value: 200,
							},
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
				Metrics: tt.fields.Metrics,
				mutex:   tt.fields.mutex,
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

func TestStore_set(t *testing.T) {
	type fields struct {
		Points map[string]Data
		mutex  sync.Mutex
	}
	type args struct {
		newPoints      map[string][]types.Point
		existingPoints map[string][]types.Point
		timeToLive     time.Duration
		now            time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]Data
		wantErr bool
	}{
		{
			name: "store_with_points",
			fields: fields{
				Points: map[string]Data{
					`__name__="testing1"`: {
						Points: []types.Point{
							{
								Time:  time.Unix(100, 0),
								Value: 100,
							},
						},
					},
					`__name__="testing2"`: {
						Points: []types.Point{
							{
								Time:  time.Unix(200, 0),
								Value: 200,
							},
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
				timeToLive: 1800,
				now:        time.Unix(0, 0),
			},
			want: map[string]Data{
				`__name__="testing1"`: {
					Points: []types.Point{
						{
							Time:  time.Unix(101, 0),
							Value: 101,
						},
					},
					ExpirationDeadline: time.Unix(1800, 0),
				},
				`__name__="testing2"`: {
					Points: []types.Point{
						{
							Time:  time.Unix(201, 0),
							Value: 201,
						},
					},
					ExpirationDeadline: time.Unix(1800, 0),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				Metrics: tt.fields.Points,
				mutex:   tt.fields.mutex,
			}
			if err := s.set(tt.args.newPoints, tt.args.existingPoints, tt.args.now, tt.args.timeToLive); (err != nil) != tt.wantErr {
				t.Errorf("set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(s.Metrics, tt.want) {
				t.Errorf("set() s.Metrics = %v, want %v", s.Metrics, tt.want)
			}
		})
	}
}

package store

import (
	"reflect"
	"squirreldb/types"
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
			name: "test_new",
			want: &Store{
				metrics: make(map[string]Data),
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
		metrics map[string]Data
		mutex   sync.Mutex
	}
	type args struct {
		newPoints      map[string][]types.Point
		existingPoints map[string][]types.Point
		now            time.Time
		timeToLive     time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]Data
		wantErr bool
	}{
		{
			name: "test_empty_store",
			fields: fields{
				metrics: make(map[string]Data),
				mutex:   sync.Mutex{},
			},
			args: args{
				newPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
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
				existingPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000001": {
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
				now:        time.Unix(300, 0),
				timeToLive: 300 * time.Second,
			},
			want: map[string]Data{
				"00000000-0000-0000-0000-000000000000": {
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
					ExpirationDeadline: time.Unix(600, 0),
				},
				"00000000-0000-0000-0000-000000000001": {
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
					ExpirationDeadline: time.Unix(600, 0),
				},
			},
			wantErr: false,
		},
		{
			name: "test_filled_store",
			fields: fields{
				metrics: map[string]Data{
					"00000000-0000-0000-0000-000000000000": {
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
						ExpirationDeadline: time.Unix(0, 0),
					},
					"00000000-0000-0000-0000-000000000001": {
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
						ExpirationDeadline: time.Unix(0, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				newPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
						{
							Time:  time.Unix(25, 0),
							Value: 25,
						},
						{
							Time:  time.Unix(75, 0),
							Value: 75,
						},
					},
				},
				existingPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000001": {
						{
							Time:  time.Unix(125, 0),
							Value: 125,
						},
						{
							Time:  time.Unix(175, 0),
							Value: 175,
						},
					},
				},
				now:        time.Unix(300, 0),
				timeToLive: 300 * time.Second,
			},
			want: map[string]Data{
				"00000000-0000-0000-0000-000000000000": {
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
							Time:  time.Unix(25, 0),
							Value: 25,
						},
						{
							Time:  time.Unix(75, 0),
							Value: 75,
						},
					},
					ExpirationDeadline: time.Unix(600, 0),
				},
				"00000000-0000-0000-0000-000000000001": {
					Points: []types.Point{
						{
							Time:  time.Unix(100, 0),
							Value: 100,
						},
						{
							Time:  time.Unix(150, 0),
							Value: 150,
						},
						{
							Time:  time.Unix(125, 0),
							Value: 125,
						},
						{
							Time:  time.Unix(175, 0),
							Value: 175,
						},
					},
					ExpirationDeadline: time.Unix(600, 0),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
				mutex:   tt.fields.mutex,
			}
			if err := s.append(tt.args.newPoints, tt.args.existingPoints, tt.args.now, tt.args.timeToLive); (err != nil) != tt.wantErr {
				t.Errorf("append() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStore_expire(t *testing.T) {
	type fields struct {
		metrics map[string]Data
		mutex   sync.Mutex
	}
	type args struct {
		now time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]Data
	}{
		{
			name: "test_no_expiration",
			fields: fields{
				metrics: map[string]Data{
					"00000000-0000-0000-0000-000000000000": {
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
						ExpirationDeadline: time.Unix(300, 0),
					},
					"00000000-0000-0000-0000-000000000001": {
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
						ExpirationDeadline: time.Unix(600, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{now: time.Unix(300, 0)},
			want: map[string]Data{
				"00000000-0000-0000-0000-000000000000": {
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
					ExpirationDeadline: time.Unix(300, 0),
				},
				"00000000-0000-0000-0000-000000000001": {
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
					ExpirationDeadline: time.Unix(600, 0),
				},
			},
		},
		{
			name: "test_no_expiration",
			fields: fields{
				metrics: map[string]Data{
					"00000000-0000-0000-0000-000000000000": {
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
						ExpirationDeadline: time.Unix(300, 0),
					},
					"00000000-0000-0000-0000-000000000001": {
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
						ExpirationDeadline: time.Unix(600, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{now: time.Unix(600, 0)},
			want: map[string]Data{
				"00000000-0000-0000-0000-000000000001": {
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
					ExpirationDeadline: time.Unix(600, 0),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
				mutex:   tt.fields.mutex,
			}
			s.expire(tt.args.now)
		})
	}
}

func TestStore_get(t *testing.T) {
	type fields struct {
		metrics map[string]Data
		mutex   sync.Mutex
	}
	type args struct {
		keys []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string][]types.Point
		wantErr bool
	}{
		{
			name: "test_empty_store",
			fields: fields{
				metrics: make(map[string]Data),
				mutex:   sync.Mutex{},
			},
			args: args{keys: []string{
				"00000000-0000-0000-0000-000000000000",
				"00000000-0000-0000-0000-000000000001",
			}},
			want:    make(map[string][]types.Point),
			wantErr: false,
		},
		{
			name: "test_filled_store",
			fields: fields{
				metrics: map[string]Data{
					"00000000-0000-0000-0000-000000000000": {
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
						ExpirationDeadline: time.Unix(0, 0),
					},
					"00000000-0000-0000-0000-000000000001": {
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
						ExpirationDeadline: time.Unix(0, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{keys: []string{
				"00000000-0000-0000-0000-000000000000",
				"00000000-0000-0000-0000-000000000001",
			}},
			want: map[string][]types.Point{
				"00000000-0000-0000-0000-000000000000": {
					{
						Time:  time.Unix(0, 0),
						Value: 0,
					},
					{
						Time:  time.Unix(50, 0),
						Value: 50,
					},
				},
				"00000000-0000-0000-0000-000000000001": {
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
				mutex:   tt.fields.mutex,
			}
			got, err := s.get(tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_set(t *testing.T) {
	type fields struct {
		metrics map[string]Data
		mutex   sync.Mutex
	}
	type args struct {
		newPoints      map[string][]types.Point
		existingPoints map[string][]types.Point
		now            time.Time
		timeToLive     time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]Data
		wantErr bool
	}{
		{
			name: "test_empty_store",
			fields: fields{
				metrics: make(map[string]Data),
				mutex:   sync.Mutex{},
			},
			args: args{
				newPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
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
				existingPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000001": {
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
				now:        time.Unix(300, 0),
				timeToLive: 300 * time.Second,
			},
			want: map[string]Data{
				"00000000-0000-0000-0000-000000000000": {
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
					ExpirationDeadline: time.Unix(600, 0),
				},
				"00000000-0000-0000-0000-000000000001": {
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
					ExpirationDeadline: time.Unix(600, 0),
				},
			},
			wantErr: false,
		},
		{
			name: "test_filled_store",
			fields: fields{
				metrics: map[string]Data{
					"00000000-0000-0000-0000-000000000000": {
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
						ExpirationDeadline: time.Unix(0, 0),
					},
					"00000000-0000-0000-0000-000000000001": {
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
						ExpirationDeadline: time.Unix(0, 0),
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				newPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000000": {
						{
							Time:  time.Unix(25, 0),
							Value: 25,
						},
						{
							Time:  time.Unix(75, 0),
							Value: 75,
						},
					},
				},
				existingPoints: map[string][]types.Point{
					"00000000-0000-0000-0000-000000000001": {
						{
							Time:  time.Unix(125, 0),
							Value: 125,
						},
						{
							Time:  time.Unix(175, 0),
							Value: 175,
						},
					},
				},
				now:        time.Unix(300, 0),
				timeToLive: 300 * time.Second,
			},
			want: map[string]Data{
				"00000000-0000-0000-0000-000000000000": {
					Points: []types.Point{
						{
							Time:  time.Unix(25, 0),
							Value: 25,
						},
						{
							Time:  time.Unix(75, 0),
							Value: 75,
						},
					},
					ExpirationDeadline: time.Unix(600, 0),
				},
				"00000000-0000-0000-0000-000000000001": {
					Points: []types.Point{
						{
							Time:  time.Unix(125, 0),
							Value: 125,
						},
						{
							Time:  time.Unix(175, 0),
							Value: 175,
						},
					},
					ExpirationDeadline: time.Unix(600, 0),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
				mutex:   tt.fields.mutex,
			}
			if err := s.set(tt.args.newPoints, tt.args.existingPoints, tt.args.now, tt.args.timeToLive); (err != nil) != tt.wantErr {
				t.Errorf("set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

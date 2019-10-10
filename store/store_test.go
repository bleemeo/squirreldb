package store

import (
	"reflect"
	"squirreldb/types"
	"sync"
	"testing"
	"time"
)

func uuidify(value string) types.MetricUUID {
	uuid := types.MetricLabels{
		{
			Name:  "__bleemeo_uuid__",
			Value: value,
		},
	}.UUID()

	return uuid
}

func TestNewStore(t *testing.T) {
	tests := []struct {
		name string
		want *Store
	}{
		{
			name: "new",
			want: &Store{
				Metrics: make(map[types.MetricUUID]metric),
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
		Metrics map[types.MetricUUID]metric
		mutex   sync.Mutex
	}
	type args struct {
		newMetrics      types.Metrics
		actualMetrics   types.Metrics
		now             time.Time
		timestampToLive int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[types.MetricUUID]metric
		wantErr bool
	}{
		{
			name: "empty_store",
			fields: fields{
				Metrics: make(map[types.MetricUUID]metric),
			},
			args: args{
				newMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000000"): {
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
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						{
							Timestamp: 150,
							Value:     150,
						},
						{
							Timestamp: 200,
							Value:     200,
						},
						{
							Timestamp: 250,
							Value:     250,
						},
					},
				},
				now:             time.Unix(0, 0),
				timestampToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000000"): {
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
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
						{
							Timestamp: 150,
							Value:     150,
						},
						{
							Timestamp: 200,
							Value:     200,
						},
						{
							Timestamp: 250,
							Value:     250,
						},
					},
					ExpirationTimestamp: 300,
				},
			},
			wantErr: false,
		},
		{
			name: "filled_store",
			fields: fields{
				Metrics: map[types.MetricUUID]metric{
					uuidify("00000000-0000-0000-0000-000000000000"): {
						Points: types.MetricPoints{
							{
								Timestamp: 25,
								Value:     25,
							},
							{
								Timestamp: 75,
								Value:     75,
							},
						},
						ExpirationTimestamp: 0,
					},
				},
			},
			args: args{
				newMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000000"): {
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
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						{
							Timestamp: 150,
							Value:     150,
						},
						{
							Timestamp: 200,
							Value:     200,
						},
						{
							Timestamp: 250,
							Value:     250,
						},
					},
				},
				now:             time.Unix(0, 0),
				timestampToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000000"): {
					Points: types.MetricPoints{
						{
							Timestamp: 25,
							Value:     25,
						},
						{
							Timestamp: 75,
							Value:     75,
						},
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
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
						{
							Timestamp: 150,
							Value:     150,
						},
						{
							Timestamp: 200,
							Value:     200,
						},
						{
							Timestamp: 250,
							Value:     250,
						},
					},
					ExpirationTimestamp: 300,
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
			if err := s.append(tt.args.newMetrics, tt.args.actualMetrics, tt.args.now, tt.args.timestampToLive); (err != nil) != tt.wantErr {
				t.Errorf("append() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(s.Metrics, tt.want) {
				t.Errorf("append() s.Metrics = %v, want %v", s.Metrics, tt.want)
			}
		})
	}
}

func TestStore_expire(t *testing.T) {
	type fields struct {
		Metrics map[types.MetricUUID]metric
		mutex   sync.Mutex
	}
	type args struct {
		now time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[types.MetricUUID]metric
	}{
		{
			name: "no_expiration",
			fields: fields{
				Metrics: map[types.MetricUUID]metric{
					uuidify("00000000-0000-0000-0000-000000000000"): {
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
						ExpirationTimestamp: 300,
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				now: time.Unix(0, 0),
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000000"): {
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
					ExpirationTimestamp: 300,
				},
			},
		},
		{
			name: "expiration",
			fields: fields{
				Metrics: map[types.MetricUUID]metric{
					uuidify("00000000-0000-0000-0000-000000000000"): {
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
						ExpirationTimestamp: 300,
					},
				},
				mutex: sync.Mutex{},
			},
			args: args{
				now: time.Unix(600, 0),
			},
			want: make(map[types.MetricUUID]metric),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				Metrics: tt.fields.Metrics,
				mutex:   tt.fields.mutex,
			}
			s.expire(tt.args.now)
			if !reflect.DeepEqual(s.Metrics, tt.want) {
				t.Errorf("expire() s.Metrics = %v, want %v", s.Metrics, tt.want)
			}
		})
	}
}

func TestStore_get(t *testing.T) {
	type fields struct {
		Metrics map[types.MetricUUID]metric
		mutex   sync.Mutex
	}
	type args struct {
		uuids []types.MetricUUID
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    types.Metrics
		wantErr bool
	}{
		{
			name: "empty_store",
			fields: fields{
				Metrics: make(map[types.MetricUUID]metric),
			},
			args: args{uuids: []types.MetricUUID{
				uuidify("00000000-0000-0000-0000-000000000000"),
			}},
			want:    make(types.Metrics),
			wantErr: false,
		},
		{
			name: "filled_store",
			fields: fields{
				Metrics: map[types.MetricUUID]metric{
					uuidify("00000000-0000-0000-0000-000000000000"): {
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
						ExpirationTimestamp: 300,
					},
				},
			},
			args: args{uuids: []types.MetricUUID{
				uuidify("00000000-0000-0000-0000-000000000000"),
			}},
			want: types.Metrics{
				uuidify("00000000-0000-0000-0000-000000000000"): {
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				Metrics: tt.fields.Metrics,
				mutex:   tt.fields.mutex,
			}
			got, err := s.get(tt.args.uuids)
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
		Metrics map[types.MetricUUID]metric
		mutex   sync.Mutex
	}
	type args struct {
		newMetrics      types.Metrics
		actualMetrics   types.Metrics
		now             time.Time
		timestampToLive int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[types.MetricUUID]metric
		wantErr bool
	}{
		{
			name: "empty_store",
			fields: fields{
				Metrics: make(map[types.MetricUUID]metric),
			},
			args: args{
				newMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000000"): {
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
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						{
							Timestamp: 150,
							Value:     150,
						},
						{
							Timestamp: 200,
							Value:     200,
						},
						{
							Timestamp: 250,
							Value:     250,
						},
					},
				},
				now:             time.Unix(0, 0),
				timestampToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000000"): {
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
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
						{
							Timestamp: 150,
							Value:     150,
						},
						{
							Timestamp: 200,
							Value:     200,
						},
						{
							Timestamp: 250,
							Value:     250,
						},
					},
					ExpirationTimestamp: 300,
				},
			},
			wantErr: false,
		},
		{
			name: "filled_store",
			fields: fields{
				Metrics: map[types.MetricUUID]metric{
					uuidify("00000000-0000-0000-0000-000000000000"): {
						Points: types.MetricPoints{
							{
								Timestamp: 25,
								Value:     25,
							},
							{
								Timestamp: 75,
								Value:     75,
							},
						},
						ExpirationTimestamp: 0,
					},
				},
			},
			args: args{
				newMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000000"): {
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
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						{
							Timestamp: 150,
							Value:     150,
						},
						{
							Timestamp: 200,
							Value:     200,
						},
						{
							Timestamp: 250,
							Value:     250,
						},
					},
				},
				now:             time.Unix(0, 0),
				timestampToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000000"): {
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
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000001"): {
					Points: types.MetricPoints{
						{
							Timestamp: 150,
							Value:     150,
						},
						{
							Timestamp: 200,
							Value:     200,
						},
						{
							Timestamp: 250,
							Value:     250,
						},
					},
					ExpirationTimestamp: 300,
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
			if err := s.set(tt.args.newMetrics, tt.args.actualMetrics, tt.args.now, tt.args.timestampToLive); (err != nil) != tt.wantErr {
				t.Errorf("set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

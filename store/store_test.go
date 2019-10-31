package store

import (
	gouuid "github.com/gofrs/uuid"
	"reflect"
	"squirreldb/types"
	"testing"
	"time"
)

func uuidify(value string) types.MetricUUID {
	uuid := types.MetricUUID{
		UUID: gouuid.FromStringOrNil(value),
	}

	return uuid
}

func TestNewStore(t *testing.T) {
	type args struct {
		batchSize int64
		offset    int64
	}
	tests := []struct {
		name string
		args args
		want *Store
	}{
		{
			name: "new",
			args: args{
				batchSize: 300,
				offset:    150,
			},
			want: &Store{
				metrics: make(map[types.MetricUUID]metric),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStore_append(t *testing.T) {
	type fields struct {
		metrics map[types.MetricUUID]metric
	}
	type args struct {
		newMetrics    types.Metrics
		actualMetrics types.Metrics
		now           time.Time
		timeToLive    int64
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
				metrics: make(map[types.MetricUUID]metric),
			},
			args: args{
				newMetrics: types.Metrics{
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
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000002"): {
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
						TimeToLive: 3600,
					},
				},
				now:        time.Unix(0, 0),
				timeToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
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
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
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
						TimeToLive: 3600,
					},
					ExpirationTimestamp: 300,
				},
			},
			wantErr: false,
		},
		{
			name: "filled_store",
			fields: fields{
				metrics: map[types.MetricUUID]metric{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
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
							TimeToLive: 3600,
						},
						ExpirationTimestamp: 0,
					},
				},
			},
			args: args{
				newMetrics: types.Metrics{
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
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000002"): {
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
						TimeToLive: 3600,
					},
				},
				now:        time.Unix(0, 0),
				timeToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
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
						TimeToLive: 3600,
					},
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
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
						TimeToLive: 3600,
					},
					ExpirationTimestamp: 300,
				},
			},
			wantErr: false,
		},
		{
			name: "replace_time_to_live",
			fields: fields{
				metrics: make(map[types.MetricUUID]metric),
			},
			args: args{
				newMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
							{
								Timestamp: 0,
								Value:     0,
							},
						},
						TimeToLive: 3600,
					},
					uuidify("00000000-0000-0000-0000-000000000002"): {
						Points: types.MetricPoints{
							{
								Timestamp: 100,
								Value:     100,
							},
						},
						TimeToLive: 3600,
					},
				},
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						Points: types.MetricPoints{
							{
								Timestamp: 50,
								Value:     50,
							},
						},
						TimeToLive: 86400,
					},
					uuidify("00000000-0000-0000-0000-000000000002"): {
						Points: types.MetricPoints{
							{
								Timestamp: 150,
								Value:     150,
							},
						},
						TimeToLive: 1800,
					},
				},
				now:        time.Unix(0, 0),
				timeToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						Points: types.MetricPoints{
							{
								Timestamp: 0,
								Value:     0,
							},
							{
								Timestamp: 50,
								Value:     50,
							},
						},
						TimeToLive: 86400,
					},
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						Points: types.MetricPoints{
							{
								Timestamp: 100,
								Value:     100,
							},
							{
								Timestamp: 150,
								Value:     150,
							},
						},
						TimeToLive: 3600,
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
				metrics: tt.fields.metrics,
			}
			if err := s.append(tt.args.newMetrics, tt.args.actualMetrics, tt.args.now, tt.args.timeToLive); (err != nil) != tt.wantErr {
				t.Errorf("append() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(s.metrics, tt.want) {
				t.Errorf("append() s.metrics = %v, want %v", s.metrics, tt.want)
			}
		})
	}
}

func TestStore_expire(t *testing.T) {
	type fields struct {
		Metrics map[types.MetricUUID]metric
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
					uuidify("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
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
						ExpirationTimestamp: 300,
					},
				},
			},
			args: args{
				now: time.Unix(0, 0),
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
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
					ExpirationTimestamp: 300,
				},
			},
		},
		{
			name: "expiration",
			fields: fields{
				Metrics: map[types.MetricUUID]metric{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
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
						ExpirationTimestamp: 300,
					},
				},
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
				metrics: tt.fields.Metrics,
			}
			s.expire(tt.args.now)
			if !reflect.DeepEqual(s.metrics, tt.want) {
				t.Errorf("expire() s.metrics = %v, want %v", s.metrics, tt.want)
			}
		})
	}
}

func TestStore_get(t *testing.T) {
	type fields struct {
		Metrics map[types.MetricUUID]metric
	}
	type args struct {
		uuids types.MetricUUIDs
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
			args: args{uuids: types.MetricUUIDs{
				uuidify("00000000-0000-0000-0000-000000000001"),
			}},
			want:    make(types.Metrics),
			wantErr: false,
		},
		{
			name: "filled_store",
			fields: fields{
				Metrics: map[types.MetricUUID]metric{
					uuidify("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
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
						ExpirationTimestamp: 300,
					},
				},
			},
			args: args{uuids: types.MetricUUIDs{
				uuidify("00000000-0000-0000-0000-000000000001"),
			}},
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.Metrics,
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
	}
	type args struct {
		newMetrics    types.Metrics
		actualMetrics types.Metrics
		now           time.Time
		timeToLive    int64
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
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000002"): {
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
						TimeToLive: 3600,
					},
				},
				now:        time.Unix(0, 0),
				timeToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
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
						TimeToLive: 3600,
					},
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
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
						TimeToLive: 3600,
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
					uuidify("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
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
							TimeToLive: 3600,
						},
						ExpirationTimestamp: 0,
					},
				},
			},
			args: args{
				newMetrics: types.Metrics{
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
				actualMetrics: types.Metrics{
					uuidify("00000000-0000-0000-0000-000000000002"): {
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
						TimeToLive: 3600,
					},
				},
				now:        time.Unix(0, 0),
				timeToLive: 300,
			},
			want: map[types.MetricUUID]metric{
				uuidify("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
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
					ExpirationTimestamp: 300,
				},
				uuidify("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
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
						TimeToLive: 3600,
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
				metrics: tt.fields.Metrics,
			}
			if err := s.set(tt.args.newMetrics, tt.args.actualMetrics, tt.args.now, tt.args.timeToLive); (err != nil) != tt.wantErr {
				t.Errorf("set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

package store

import (
	"reflect"
	"squirreldb/types"
	"testing"
	"time"
)

func uuidFromStringOrNil(s string) types.MetricUUID {
	uuid, _ := types.UUIDFromString(s)

	return uuid
}

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		want *Store
	}{
		{
			name: "new",
			want: &Store{
				metrics: make(map[types.MetricUUID]storeData),
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
		metrics map[types.MetricUUID]storeData
	}
	type args struct {
		newMetrics      map[types.MetricUUID]types.MetricData
		existingMetrics map[types.MetricUUID]types.MetricData
		timeToLive      int64
		now             time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[types.MetricUUID]storeData
		wantErr bool
	}{
		{
			name: "store_filled",
			fields: fields{
				metrics: map[types.MetricUUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
							},
							TimeToLive: 150,
						},
						expirationTimestamp: 400,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20,
									Value:     100,
								},
							},
							TimeToLive: 2400,
						},
						expirationTimestamp: 400,
					},
				},
			},
			args: args{
				newMetrics: map[types.MetricUUID]types.MetricData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
							{
								Timestamp: 50,
								Value:     60,
							},
							{
								Timestamp: 60,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
				},
				existingMetrics: map[types.MetricUUID]types.MetricData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
							{
								Timestamp: 100,
								Value:     300,
							},
							{
								Timestamp: 120,
								Value:     350,
							},
						},
						TimeToLive: 1200,
					},
				},
				timeToLive: 1200,
				now:        time.Unix(400, 0),
			},
			want: map[types.MetricUUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
							{
								Timestamp: 50,
								Value:     60,
							},
							{
								Timestamp: 60,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
					expirationTimestamp: 1600,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
							{
								Timestamp: 100,
								Value:     300,
							},
							{
								Timestamp: 120,
								Value:     350,
							},
						},
						TimeToLive: 2400,
					},
					expirationTimestamp: 1600,
				},
			},
			wantErr: false,
		},
		{
			name: "store_empty",
			fields: fields{
				metrics: make(map[types.MetricUUID]storeData),
			},
			args: args{
				newMetrics: map[types.MetricUUID]types.MetricData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
				},
				existingMetrics: map[types.MetricUUID]types.MetricData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
				},
				timeToLive: 600,
				now:        time.Unix(200, 0),
			},
			want: map[types.MetricUUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
					expirationTimestamp: 800,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
					expirationTimestamp: 800,
				},
			},
			wantErr: false,
		},
		{
			name: "store_filled_metrics_empty",
			fields: fields{
				metrics: map[types.MetricUUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
							},
							TimeToLive: 150,
						},
						expirationTimestamp: 400,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20,
									Value:     100,
								},
							},
							TimeToLive: 2400,
						},
						expirationTimestamp: 400,
					},
				},
			},
			args: args{
				newMetrics:      nil,
				existingMetrics: nil,
				timeToLive:      600,
				now:             time.Unix(200, 0),
			},
			want: map[types.MetricUUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
						},
						TimeToLive: 150,
					},
					expirationTimestamp: 400,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
						},
						TimeToLive: 2400,
					},
					expirationTimestamp: 400,
				},
			},
			wantErr: false,
		},
		{
			name: "store_empty_metrics_empty",
			fields: fields{
				metrics: make(map[types.MetricUUID]storeData),
			},
			args: args{
				newMetrics:      nil,
				existingMetrics: nil,
				timeToLive:      600,
				now:             time.Unix(200, 0),
			},
			want:    make(map[types.MetricUUID]storeData),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
			}
			if err := s.append(tt.args.newMetrics, tt.args.existingMetrics, tt.args.timeToLive, tt.args.now); (err != nil) != tt.wantErr {
				t.Errorf("append() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(s.metrics, tt.want) {
				t.Errorf("append() metrics = %v, want %v", s.metrics, tt.want)
			}
		})
	}
}

func TestStore_expire(t *testing.T) {
	type fields struct {
		metrics map[types.MetricUUID]storeData
	}
	type args struct {
		now time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[types.MetricUUID]storeData
	}{
		{
			name: "no_expire",
			fields: fields{
				metrics: map[types.MetricUUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData:          types.MetricData{},
						expirationTimestamp: 800,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						MetricData:          types.MetricData{},
						expirationTimestamp: 1600,
					},
				},
			},
			args: args{
				now: time.Unix(600, 0),
			},
			want: map[types.MetricUUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData:          types.MetricData{},
					expirationTimestamp: 800,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData:          types.MetricData{},
					expirationTimestamp: 1600,
				},
			},
		},
		{
			name: "expire",
			fields: fields{
				metrics: map[types.MetricUUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData:          types.MetricData{},
						expirationTimestamp: 800,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						MetricData:          types.MetricData{},
						expirationTimestamp: 1600,
					},
				},
			},
			args: args{
				now: time.Unix(1200, 0),
			},
			want: map[types.MetricUUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData:          types.MetricData{},
					expirationTimestamp: 1600,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
			}
			s.expire(tt.args.now)
			if !reflect.DeepEqual(s.metrics, tt.want) {
				t.Errorf("expire() metrics = %v, want %v", s.metrics, tt.want)
			}
		})
	}
}

func TestStore_get(t *testing.T) {
	type fields struct {
		metrics map[types.MetricUUID]storeData
	}
	type args struct {
		uuids []types.MetricUUID
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[types.MetricUUID]types.MetricData
		wantErr bool
	}{
		{
			name: "store_empty",
			fields: fields{
				metrics: make(map[types.MetricUUID]storeData),
			},
			args: args{
				uuids: []types.MetricUUID{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				},
			},
			want:    make(map[types.MetricUUID]types.MetricData),
			wantErr: false,
		},
		{
			name: "store_filled",
			fields: fields{
				metrics: map[types.MetricUUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
								{
									Timestamp: 20,
									Value:     30,
								},
								{
									Timestamp: 30,
									Value:     40,
								},
								{
									Timestamp: 40,
									Value:     50,
								},
							},
							TimeToLive: 300,
						},
						expirationTimestamp: 800,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20,
									Value:     100,
								},
								{
									Timestamp: 40,
									Value:     150,
								},
								{
									Timestamp: 60,
									Value:     200,
								},
								{
									Timestamp: 80,
									Value:     250,
								},
							},
							TimeToLive: 1200,
						},
						expirationTimestamp: 800,
					},
				},
			},
			args: args{
				uuids: []types.MetricUUID{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				},
			},
			want: map[types.MetricUUID]types.MetricData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					Points: []types.MetricPoint{
						{
							Timestamp: 0,
							Value:     10,
						},
						{
							Timestamp: 10,
							Value:     20,
						},
						{
							Timestamp: 20,
							Value:     30,
						},
						{
							Timestamp: 30,
							Value:     40,
						},
						{
							Timestamp: 40,
							Value:     50,
						},
					},
					TimeToLive: 300,
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
		metrics map[types.MetricUUID]storeData
	}
	type args struct {
		metrics    map[types.MetricUUID]types.MetricData
		timeToLive int64
		now        time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[types.MetricUUID]storeData
		wantErr bool
	}{
		{
			name: "store_filled",
			fields: fields{
				metrics: map[types.MetricUUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
							},
							TimeToLive: 150,
						},
						expirationTimestamp: 800,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20,
									Value:     100,
								},
							},
							TimeToLive: 2400,
						},
						expirationTimestamp: 800,
					},
				},
			},
			args: args{
				metrics: map[types.MetricUUID]types.MetricData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
							{
								Timestamp: 50,
								Value:     60,
							},
							{
								Timestamp: 60,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
							{
								Timestamp: 100,
								Value:     300,
							},
							{
								Timestamp: 120,
								Value:     350,
							},
						},
						TimeToLive: 1200,
					},
				},
				timeToLive: 1200,
				now:        time.Unix(400, 0),
			},
			want: map[types.MetricUUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
							{
								Timestamp: 50,
								Value:     60,
							},
							{
								Timestamp: 60,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
					expirationTimestamp: 1600,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
							{
								Timestamp: 100,
								Value:     300,
							},
							{
								Timestamp: 120,
								Value:     350,
							},
						},
						TimeToLive: 1200,
					},
					expirationTimestamp: 1600,
				},
			},
			wantErr: false,
		},
		{
			name: "store_empty",
			fields: fields{
				metrics: make(map[types.MetricUUID]storeData),
			},
			args: args{
				metrics: map[types.MetricUUID]types.MetricData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
				},
				timeToLive: 600,
				now:        time.Unix(200, 0),
			},
			want: map[types.MetricUUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
							{
								Timestamp: 20,
								Value:     30,
							},
							{
								Timestamp: 30,
								Value:     40,
							},
							{
								Timestamp: 40,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
					expirationTimestamp: 800,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
							{
								Timestamp: 40,
								Value:     150,
							},
							{
								Timestamp: 60,
								Value:     200,
							},
							{
								Timestamp: 80,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
					expirationTimestamp: 800,
				},
			},
			wantErr: false,
		},
		{
			name: "store_filled_metrics_empty",
			fields: fields{
				metrics: map[types.MetricUUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10,
									Value:     20,
								},
							},
							TimeToLive: 150,
						},
						expirationTimestamp: 400,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20,
									Value:     100,
								},
							},
							TimeToLive: 2400,
						},
						expirationTimestamp: 400,
					},
				},
			},
			args: args{
				metrics:    nil,
				timeToLive: 600,
				now:        time.Unix(200, 0),
			},
			want: map[types.MetricUUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10,
								Value:     20,
							},
						},
						TimeToLive: 150,
					},
					expirationTimestamp: 400,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20,
								Value:     100,
							},
						},
						TimeToLive: 2400,
					},
					expirationTimestamp: 400,
				},
			},
			wantErr: false,
		},
		{
			name: "store_empty_metrics_empty",
			fields: fields{
				metrics: make(map[types.MetricUUID]storeData),
			},
			args: args{
				metrics:    nil,
				timeToLive: 600,
				now:        time.Unix(200, 0),
			},
			want:    make(map[types.MetricUUID]storeData),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
			}
			if err := s.set(tt.args.metrics, tt.args.timeToLive, tt.args.now); (err != nil) != tt.wantErr {
				t.Errorf("set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

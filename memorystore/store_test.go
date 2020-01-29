package memorystore

import (
	"reflect"
	"squirreldb/types"
	"testing"
	"time"

	gouuid "github.com/gofrs/uuid"
)

func uuidFromStringOrNil(s string) gouuid.UUID {
	uuid, _ := gouuid.FromString(s)

	return uuid
}

func TestAppend(t *testing.T) {
	type fields struct {
		metrics map[gouuid.UUID]storeData
	}
	type args struct {
		points []types.MetricData
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      []int
		wantState map[gouuid.UUID]storeData
	}{
		{
			name: "store_filled",
			fields: fields{
				metrics: map[gouuid.UUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
							UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
						expirationTimestamp: 0,
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						MetricData: types.MetricData{
							UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
						expirationTimestamp: 0,
					},
				},
			},
			args: args{
				points: []types.MetricData{
					{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
			},
			wantState: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					expirationTimestamp: 0,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
					expirationTimestamp: 0,
				},
			},
			want: []int{7, 7},
		},
		{
			name: "store_empty",
			fields: fields{
				metrics: make(map[gouuid.UUID]storeData),
			},
			args: args{
				points: []types.MetricData{
					{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
			},
			wantState: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					expirationTimestamp: 0,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
					expirationTimestamp: 0,
				},
			},
			want: []int{5, 5},
		},
		{
			name: "store_filled_metrics_empty",
			fields: fields{
				metrics: map[gouuid.UUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						MetricData: types.MetricData{
							UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
							UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
				points: nil,
			},
			wantState: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
			want: nil,
		},
		{
			name: "store_empty_metrics_empty",
			fields: fields{
				metrics: make(map[gouuid.UUID]storeData),
			},
			args: args{
				points: nil,
			},
			wantState: make(map[gouuid.UUID]storeData),
			want:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
			}
			got, err := s.Append(tt.args.points)
			if err != nil {
				t.Errorf("Append() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Append() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(s.metrics, tt.wantState) {
				t.Errorf("Append() metrics = %v, want %v", s.metrics, tt.wantState)
			}
		})
	}
}

func TestStore_expire(t *testing.T) {
	type fields struct {
		metrics map[gouuid.UUID]storeData
	}
	type args struct {
		now time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[gouuid.UUID]storeData
	}{
		{
			name: "no_expire",
			fields: fields{
				metrics: map[gouuid.UUID]storeData{
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
			want: map[gouuid.UUID]storeData{
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
				metrics: map[gouuid.UUID]storeData{
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
			want: map[gouuid.UUID]storeData{
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

func TestStoreReadPointsAndOffset(t *testing.T) {
	type fields struct {
		metrics map[gouuid.UUID]storeData
	}
	type args struct {
		uuids []gouuid.UUID
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		want       []types.MetricData
		wantOffset []int
		wantErr    bool
	}{
		{
			name: "store_empty",
			fields: fields{
				metrics: make(map[gouuid.UUID]storeData),
			},
			args: args{
				uuids: []gouuid.UUID{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				},
			},
			want:       make([]types.MetricData, 1),
			wantOffset: []int{0},
			wantErr:    false,
		},
		{
			name: "store_filled",
			fields: fields{
				metrics: map[gouuid.UUID]storeData{
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
						WriteOffset:         1,
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
						WriteOffset:         0,
						expirationTimestamp: 800,
					},
				},
			},
			args: args{
				uuids: []gouuid.UUID{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				},
			},
			want: []types.MetricData{
				{
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
			wantOffset: []int{1},
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
			}
			got, gotOffset, err := s.ReadPointsAndOffset(tt.args.uuids)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadPointsAndOffset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadPointsAndOffset() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(gotOffset, tt.wantOffset) {
				t.Errorf("ReadPointsAndOffset() gotOffset = %v, want %v", gotOffset, tt.wantOffset)
			}
		})
	}
}

func TestStoreGetSetPointsAndOffset(t *testing.T) {
	type fields struct {
		metrics map[gouuid.UUID]storeData
	}
	type args struct {
		points  []types.MetricData
		offsets []int
		now     time.Time
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      []types.MetricData
		wantState map[gouuid.UUID]storeData
	}{
		{
			name: "store_filled",
			fields: fields{
				metrics: map[gouuid.UUID]storeData{
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
				points: []types.MetricData{
					{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
				offsets: []int{1, 3},
				now:     time.Unix(400, 0),
			},
			wantState: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					expirationTimestamp: 400 + int64(defaultTTL.Seconds()),
					WriteOffset:         1,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
					expirationTimestamp: 400 + int64(defaultTTL.Seconds()),
					WriteOffset:         3,
				},
			},
			want: []types.MetricData{
				{
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
				{
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
			},
		},
		{
			name: "store_empty",
			fields: fields{
				metrics: make(map[gouuid.UUID]storeData),
			},
			args: args{
				points: []types.MetricData{
					{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
				offsets: []int{1, 0},
				now:     time.Unix(200, 0),
			},
			wantState: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
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
					expirationTimestamp: 200 + int64(defaultTTL.Seconds()),
					WriteOffset:         1,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
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
					expirationTimestamp: 200 + int64(defaultTTL.Seconds()),
					WriteOffset:         0,
				},
			},
			want: make([]types.MetricData, 2),
		},
		{
			name: "store_filled_metrics_empty",
			fields: fields{
				metrics: map[gouuid.UUID]storeData{
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
				points:  nil,
				offsets: nil,
				now:     time.Unix(200, 0),
			},
			wantState: map[gouuid.UUID]storeData{
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
			want: nil,
		},
		{
			name: "store_empty_metrics_empty",
			fields: fields{
				metrics: make(map[gouuid.UUID]storeData),
			},
			args: args{
				points:  nil,
				offsets: nil,
				now:     time.Unix(200, 0),
			},
			wantState: make(map[gouuid.UUID]storeData),
			want:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics:      tt.fields.metrics,
				knownMetrics: make(map[gouuid.UUID]interface{}),
			}
			got, err := s.getSetPointsAndOffset(tt.args.points, tt.args.offsets, tt.args.now)
			if err != nil {
				t.Errorf("getSetPointsAndOffset() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSetPointsAndOffset() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(s.metrics, tt.wantState) {
				t.Errorf("getSetPointsAndOffset() metrics = %v, want %v", s.metrics, tt.wantState)
			}
		})
	}
}

func TestStore_markToExpire(t *testing.T) {
	type args struct {
		uuids []gouuid.UUID
		ttl   time.Duration
		now   time.Time
	}
	tests := []struct {
		name      string
		state     map[gouuid.UUID]storeData
		args      args
		wantState map[gouuid.UUID]storeData
	}{
		{
			name: "simple",
			state: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					},
					WriteOffset:         1,
					expirationTimestamp: 900000,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					WriteOffset:         2,
					expirationTimestamp: 900000,
				},
			},
			args: args{
				uuids: []gouuid.UUID{uuidFromStringOrNil("00000000-0000-0000-0000-000000000002")},
				ttl:   30 * time.Second,
				now:   time.Unix(600, 0),
			},
			wantState: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					},
					WriteOffset:         1,
					expirationTimestamp: 900000,
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					MetricData: types.MetricData{
						UUID: uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					},
					WriteOffset:         2,
					expirationTimestamp: 600 + 30,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.state,
			}
			if err := s.markToExpire(tt.args.uuids, tt.args.ttl, tt.args.now); err != nil {
				t.Errorf("Store.markToExpire() error = %v", err)
			}
			if !reflect.DeepEqual(s.metrics, tt.wantState) {
				t.Errorf("Store.markToExpire() metrics = %v, want %v", s.metrics, tt.wantState)
			}
		})
	}
}

func TestStore_GetSetFlushDeadline(t *testing.T) {
	tests := []struct {
		name      string
		state     map[gouuid.UUID]storeData
		args      map[gouuid.UUID]time.Time
		want      map[gouuid.UUID]time.Time
		wantState map[gouuid.UUID]storeData
	}{
		{
			name: "simple",
			state: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					flushDeadline: time.Unix(42, 0),
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					flushDeadline: time.Unix(1337, 0),
				},
			},
			args: map[gouuid.UUID]time.Time{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): time.Unix(200, 0),
			},
			want: map[gouuid.UUID]time.Time{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): time.Unix(1337, 0),
			},
			wantState: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
					flushDeadline: time.Unix(42, 0),
				},
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					flushDeadline: time.Unix(200, 0),
				},
			},
		},
		{
			name:  "no-state",
			state: map[gouuid.UUID]storeData{},
			args: map[gouuid.UUID]time.Time{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): time.Unix(200, 0),
			},
			want: map[gouuid.UUID]time.Time{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): time.Time{},
			},
			wantState: map[gouuid.UUID]storeData{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
					flushDeadline: time.Unix(200, 0),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.state,
			}
			got, err := s.GetSetFlushDeadline(tt.args)
			if err != nil {
				t.Errorf("Store.GetSetFlushDeadline() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Store.GetSetFlushDeadline() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(s.metrics, tt.wantState) {
				t.Errorf("Store.GetSetFlushDeadline() flushDeadlines = %v, want %v", s.metrics, tt.wantState)
			}
		})
	}
}

func TestStore_GetTransfert(t *testing.T) {
	type fields struct {
		metrics          map[gouuid.UUID]storeData
		transfertMetrics []gouuid.UUID
	}
	tests := []struct {
		name      string
		fields    fields
		args      int
		want      map[gouuid.UUID]time.Time
		wantState []gouuid.UUID
	}{
		{
			name: "empty-nil",
			fields: fields{
				metrics:          nil,
				transfertMetrics: nil,
			},
			args:      50,
			want:      map[gouuid.UUID]time.Time{},
			wantState: nil,
		},
		{
			name: "empty",
			fields: fields{
				metrics:          nil,
				transfertMetrics: []gouuid.UUID{},
			},
			args:      50,
			want:      map[gouuid.UUID]time.Time{},
			wantState: []gouuid.UUID{},
		},
		{
			name: "less-than-requested",
			fields: fields{
				metrics: map[gouuid.UUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						flushDeadline: time.Unix(0, 0),
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						flushDeadline: time.Unix(42, 0),
					},
				},
				transfertMetrics: []gouuid.UUID{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000003"),
				},
			},
			args: 50,
			want: map[gouuid.UUID]time.Time{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): time.Unix(0, 0),
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): time.Unix(42, 0),
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000003"): time.Time{},
			},
			wantState: []gouuid.UUID{},
		},
		{
			name: "more-than-requested",
			fields: fields{
				metrics: map[gouuid.UUID]storeData{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): {
						flushDeadline: time.Unix(0, 0),
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): {
						flushDeadline: time.Unix(42, 0),
					},
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000003"): {
						flushDeadline: time.Unix(1337, 0),
					},
				},
				transfertMetrics: []gouuid.UUID{
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"),
					uuidFromStringOrNil("00000000-0000-0000-0000-000000000003"),
				},
			},
			args: 2,
			want: map[gouuid.UUID]time.Time{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): time.Unix(0, 0),
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000002"): time.Unix(42, 0),
			},
			wantState: []gouuid.UUID{
				uuidFromStringOrNil("00000000-0000-0000-0000-000000000003"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics:          tt.fields.metrics,
				transfertMetrics: tt.fields.transfertMetrics,
			}
			got, err := s.GetTransfert(tt.args)
			if err != nil {
				t.Errorf("Store.GetTransfert() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Store.GetTransfert() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(s.transfertMetrics, tt.wantState) {
				t.Errorf("Store.GetTransfert() = %v, want %v", s.transfertMetrics, tt.wantState)
			}
		})
	}
}

func TestStore_GetAllKnownMetrics(t *testing.T) {
	store := New()
	store.getSetPointsAndOffset(
		[]types.MetricData{
			{
				UUID:       uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"),
				TimeToLive: 42,
				Points: []types.MetricPoint{
					{Timestamp: 10},
				},
			},
		},
		[]int{0},
		time.Unix(10, 0),
	)

	want := map[gouuid.UUID]time.Time{
		uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): time.Time{},
	}

	got, _ := store.GetAllKnownMetrics()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetAllKnownMetrics() = %v, want %v", got, want)
	}

	store.GetSetFlushDeadline(map[gouuid.UUID]time.Time{
		uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): time.Unix(42, 42),
	})

	want = map[gouuid.UUID]time.Time{
		uuidFromStringOrNil("00000000-0000-0000-0000-000000000001"): time.Unix(42, 42),
	}

	got, _ = store.GetAllKnownMetrics()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetAllKnownMetrics() = %v, want %v", got, want)
	}

	store.markToExpire(
		[]gouuid.UUID{uuidFromStringOrNil("00000000-0000-0000-0000-000000000001")},
		time.Minute,
		time.Unix(10, 0),
	)

	want = map[gouuid.UUID]time.Time{}

	got, _ = store.GetAllKnownMetrics()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetAllKnownMetrics() = %v, want %v", got, want)
	}
}

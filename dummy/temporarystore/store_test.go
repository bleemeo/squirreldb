package temporarystore

import (
	"context"
	"reflect"
	"squirreldb/types"
	"testing"
	"time"
)

const (
	MetricIDTest1 = 1
	MetricIDTest2 = 2
	MetricIDTest3 = 3
)

func TestAppend(t *testing.T) {
	type fields struct {
		metrics map[types.MetricID]storeData
	}
	type args struct {
		points []types.MetricData
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      []int
		wantState map[types.MetricID]storeData
	}{
		{
			name: "store_filled",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						MetricData: types.MetricData{
							ID: MetricIDTest1,
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10000,
									Value:     20,
								},
							},
							TimeToLive: 150,
						},
					},
					MetricIDTest2: {
						MetricData: types.MetricData{
							ID: MetricIDTest2,
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20000,
									Value:     100,
								},
							},
							TimeToLive: 2400,
						},
					},
				},
			},
			args: args{
				points: []types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
							{
								Timestamp: 50000,
								Value:     60,
							},
							{
								Timestamp: 60000,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
							{
								Timestamp: 100000,
								Value:     300,
							},
							{
								Timestamp: 120000,
								Value:     350,
							},
						},
						TimeToLive: 1200,
					},
				},
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData: types.MetricData{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
							{
								Timestamp: 50000,
								Value:     60,
							},
							{
								Timestamp: 60000,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
				},
				MetricIDTest2: {
					MetricData: types.MetricData{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
							{
								Timestamp: 100000,
								Value:     300,
							},
							{
								Timestamp: 120000,
								Value:     350,
							},
						},
						TimeToLive: 2400,
					},
				},
			},
			want: []int{7, 7},
		},
		{
			name: "store_empty",
			fields: fields{
				metrics: make(map[types.MetricID]storeData),
			},
			args: args{
				points: []types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
				},
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData: types.MetricData{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
				},
				MetricIDTest2: {
					MetricData: types.MetricData{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
				},
			},
			want: []int{5, 5},
		},
		{
			name: "store_filled_metrics_empty",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						MetricData: types.MetricData{
							ID: MetricIDTest1,
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10000,
									Value:     20,
								},
							},
							TimeToLive: 150,
						},
						expirationTime: time.Unix(400, 0),
					},
					MetricIDTest2: {
						MetricData: types.MetricData{
							ID: MetricIDTest2,
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20000,
									Value:     100,
								},
							},
							TimeToLive: 2400,
						},
						expirationTime: time.Unix(400, 0),
					},
				},
			},
			args: args{
				points: nil,
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData: types.MetricData{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
						},
						TimeToLive: 150,
					},
					expirationTime: time.Unix(400, 0),
				},
				MetricIDTest2: {
					MetricData: types.MetricData{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
						},
						TimeToLive: 2400,
					},
					expirationTime: time.Unix(400, 0),
				},
			},
			want: nil,
		},
		{
			name: "store_empty_metrics_empty",
			fields: fields{
				metrics: make(map[types.MetricID]storeData),
			},
			args: args{
				points: nil,
			},
			wantState: make(map[types.MetricID]storeData),
			want:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.fields.metrics,
			}
			got, err := s.Append(context.Background(), tt.args.points)
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
		metrics map[types.MetricID]storeData
	}
	type args struct {
		now time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[types.MetricID]storeData
	}{
		{
			name: "no_expire",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						MetricData:     types.MetricData{},
						expirationTime: time.Unix(800, 0),
					},
					MetricIDTest2: {
						MetricData:     types.MetricData{},
						expirationTime: time.Unix(1600, 0),
					},
				},
			},
			args: args{
				now: time.Unix(600, 0),
			},
			want: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData:     types.MetricData{},
					expirationTime: time.Unix(800, 0),
				},
				MetricIDTest2: {
					MetricData:     types.MetricData{},
					expirationTime: time.Unix(1600, 0),
				},
			},
		},
		{
			name: "expire",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						MetricData:     types.MetricData{},
						expirationTime: time.Unix(800, 0),
					},
					MetricIDTest2: {
						MetricData:     types.MetricData{},
						expirationTime: time.Unix(1600, 0),
					},
				},
			},
			args: args{
				now: time.Unix(1200, 0),
			},
			want: map[types.MetricID]storeData{
				MetricIDTest2: {
					MetricData:     types.MetricData{},
					expirationTime: time.Unix(1600, 0),
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
		metrics map[types.MetricID]storeData
	}
	type args struct {
		ids []types.MetricID
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
				metrics: make(map[types.MetricID]storeData),
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
				},
			},
			want:       make([]types.MetricData, 1),
			wantOffset: []int{0},
			wantErr:    false,
		},
		{
			name: "store_filled",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10000,
									Value:     20,
								},
								{
									Timestamp: 20000,
									Value:     30,
								},
								{
									Timestamp: 30000,
									Value:     40,
								},
								{
									Timestamp: 40000,
									Value:     50,
								},
							},
							TimeToLive: 300,
						},
						WriteOffset:    1,
						expirationTime: time.Unix(800, 0),
					},
					MetricIDTest2: {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20000,
									Value:     100,
								},
								{
									Timestamp: 40000,
									Value:     150,
								},
								{
									Timestamp: 60000,
									Value:     200,
								},
								{
									Timestamp: 80000,
									Value:     250,
								},
							},
							TimeToLive: 1200,
						},
						WriteOffset:    0,
						expirationTime: time.Unix(800, 0),
					},
				},
			},
			args: args{
				ids: []types.MetricID{
					MetricIDTest1,
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
							Timestamp: 10000,
							Value:     20,
						},
						{
							Timestamp: 20000,
							Value:     30,
						},
						{
							Timestamp: 30000,
							Value:     40,
						},
						{
							Timestamp: 40000,
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
			got, gotOffset, err := s.ReadPointsAndOffset(context.Background(), tt.args.ids)
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
		metrics map[types.MetricID]storeData
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
		wantState map[types.MetricID]storeData
	}{
		{
			name: "store_filled",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10000,
									Value:     20,
								},
							},
							TimeToLive: 150,
						},
						expirationTime: time.Unix(800, 0),
					},
					MetricIDTest2: {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20000,
									Value:     100,
								},
							},
							TimeToLive: 2400,
						},
						expirationTime: time.Unix(800, 0),
					},
				},
			},
			args: args{
				points: []types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
							{
								Timestamp: 50000,
								Value:     60,
							},
							{
								Timestamp: 60000,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
							{
								Timestamp: 100000,
								Value:     300,
							},
							{
								Timestamp: 120000,
								Value:     350,
							},
						},
						TimeToLive: 1200,
					},
				},
				offsets: []int{1, 3},
				now:     time.Unix(400, 0),
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData: types.MetricData{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
							{
								Timestamp: 50000,
								Value:     60,
							},
							{
								Timestamp: 60000,
								Value:     70,
							},
						},
						TimeToLive: 300,
					},
					expirationTime: time.Unix(400, 0).Add(defaultTTL),
					WriteOffset:    1,
				},
				MetricIDTest2: {
					MetricData: types.MetricData{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
							{
								Timestamp: 100000,
								Value:     300,
							},
							{
								Timestamp: 120000,
								Value:     350,
							},
						},
						TimeToLive: 1200,
					},
					expirationTime: time.Unix(400, 0).Add(defaultTTL),
					WriteOffset:    3,
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
							Timestamp: 10000,
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
							Timestamp: 20000,
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
				metrics: make(map[types.MetricID]storeData),
			},
			args: args{
				points: []types.MetricData{
					{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
					{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
				},
				offsets: []int{1, 0},
				now:     time.Unix(200, 0),
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData: types.MetricData{
						ID: MetricIDTest1,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
							{
								Timestamp: 20000,
								Value:     30,
							},
							{
								Timestamp: 30000,
								Value:     40,
							},
							{
								Timestamp: 40000,
								Value:     50,
							},
						},
						TimeToLive: 300,
					},
					expirationTime: time.Unix(200, 0).Add(defaultTTL),
					WriteOffset:    1,
				},
				MetricIDTest2: {
					MetricData: types.MetricData{
						ID: MetricIDTest2,
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
							{
								Timestamp: 40000,
								Value:     150,
							},
							{
								Timestamp: 60000,
								Value:     200,
							},
							{
								Timestamp: 80000,
								Value:     250,
							},
						},
						TimeToLive: 1200,
					},
					expirationTime: time.Unix(200, 0).Add(defaultTTL),
					WriteOffset:    0,
				},
			},
			want: make([]types.MetricData, 2),
		},
		{
			name: "store_filled_metrics_empty",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     10,
								},
								{
									Timestamp: 10000,
									Value:     20,
								},
							},
							TimeToLive: 150,
						},
						expirationTime: time.Unix(400, 0),
					},
					MetricIDTest2: {
						MetricData: types.MetricData{
							Points: []types.MetricPoint{
								{
									Timestamp: 0,
									Value:     50,
								},
								{
									Timestamp: 20000,
									Value:     100,
								},
							},
							TimeToLive: 2400,
						},
						expirationTime: time.Unix(400, 0),
					},
				},
			},
			args: args{
				points:  nil,
				offsets: nil,
				now:     time.Unix(200, 0),
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     10,
							},
							{
								Timestamp: 10000,
								Value:     20,
							},
						},
						TimeToLive: 150,
					},
					expirationTime: time.Unix(400, 0),
				},
				MetricIDTest2: {
					MetricData: types.MetricData{
						Points: []types.MetricPoint{
							{
								Timestamp: 0,
								Value:     50,
							},
							{
								Timestamp: 20000,
								Value:     100,
							},
						},
						TimeToLive: 2400,
					},
					expirationTime: time.Unix(400, 0),
				},
			},
			want: nil,
		},
		{
			name: "store_empty_metrics_empty",
			fields: fields{
				metrics: make(map[types.MetricID]storeData),
			},
			args: args{
				points:  nil,
				offsets: nil,
				now:     time.Unix(200, 0),
			},
			wantState: make(map[types.MetricID]storeData),
			want:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics:      tt.fields.metrics,
				knownMetrics: make(map[types.MetricID]interface{}),
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
		ids []types.MetricID
		ttl time.Duration
		now time.Time
	}
	tests := []struct {
		name      string
		state     map[types.MetricID]storeData
		args      args
		wantState map[types.MetricID]storeData
	}{
		{
			name: "simple",
			state: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData: types.MetricData{
						ID: MetricIDTest1,
					},
					WriteOffset:    1,
					expirationTime: time.Unix(900000, 0),
				},
				MetricIDTest2: {
					MetricData: types.MetricData{
						ID: MetricIDTest2,
					},
					WriteOffset:    2,
					expirationTime: time.Unix(900000, 0),
				},
			},
			args: args{
				ids: []types.MetricID{MetricIDTest2},
				ttl: 30 * time.Second,
				now: time.Unix(600, 0),
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest1: {
					MetricData: types.MetricData{
						ID: MetricIDTest1,
					},
					WriteOffset:    1,
					expirationTime: time.Unix(900000, 0),
				},
				MetricIDTest2: {
					MetricData: types.MetricData{
						ID: MetricIDTest2,
					},
					WriteOffset:    2,
					expirationTime: time.Unix(600+30, 0),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics: tt.state,
			}
			if err := s.markToExpire(tt.args.ids, tt.args.ttl, tt.args.now); err != nil {
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
		state     map[types.MetricID]storeData
		args      map[types.MetricID]time.Time
		want      map[types.MetricID]time.Time
		wantState map[types.MetricID]storeData
	}{
		{
			name: "simple",
			state: map[types.MetricID]storeData{
				MetricIDTest1: {
					flushDeadline: time.Unix(42, 0),
				},
				MetricIDTest2: {
					flushDeadline: time.Unix(1337, 0),
				},
			},
			args: map[types.MetricID]time.Time{
				MetricIDTest2: time.Unix(200, 0),
			},
			want: map[types.MetricID]time.Time{
				MetricIDTest2: time.Unix(1337, 0),
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest1: {
					flushDeadline: time.Unix(42, 0),
				},
				MetricIDTest2: {
					flushDeadline: time.Unix(200, 0),
				},
			},
		},
		{
			name:  "no-state",
			state: map[types.MetricID]storeData{},
			args: map[types.MetricID]time.Time{
				MetricIDTest2: time.Unix(200, 0),
			},
			want: map[types.MetricID]time.Time{
				MetricIDTest2: time.Time{},
			},
			wantState: map[types.MetricID]storeData{
				MetricIDTest2: {
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
			got, err := s.GetSetFlushDeadline(context.Background(), tt.args)
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
		metrics          map[types.MetricID]storeData
		transfertMetrics []types.MetricID
	}
	tests := []struct {
		name      string
		fields    fields
		args      int
		want      map[types.MetricID]time.Time
		wantState []types.MetricID
	}{
		{
			name: "empty-nil",
			fields: fields{
				metrics:          nil,
				transfertMetrics: nil,
			},
			args:      50,
			want:      map[types.MetricID]time.Time{},
			wantState: nil,
		},
		{
			name: "empty",
			fields: fields{
				metrics:          nil,
				transfertMetrics: []types.MetricID{},
			},
			args:      50,
			want:      map[types.MetricID]time.Time{},
			wantState: []types.MetricID{},
		},
		{
			name: "less-than-requested",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						flushDeadline: time.Unix(0, 0),
					},
					MetricIDTest2: {
						flushDeadline: time.Unix(42, 0),
					},
				},
				transfertMetrics: []types.MetricID{
					MetricIDTest1,
					MetricIDTest2,
					MetricIDTest3,
				},
			},
			args: 50,
			want: map[types.MetricID]time.Time{
				MetricIDTest1: time.Unix(0, 0),
				MetricIDTest2: time.Unix(42, 0),
				MetricIDTest3: time.Time{},
			},
			wantState: []types.MetricID{},
		},
		{
			name: "more-than-requested",
			fields: fields{
				metrics: map[types.MetricID]storeData{
					MetricIDTest1: {
						flushDeadline: time.Unix(0, 0),
					},
					MetricIDTest2: {
						flushDeadline: time.Unix(42, 0),
					},
					MetricIDTest3: {
						flushDeadline: time.Unix(1337, 0),
					},
				},
				transfertMetrics: []types.MetricID{
					MetricIDTest1,
					MetricIDTest2,
					MetricIDTest3,
				},
			},
			args: 2,
			want: map[types.MetricID]time.Time{
				MetricIDTest1: time.Unix(0, 0),
				MetricIDTest2: time.Unix(42, 0),
			},
			wantState: []types.MetricID{
				MetricIDTest3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Store{
				metrics:          tt.fields.metrics,
				transfertMetrics: tt.fields.transfertMetrics,
			}
			got, err := s.GetTransfert(context.Background(), tt.args)
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
				ID:         MetricIDTest1,
				TimeToLive: 42,
				Points: []types.MetricPoint{
					{Timestamp: 10000},
				},
			},
		},
		[]int{0},
		time.Unix(10, 0),
	)

	want := map[types.MetricID]time.Time{
		MetricIDTest1: time.Time{},
	}

	got, _ := store.GetAllKnownMetrics(context.Background())
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetAllKnownMetrics() = %v, want %v", got, want)
	}

	store.GetSetFlushDeadline(context.Background(), map[types.MetricID]time.Time{
		MetricIDTest1: time.Unix(42, 42),
	})

	want = map[types.MetricID]time.Time{
		MetricIDTest1: time.Unix(42, 42),
	}

	got, _ = store.GetAllKnownMetrics(context.Background())
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetAllKnownMetrics() = %v, want %v", got, want)
	}

	store.markToExpire(
		[]types.MetricID{MetricIDTest1},
		time.Minute,
		time.Unix(10, 0),
	)

	want = map[types.MetricID]time.Time{}

	got, _ = store.GetAllKnownMetrics(context.Background())
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetAllKnownMetrics() = %v, want %v", got, want)
	}
}

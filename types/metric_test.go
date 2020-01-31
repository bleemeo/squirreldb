package types

import (
	"math/rand"
	"reflect"
	"testing"

	gouuid "github.com/gofrs/uuid"
)

func TestMetricUUID_Uint64(t *testing.T) {
	type fields struct {
		UUID gouuid.UUID
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		{
			name: "uuid_0",
			fields: fields{
				UUID: gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
			},
			want: 1,
		},
		{
			name: "uuid_10",
			fields: fields{
				UUID: gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UintFromUUID(tt.fields.UUID); got != tt.want {
				t.Errorf("Uint64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeduplicatePoints(t *testing.T) {
	type args struct {
		points []MetricPoint
	}
	tests := []struct {
		name string
		args args
		want []MetricPoint
	}{
		{
			name: "no_duplicated_sorted",
			args: args{
				points: []MetricPoint{
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
				},
			},
			want: []MetricPoint{
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
			},
		},
		{
			name: "no_duplicated_no_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 20000,
						Value:     30,
					},
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
				},
			},
			want: []MetricPoint{
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
			},
		},
		{
			name: "duplicated_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 10000,
						Value:     20,
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
						Timestamp: 20000,
						Value:     30,
					},
				},
			},
			want: []MetricPoint{
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
			},
		},
		{
			name: "duplicated_no_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 20000,
						Value:     30,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 20000,
						Value:     30,
					},
				},
			},
			want: []MetricPoint{
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
			},
		},
		{
			name: "points_empty",
			args: args{
				points: []MetricPoint{},
			},
			want: []MetricPoint{},
		},
		{
			name: "points_nil",
			args: args{
				points: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DeduplicatePoints(tt.args.points); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DeduplicatePoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSortPoints(t *testing.T) {
	type args struct {
		points []MetricPoint
	}
	tests := []struct {
		name string
		args args
		want []MetricPoint
	}{
		{
			name: "sorted",
			args: args{
				points: []MetricPoint{
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
			},
			want: []MetricPoint{
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
		},
		{
			name: "unsorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 20000,
						Value:     30,
					},
					{
						Timestamp: 40000,
						Value:     50,
					},
					{
						Timestamp: 10000,
						Value:     20,
					},
					{
						Timestamp: 30000,
						Value:     40,
					},
					{
						Timestamp: 0,
						Value:     10,
					},
				},
			},
			want: []MetricPoint{
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
		},
		{
			name: "points_empty",
			args: args{
				points: []MetricPoint{},
			},
			want: []MetricPoint{},
		},
		{
			name: "points_nil",
			args: args{
				points: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		var result []MetricPoint
		if tt.args.points != nil {
			result = make([]MetricPoint, len(tt.args.points))
			copy(result, tt.args.points)
		}
		t.Run(tt.name, func(t *testing.T) {
			sortPoints(result)
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("sortPoints() = %v, want %v", result, tt.want)
			}
		})
	}
}

func BenchmarkDeduplicatePoints(b *testing.B) {
	rand.Seed(42)
	tests := []struct {
		name   string
		points []MetricPoint
	}{
		{
			name:   "no_duplicated_sorted_30",
			points: MakePointsForTest(30),
		},
		{
			name:   "no_duplicated_sorted_1000",
			points: MakePointsForTest(1000),
		},
		{
			name:   "no_duplicated_sorted_10000",
			points: MakePointsForTest(10000),
		},
		{
			name:   "duplicated_sorted_1100",
			points: AddDuplicateForTest(MakePointsForTest(1000), 100),
		},
		{
			name:   "duplicated_1100",
			points: ShuffleForTest(AddDuplicateForTest(MakePointsForTest(1000), 100)),
		},
		{
			name:   "two_duplicated_block_2000",
			points: append(MakePointsForTest(1000), MakePointsForTest(1000)...),
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = DeduplicatePoints(tt.points)
			}
		})
	}
}

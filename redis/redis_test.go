package redis

import (
	"reflect"
	"squirreldb/types"
	"testing"

	gouuid "github.com/gofrs/uuid"
)

func Benchmark_valuesFromData(b *testing.B) {
	tests := []struct {
		name string
		data types.MetricData
	}{
		{
			name: "small_5",
			data: types.MetricData{
				Points: []types.MetricPoint{
					{Timestamp: 20, Value: 20},
					{Timestamp: 30, Value: 30},
					{Timestamp: 40, Value: 40},
					{Timestamp: 50, Value: 50},
					{Timestamp: 60, Value: 60},
				},
				TimeToLive: 360,
			},
		},
		{
			name: "medium_30",
			data: types.MetricData{
				Points:     types.MakePointsForTest(30),
				TimeToLive: 360,
			},
		},
		{
			name: "large_300",
			data: types.MetricData{
				Points:     types.MakePointsForTest(300),
				TimeToLive: 360,
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, _ = valuesFromData(tt.data)
			}
		})
	}
}

func Benchmark_dataFromValues(b *testing.B) {
	tests := []struct {
		name string
		data types.MetricData
	}{
		{
			name: "small_5",
			data: types.MetricData{
				UUID: gouuid.UUID{},
				Points: []types.MetricPoint{
					{Timestamp: 20, Value: 20},
					{Timestamp: 30, Value: 30},
					{Timestamp: 40, Value: 40},
					{Timestamp: 50, Value: 50},
					{Timestamp: 60, Value: 60},
				},
				TimeToLive: 360,
			},
		},
		{
			name: "medium_30",
			data: types.MetricData{
				UUID:       gouuid.UUID{},
				Points:     types.MakePointsForTest(30),
				TimeToLive: 360,
			},
		},
		{
			name: "large_300",
			data: types.MetricData{
				UUID:       gouuid.UUID{},
				Points:     types.MakePointsForTest(300),
				TimeToLive: 360,
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			dataBytes, _ := valuesFromData(tt.data)
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_, _ = dataFromValues(gouuid.UUID{}, dataBytes)
			}
		})
	}
}

func Test_valuesSerialization(t *testing.T) {
	tests := []struct {
		name string
		data types.MetricData
	}{
		{
			name: "small_5",
			data: types.MetricData{
				UUID: gouuid.UUID{},
				Points: []types.MetricPoint{
					{Timestamp: 20, Value: 20},
					{Timestamp: 30, Value: 30},
					{Timestamp: 40, Value: 40},
					{Timestamp: 50, Value: 50},
					{Timestamp: 60, Value: 60},
				},
				TimeToLive: 360,
			},
		},
		{
			name: "medium_30",
			data: types.MetricData{
				UUID:       gouuid.UUID{},
				Points:     types.MakePointsForTest(30),
				TimeToLive: 360,
			},
		},
		{
			name: "large_300",
			data: types.MetricData{
				UUID:       gouuid.UUID{},
				Points:     types.MakePointsForTest(300),
				TimeToLive: 360,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBytes, err := valuesFromData(tt.data)
			if err != nil {
				t.Errorf("valuesFromData() error = %v", err)
				return
			}
			got, err := dataFromValues(gouuid.UUID{}, gotBytes)
			if err != nil {
				t.Errorf("dataFromValues() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.data) {
				t.Errorf("dataFromValues(valuesFromData()) = %v, want %v", got, tt.data)
			}
		})
	}
}

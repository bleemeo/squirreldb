package temporarystore

import (
	"bytes"
	"reflect"
	"squirreldb/types"
	"testing"
)

const (
	MetricIDTest1 = 1
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
					{Timestamp: 20000, Value: 20},
					{Timestamp: 30000, Value: 30},
					{Timestamp: 40000, Value: 40},
					{Timestamp: 50000, Value: 50},
					{Timestamp: 60000, Value: 60},
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
				_, _ = valuesFromData(tt.data, nil, nil)
			}
		})
		b.Run(tt.name+"-reuse", func(b *testing.B) {
			dataSerialized := make([]serializedPoints, 1024)
			buffer := new(bytes.Buffer)

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				_, _ = valuesFromData(tt.data, buffer, dataSerialized)
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
				ID: MetricIDTest1,
				Points: []types.MetricPoint{
					{Timestamp: 20000, Value: 20},
					{Timestamp: 30000, Value: 30},
					{Timestamp: 40000, Value: 40},
					{Timestamp: 50000, Value: 50},
					{Timestamp: 60000, Value: 60},
				},
				TimeToLive: 360,
			},
		},
		{
			name: "medium_30",
			data: types.MetricData{
				ID:         MetricIDTest1,
				Points:     types.MakePointsForTest(30),
				TimeToLive: 360,
			},
		},
		{
			name: "large_300",
			data: types.MetricData{
				ID:         MetricIDTest1,
				Points:     types.MakePointsForTest(300),
				TimeToLive: 360,
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			dataBytes, _ := valuesFromData(tt.data, nil, nil)

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				_, _ = dataFromValues(MetricIDTest1, dataBytes, nil)
			}
		})
		b.Run(tt.name+"-reuse", func(b *testing.B) {
			dataBytes, _ := valuesFromData(tt.data, nil, nil)
			dataSerialized := make([]serializedPoints, 1024)

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				_, _ = dataFromValues(MetricIDTest1, dataBytes, dataSerialized)
			}
		})
	}
}

func Test_valuesSerialization(t *testing.T) {
	r := &Redis{}
	r.initPool()

	tests := []struct {
		name string
		data types.MetricData
	}{
		{
			name: "small_5",
			data: types.MetricData{
				ID: MetricIDTest1,
				Points: []types.MetricPoint{
					{Timestamp: 20000, Value: 20},
					{Timestamp: 30000, Value: 30},
					{Timestamp: 40000, Value: 40},
					{Timestamp: 50000, Value: 50},
					{Timestamp: 60000, Value: 60},
				},
				TimeToLive: 360,
			},
		},
		{
			name: "big_timestamp",
			data: types.MetricData{
				ID: MetricIDTest1,
				Points: []types.MetricPoint{
					{Timestamp: 20, Value: 0},
					{Timestamp: 1600787944491, Value: -0},
					{Timestamp: 16007879444910, Value: -1e9},
					{Timestamp: 160078794449100, Value: 1e9},
					{Timestamp: 1600787944491000, Value: 1e25},
				},
				TimeToLive: 86400 * 9000,
			},
		},
		{
			name: "medium_30",
			data: types.MetricData{
				ID:         MetricIDTest1,
				Points:     types.MakePointsForTest(30),
				TimeToLive: 360,
			},
		},
		{
			name: "large_300",
			data: types.MetricData{
				ID:         MetricIDTest1,
				Points:     types.MakePointsForTest(300),
				TimeToLive: 360,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBytes, err := valuesFromData(tt.data, nil, nil)
			if err != nil {
				t.Errorf("valuesFromData() error = %v", err)

				return
			}

			got, err := dataFromValues(MetricIDTest1, gotBytes, nil)
			if err != nil {
				t.Errorf("dataFromValues() error = %v", err)

				return
			}

			if !reflect.DeepEqual(got, tt.data) {
				t.Errorf("dataFromValues(valuesFromData()) = %v, want %v", got, tt.data)
			}
		})
		t.Run(tt.name+"reuse", func(t *testing.T) {
			buffer := r.getBuffer()
			tmp := r.getSerializedPoints()
			tmp2 := r.getSerializedPoints()

			for i := 0; i < 6; i++ {
				gotBytes, err := valuesFromData(tt.data, buffer, tmp)
				if err != nil {
					t.Errorf("valuesFromData() error = %v", err)

					return
				}

				got, err := dataFromValues(MetricIDTest1, gotBytes, tmp2)
				if err != nil {
					t.Errorf("dataFromValues() error = %v", err)

					return
				}

				if !reflect.DeepEqual(got, tt.data) {
					t.Errorf("dataFromValues(valuesFromData()) = %v, want %v", got, tt.data)
				}

				if i == 3 {
					r.bufferPool.Put(buffer)
					r.serializedPointsPool.Put(&tmp)
					r.serializedPointsPool.Put(&tmp2)

					buffer = r.getBuffer()
					tmp = r.getSerializedPoints()
					tmp2 = r.getSerializedPoints()
				}
			}
		})
	}
}

package wal

import (
	"math/rand"
	"reflect"
	"sort"
	"squirreldb/types"
	"testing"
)

type testCase struct {
	name string
	data [][]types.MetricData
}

func generateData() []testCase {
	rnd := rand.New(rand.NewSource(42))
	hundersMetricsList := make([][]types.MetricData, 30)

	for n := range hundersMetricsList {
		hundersMetricsList[n] = make([]types.MetricData, 100)
		for i := range hundersMetricsList[n] {
			hundersMetricsList[n][i] = types.MetricData{
				ID:         types.MetricID(50000 + n*1000 + i),
				TimeToLive: 86400,
				Points: []types.MetricPoint{
					{Timestamp: 1590613630, Value: rnd.Float64()},
				},
			}
		}
	}

	return []testCase{
		{
			name: "simple",
			data: [][]types.MetricData{
				{
					{
						ID:         42,
						Points:     []types.MetricPoint{{Timestamp: 42, Value: 42.0}},
						TimeToLive: 3600,
					},
				},
			},
		},
		{
			name: "two",
			data: [][]types.MetricData{
				{
					{
						ID:         43,
						Points:     []types.MetricPoint{{Timestamp: 42, Value: 42.0}},
						TimeToLive: 3600,
					},
				},
				{
					{
						ID:         12,
						Points:     []types.MetricPoint{{Timestamp: 34, Value: 5.6}},
						TimeToLive: 789,
					},
				},
			},
		},
		{
			name: "lotOfPoints",
			data: [][]types.MetricData{
				{
					{
						ID:         1234,
						Points:     types.MakePointsForTest(10000),
						TimeToLive: 3600,
					},
				},
			},
		},
		{
			name: "multipleBatch",
			data: hundersMetricsList,
		},
	}
}

func Test_encoder(t *testing.T) {
	for _, tt := range generateData() {
		ok := t.Run(tt.name, func(t *testing.T) {
			var want []types.MetricData

			e := encoder{}
			e.Reset()

			for _, d := range tt.data {
				if err := e.Encode(d); err != nil {
					t.Errorf("encoder.Encode() error = %v", err)
				}

				want = append(want, d...)
			}

			got, err := decode(e.Bytes())
			if err != nil {
				t.Errorf("decode error = %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("decode() = %v, want %v", got, want)
				return
			}

			// Redo to ensure reset works
			e.Reset()

			for _, d := range tt.data {
				if err := e.Encode(d); err != nil {
					t.Errorf("encoder.Encode() error = %v", err)
				}
			}

			got, err = decode(e.Bytes())
			if err != nil {
				t.Errorf("decode error = %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("decode() = %v, want %v", got, want)
				return
			}
		})
		if !ok {
			break
		}
	}
}

func Benchmark_encoder(b *testing.B) {
	for _, tt := range generateData() {
		b.Run(tt.name, func(b *testing.B) {
			e := encoder{}
			e.Reset()

			for n := 0; n < b.N; n++ {
				for _, d := range tt.data {
					if err := e.Encode(d); err != nil {
						b.Errorf("encoder.Encode() error = %v", err)
					}

					if e.buffer.Len() >= flushSizeBytes {
						e.Reset()
					}
				}
			}
		})

		e := encoder{}
		e.Reset()

		for _, d := range tt.data {
			if err := e.Encode(d); err != nil {
				b.Errorf("encoder.Encode() error = %v", err)
			}
		}

		got := e.Bytes()

		b.Run(tt.name+"_decode", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err := decode(got)
				if err != nil {
					b.Errorf("decode error = %v", err)
				}
			}
		})
	}
}

func Test_pool(t *testing.T) {
	for _, tt := range generateData() {
		ok := t.Run(tt.name, func(t *testing.T) {
			var want []types.MetricData

			e := pool{}
			e.Init(4)

			for _, d := range tt.data {
				if err := e.Encode(d); err != nil {
					t.Errorf("encoder.Encode() error = %v", err)
				}

				want = append(want, d...)
			}

			got, err := decode(e.BytesAndReset())
			if err != nil {
				t.Errorf("decode error = %v", err)
			}

			// the pool no longer guarantee the order... sort want & got
			sort.Slice(want, func(i, j int) bool {
				return want[i].ID < want[j].ID
			})

			sort.Slice(got, func(i, j int) bool {
				return got[i].ID < got[j].ID
			})

			if !reflect.DeepEqual(got, want) {
				t.Errorf("decode() = %v, want %v", got, want)
				return
			}

			for _, d := range tt.data {
				if err := e.Encode(d); err != nil {
					t.Errorf("encoder.Encode() error = %v", err)
				}
			}

			got, err = decode(e.BytesAndReset())
			if err != nil {
				t.Errorf("decode error = %v", err)
			}

			sort.Slice(got, func(i, j int) bool {
				return got[i].ID < got[j].ID
			})

			if !reflect.DeepEqual(got, want) {
				t.Errorf("decode() = %v, want %v", got, want)
				return
			}

			buffer := e.BytesAndReset()
			if len(buffer) > 0 {
				t.Errorf("BytesAndReset() = %v, want []", buffer)
				return
			}
		})
		if !ok {
			break
		}
	}
}

func Benchmark_pool(b *testing.B) {
	for _, tt := range generateData() {
		b.Run(tt.name, func(b *testing.B) {
			e := pool{}
			e.Init(4)

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for _, d := range tt.data {
						if newSize, addedBytes, err := e.EncodeEx(d); err != nil {
							b.Errorf("encoder.Encode() error = %v", err)
						} else if newSize >= flushSizeBytes && newSize-addedBytes < flushSizeBytes {
							size := len(e.BytesAndReset())
							if size < flushSizeBytes {
								b.Errorf("len(e.BytesAndReset()) = %d want > %d", size, flushSizeBytes)
							}
						}
					}
				}
			})
		})
	}
}

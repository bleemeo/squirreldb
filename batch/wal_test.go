package batch

import (
	"context"
	"reflect"
	"squirreldb/dummy"
	"squirreldb/types"
	"sync"
	"testing"
	"time"
)

func TestWalBatcher(t *testing.T) {

	nowTS := time.Now().Unix() * 1000

	tests := []struct {
		name        string
		walInitial  []types.MetricData
		walOther    []types.MetricData
		writes      [][]types.MetricData
		wantInStore []types.MetricData
		flushOnStop bool
	}{
		{
			name:       "simple",
			walInitial: []types.MetricData{},
			walOther:   []types.MetricData{},
			writes: [][]types.MetricData{
				{
					{
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42}},
					},
					{
						ID:         MetricIDTest2,
						TimeToLive: 3600,
						Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42.1}},
					},
				},
			},
			wantInStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42}},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 3600,
					Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42.1}},
				},
			},
		},
		{
			name:       "otherWrite",
			walInitial: []types.MetricData{},
			walOther: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: nowTS - 10, Value: 42}},
				},
				{
					ID:         MetricIDTest3,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 1337}},
				},
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: nowTS + 10, Value: 888}},
				},
			},
			writes: [][]types.MetricData{
				{
					{
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42}},
					},
					{
						ID:         MetricIDTest2,
						TimeToLive: 3600,
						Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42.1}},
					},
				},
			},
			wantInStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points: []types.MetricPoint{
						{Timestamp: nowTS - 10, Value: 42},
						{Timestamp: nowTS, Value: 42},
						{Timestamp: nowTS + 10, Value: 888},
					},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 3600,
					Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42.1}},
				},
				{
					ID:         MetricIDTest3,
					TimeToLive: 1234,
					Points: []types.MetricPoint{
						{Timestamp: nowTS, Value: 1337},
					},
				},
			},
		},
		{
			name: "previousWal",
			walInitial: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: nowTS - 10, Value: 3}},
				},
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points: []types.MetricPoint{
						{Timestamp: nowTS - 30, Value: 1},
						{Timestamp: nowTS - 20, Value: 2},
					},
				},
			},
			walOther: []types.MetricData{},
			writes: [][]types.MetricData{
				{
					{
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 4}},
					},
				},
			},
			wantInStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points: []types.MetricPoint{
						{Timestamp: nowTS - 30, Value: 1},
						{Timestamp: nowTS - 20, Value: 2},
						{Timestamp: nowTS - 10, Value: 3},
						{Timestamp: nowTS, Value: 4},
					},
				},
			},
		},
		{
			name: "dedup",
			walInitial: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: nowTS - 10, Value: 3}},
				},
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points: []types.MetricPoint{
						{Timestamp: nowTS - 30, Value: 1},
						{Timestamp: nowTS - 10, Value: 3},
						{Timestamp: nowTS - 20, Value: 2},
					},
				},
			},
			walOther: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: nowTS - 10, Value: 3}},
				},
			},
			writes: [][]types.MetricData{
				{
					{
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: nowTS - 10, Value: 3}},
					},
					{
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: nowTS - 10, Value: 3}},
					},
				},
			},
			wantInStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points: []types.MetricPoint{
						{Timestamp: nowTS - 30, Value: 1},
						{Timestamp: nowTS - 20, Value: 2},
						{Timestamp: nowTS - 10, Value: 3},
					},
				},
			},
		},
		{
			name:       "persistOnStop",
			walInitial: []types.MetricData{},
			walOther:   []types.MetricData{},
			writes: [][]types.MetricData{
				{
					{
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42}},
					},
					{
						ID:         MetricIDTest2,
						TimeToLive: 3600,
						Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42.1}},
					},
				},
			},
			wantInStore: []types.MetricData{
				{
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42}},
				},
				{
					ID:         MetricIDTest2,
					TimeToLive: 3600,
					Points:     []types.MetricPoint{{Timestamp: nowTS, Value: 42.1}},
				},
			},
			flushOnStop: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tsdb := &dummy.MemoryTSDB{}
			batch := WalBatcher{
				WalStore: &dummy.Wal{
					MockReadOther: tt.walOther,
				},
				PersitentStore:          tsdb,
				WriteToPersistentOnStop: tt.flushOnStop,
			}
			batch.WalStore.Write(tt.walInitial)

			var wg sync.WaitGroup

			ctx, cancel := context.WithCancel(context.Background())
			readiness := make(chan error)

			wg.Add(1)

			go func() {
				defer wg.Done()
				batch.Run(ctx, readiness)
			}()

			err := <-readiness
			if err != nil {
				t.Errorf("batch.Run() failed: %v", err)
				cancel()
				wg.Wait()
				return
			}

			for _, w := range tt.writes {
				err := batch.Write(w)
				if err != nil {
					t.Errorf("batch.Write() failed: %v", err)
					return
				}
			}

			if tt.flushOnStop {
				cancel()
				wg.Wait()
			} else {
				batch.Flush()
			}

			if len(tsdb.Data) != len(tt.wantInStore) {
				t.Errorf("len(tsdb.Data) = %d, want %d (tsdb.Data=%v)", len(tsdb.Data), len(tt.wantInStore), tsdb.Data)
			} else {
				for _, want := range tt.wantInStore {
					got := tsdb.Data[want.ID]
					if !reflect.DeepEqual(got, want) {
						t.Errorf("tsdb.Data[%d] = %v, want %v", want.ID, got, want)
					}
				}
			}

			if !tt.flushOnStop {
				cancel()
				wg.Wait()
			}
		})
	}

}

func Test_merge(t *testing.T) {
	type args struct {
		data    map[types.MetricID]types.MetricData
		metrics []types.MetricData
	}
	tests := []struct {
		name string
		args args
		want map[types.MetricID]types.MetricData
	}{
		{
			name: "add-to-empty",
			args: args{
				data: nil,
				metrics: []types.MetricData{
					{
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: 999, Value: 42}},
					},
				},
			},
			want: map[types.MetricID]types.MetricData{
				MetricIDTest1: {
					ID:         MetricIDTest1,
					TimeToLive: 1234,
					Points:     []types.MetricPoint{{Timestamp: 999, Value: 42}},
				},
			},
		},
		{
			name: "add",
			args: args{
				data: map[types.MetricID]types.MetricData{
					MetricIDTest1: {
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: 999, Value: 42}},
					},
				},
				metrics: []types.MetricData{
					{
						ID:         MetricIDTest1,
						TimeToLive: 99999,
						Points:     []types.MetricPoint{{Timestamp: 1000, Value: 12}},
					},
				},
			},
			want: map[types.MetricID]types.MetricData{
				MetricIDTest1: {
					ID:         MetricIDTest1,
					TimeToLive: 99999,
					Points: []types.MetricPoint{
						{Timestamp: 999, Value: 42},
						{Timestamp: 1000, Value: 12},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _, _ := merge(tt.args.data, tt.args.metrics); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_merge(b *testing.B) {
	type args struct {
		data    []types.MetricData
		metrics []types.MetricData
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "add-to-empty",
			args: args{
				data: nil,
				metrics: []types.MetricData{
					{
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: 999, Value: 42}},
					},
				},
			},
		},
		{
			name: "add",
			args: args{
				data: []types.MetricData{
					MetricIDTest1: {
						ID:         MetricIDTest1,
						TimeToLive: 1234,
						Points:     []types.MetricPoint{{Timestamp: 999, Value: 42}},
					},
				},
				metrics: []types.MetricData{
					{
						ID:         MetricIDTest1,
						TimeToLive: 99999,
						Points:     []types.MetricPoint{{Timestamp: 1000, Value: 12}},
					},
				},
			},
		},
		{
			name: "add-lots-metrics",
			args: args{
				data:    types.MakeMetricDataForTest(50, 1, 0),
				metrics: types.MakeMetricDataForTest(100, 1, 10000),
			},
		},
		{
			name: "add-lots-points",
			args: args{
				data:    types.MakeMetricDataForTest(10, 10, 0),
				metrics: types.MakeMetricDataForTest(10, 100, 10000*10),
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				var data map[types.MetricID]types.MetricData

				if tt.args.data != nil {
					data, _, _ = merge(data, tt.args.data)
				}
				data, _, _ = merge(data, tt.args.metrics)
			}
		})
	}
}

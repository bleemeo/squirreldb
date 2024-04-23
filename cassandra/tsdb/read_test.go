package tsdb

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func Test_filterPoints(t *testing.T) {
	type args struct {
		points        []types.MetricPoint
		fromTimestamp int64
		toTimestamp   int64
	}

	tests := []struct {
		name string
		want []types.MetricPoint
		args args
	}{
		{
			name: "exact-fit",
			args: args{
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
				},
				fromTimestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6,
				toTimestamp:   time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6,
			},
			want: []types.MetricPoint{
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
			},
		},
		{
			name: "more-data-before",
			args: args{
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 2, 4, 15, 47, 41, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 47, 51, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
				},
				fromTimestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6,
				toTimestamp:   time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6,
			},
			want: []types.MetricPoint{
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
			},
		},
		{
			name: "more-data-after",
			args: args{
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 41, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 51, 0, time.UTC).UnixNano() / 1e6},
				},
				fromTimestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6,
				toTimestamp:   time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6,
			},
			want: []types.MetricPoint{
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
			},
		},
		{
			name: "more-data-both-end",
			args: args{
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 2, 4, 15, 47, 41, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 47, 51, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 41, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 51, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 49, 1, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 49, 11, 0, time.UTC).UnixNano() / 1e6},
				},
				fromTimestamp: time.Date(2020, 2, 4, 15, 48, 0, 0, time.UTC).UnixNano() / 1e6,
				toTimestamp:   time.Date(2020, 2, 4, 15, 49, 0, 0, time.UTC).UnixNano() / 1e6,
			},
			want: []types.MetricPoint{
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 41, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 51, 0, time.UTC).UnixNano() / 1e6},
			},
		},
		{
			name: "less-data-before",
			args: args{
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 41, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 51, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 49, 1, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 49, 11, 0, time.UTC).UnixNano() / 1e6},
				},
				fromTimestamp: time.Date(2020, 2, 4, 15, 48, 0, 0, time.UTC).UnixNano() / 1e6,
				toTimestamp:   time.Date(2020, 2, 4, 15, 49, 0, 0, time.UTC).UnixNano() / 1e6,
			},
			want: []types.MetricPoint{
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 41, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 51, 0, time.UTC).UnixNano() / 1e6},
			},
		},
		{
			name: "less-data-after",
			args: args{
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
				},
				fromTimestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6,
				toTimestamp:   time.Date(2020, 2, 4, 15, 49, 0, 0, time.UTC).UnixNano() / 1e6,
			},
			want: []types.MetricPoint{
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 1, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 11, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 21, 0, time.UTC).UnixNano() / 1e6},
			},
		},
		{
			name: "less-data-both-end",
			args: args{
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 41, 0, time.UTC).UnixNano() / 1e6},
				},
				fromTimestamp: time.Date(2020, 2, 4, 15, 48, 0, 0, time.UTC).UnixNano() / 1e6,
				toTimestamp:   time.Date(2020, 2, 4, 15, 49, 0, 0, time.UTC).UnixNano() / 1e6,
			},
			want: []types.MetricPoint{
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
				{Timestamp: time.Date(2020, 2, 4, 15, 48, 41, 0, time.UTC).UnixNano() / 1e6},
			},
		},
		{
			name: "no-match",
			args: args{
				points: []types.MetricPoint{
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 31, 0, time.UTC).UnixNano() / 1e6},
					{Timestamp: time.Date(2020, 2, 4, 15, 48, 41, 0, time.UTC).UnixNano() / 1e6},
				},
				fromTimestamp: time.Date(2020, 2, 4, 15, 49, 0, 0, time.UTC).UnixNano() / 1e6,
				toTimestamp:   time.Date(2020, 2, 4, 15, 50, 0, 0, time.UTC).UnixNano() / 1e6,
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterPoints(tt.args.points, tt.args.fromTimestamp, tt.args.toTimestamp)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterPoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mergePoints(t *testing.T) {
	type args struct {
		dst []types.MetricPoint
		src []types.MetricPoint
	}

	tests := []struct {
		name string
		args args
		want []types.MetricPoint
	}{
		{
			name: "nil dst",
			args: args{
				dst: nil,
				src: []types.MetricPoint{
					{Timestamp: 1234, Value: 42.0},
					{Timestamp: 1235, Value: 43.0},
					{Timestamp: 1236, Value: 44.0},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1236, Value: 44.0},
				{Timestamp: 1235, Value: 43.0},
				{Timestamp: 1234, Value: 42.0},
			},
		},
		{
			name: "empty dst",
			args: args{
				dst: make([]types.MetricPoint, 0, 10),
				src: []types.MetricPoint{
					{Timestamp: 1234, Value: 42.0},
					{Timestamp: 1235, Value: 43.0},
					{Timestamp: 1236, Value: 44.0},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1236, Value: 44.0},
				{Timestamp: 1235, Value: 43.0},
				{Timestamp: 1234, Value: 42.0},
			},
		},
		{
			name: "no overlap",
			args: args{
				dst: []types.MetricPoint{
					{Timestamp: 1008, Value: 42.0},
					{Timestamp: 1006, Value: 42.0},
					{Timestamp: 1004, Value: 42.0},
				},
				src: []types.MetricPoint{
					{Timestamp: 1000, Value: 42.0},
					{Timestamp: 1001, Value: 42.0},
					{Timestamp: 1003, Value: 42.0},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1008, Value: 42.0},
				{Timestamp: 1006, Value: 42.0},
				{Timestamp: 1004, Value: 42.0},
				{Timestamp: 1003, Value: 42.0},
				{Timestamp: 1001, Value: 42.0},
				{Timestamp: 1000, Value: 42.0},
			},
		},
		{
			name: "overlap",
			args: args{
				dst: []types.MetricPoint{
					{Timestamp: 1008, Value: 42.0},
					{Timestamp: 1006, Value: 42.0},
					{Timestamp: 1004, Value: 42.0},
				},
				src: []types.MetricPoint{
					{Timestamp: 1000, Value: 42.0},
					{Timestamp: 1001, Value: 42.0},
					{Timestamp: 1003, Value: 42.0},
					{Timestamp: 1004, Value: 42.0},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1008, Value: 42.0},
				{Timestamp: 1006, Value: 42.0},
				{Timestamp: 1004, Value: 42.0},
				{Timestamp: 1003, Value: 42.0},
				{Timestamp: 1001, Value: 42.0},
				{Timestamp: 1000, Value: 42.0},
			},
		},
		{
			name: "dup",
			args: args{
				dst: []types.MetricPoint{
					{Timestamp: 1008, Value: 42.0},
					{Timestamp: 1006, Value: 42.0},
					{Timestamp: 1004, Value: 42.0},
				},
				src: []types.MetricPoint{
					{Timestamp: 1000, Value: 42.0},
					{Timestamp: 1001, Value: 42.0},
					{Timestamp: 1001, Value: 42.0},
					{Timestamp: 1003, Value: 42.0},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1008, Value: 42.0},
				{Timestamp: 1006, Value: 42.0},
				{Timestamp: 1004, Value: 42.0},
				{Timestamp: 1003, Value: 42.0},
				{Timestamp: 1001, Value: 42.0},
				{Timestamp: 1000, Value: 42.0},
			},
		},
		{
			name: "overlap2",
			args: args{
				dst: []types.MetricPoint{
					{Timestamp: 1008, Value: 42.0},
					{Timestamp: 1006, Value: 42.0},
					{Timestamp: 1004, Value: 42.0},
				},
				src: []types.MetricPoint{
					{Timestamp: 1000, Value: 42.0},
					{Timestamp: 1001, Value: 42.0},
					{Timestamp: 1003, Value: 42.0},
					{Timestamp: 1005, Value: 42.0},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1008, Value: 42.0},
				{Timestamp: 1006, Value: 42.0},
				{Timestamp: 1005, Value: 42.0},
				{Timestamp: 1004, Value: 42.0},
				{Timestamp: 1003, Value: 42.0},
				{Timestamp: 1001, Value: 42.0},
				{Timestamp: 1000, Value: 42.0},
			},
		},
		{
			name: "overlap3",
			args: args{
				dst: []types.MetricPoint{
					{Timestamp: 1008, Value: 42.0},
					{Timestamp: 1006, Value: 42.0},
					{Timestamp: 1004, Value: 42.0},
				},
				src: []types.MetricPoint{
					{Timestamp: 1000, Value: 42.0},
					{Timestamp: 1001, Value: 42.0},
					{Timestamp: 1004, Value: 42.0},
					{Timestamp: 1005, Value: 42.0},
					{Timestamp: 1007, Value: 42.0},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1008, Value: 42.0},
				{Timestamp: 1007, Value: 42.0},
				{Timestamp: 1006, Value: 42.0},
				{Timestamp: 1005, Value: 42.0},
				{Timestamp: 1004, Value: 42.0},
				{Timestamp: 1001, Value: 42.0},
				{Timestamp: 1000, Value: 42.0},
			},
		},
		{
			name: "src before dst with overlap and dup",
			args: args{
				dst: []types.MetricPoint{
					{Timestamp: 1008, Value: 42.0},
					{Timestamp: 1006, Value: 42.0},
					{Timestamp: 1004, Value: 42.0},
				},
				src: []types.MetricPoint{
					{Timestamp: 1008, Value: 42.0},
					{Timestamp: 1009, Value: 42.0},
					{Timestamp: 1009, Value: 42.0},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1009, Value: 42.0},
				{Timestamp: 1008, Value: 42.0},
				{Timestamp: 1006, Value: 42.0},
				{Timestamp: 1004, Value: 42.0},
			},
		},
		{
			name: "real-merge-with-aggregated-data",
			args: args{
				dst: []types.MetricPoint{
					{Timestamp: 1679417400000, Value: 13.224138},
					{Timestamp: 1679417100000, Value: 16.579925},
					{Timestamp: 1679416800000, Value: 14.961766},
					{Timestamp: 1679416500000, Value: 13.894485},
					{Timestamp: 1679416200000, Value: 16.045372},
					{Timestamp: 1679415900000, Value: 23.730149},
					{Timestamp: 1679415600000, Value: 12.967437},
					{Timestamp: 1679415300000, Value: 8.556047},
				},
				src: []types.MetricPoint{
					{Timestamp: 1679478708000, Value: 13.750635},
					{Timestamp: 1679478718000, Value: 17.987342},
					{Timestamp: 1679478728000, Value: 23.720223},
					{Timestamp: 1679478738000, Value: 16.643498},
					{Timestamp: 1679478748000, Value: 9.113956},
				},
			},
			want: []types.MetricPoint{
				{Timestamp: 1679478748000, Value: 9.113956},
				{Timestamp: 1679478738000, Value: 16.643498},
				{Timestamp: 1679478728000, Value: 23.720223},
				{Timestamp: 1679478718000, Value: 17.987342},
				{Timestamp: 1679478708000, Value: 13.750635},
				{Timestamp: 1679417400000, Value: 13.224138},
				{Timestamp: 1679417100000, Value: 16.579925},
				{Timestamp: 1679416800000, Value: 14.961766},
				{Timestamp: 1679416500000, Value: 13.894485},
				{Timestamp: 1679416200000, Value: 16.045372},
				{Timestamp: 1679415900000, Value: 23.730149},
				{Timestamp: 1679415600000, Value: 12.967437},
				{Timestamp: 1679415300000, Value: 8.556047},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergePoints(tt.args.dst, tt.args.src)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mergePoints() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_mergePointsRandom(t *testing.T) {
	const (
		srcLen = 15
		dstLen = 45
	)

	for testNumber := range 15 {
		t.Run(fmt.Sprintf("test-%d", testNumber), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(int64(testNumber)))

			t.Parallel()

			src := make([]types.MetricPoint, 0, 15)
			dst := make([]types.MetricPoint, 0, 45)

			for range srcLen {
				src = append(src, types.MetricPoint{
					Timestamp: rnd.Int63n(3600),
					Value:     12.34,
				})
			}

			for range dstLen {
				dst = append(dst, types.MetricPoint{
					Timestamp: rnd.Int63n(3600),
					Value:     12.34,
				})
			}

			want := make([]types.MetricPoint, 0, len(src)+len(dst))
			want = append(want, src...)
			want = append(want, dst...)

			sort.Slice(src, func(i, j int) bool {
				return src[i].Timestamp < src[j].Timestamp
			})

			// dst and want are deduplicated
			dst = types.DeduplicatePoints(dst)
			want = types.DeduplicatePoints(want)

			// dst and want are sorted descending
			sort.Slice(dst, func(i, j int) bool {
				return dst[i].Timestamp > dst[j].Timestamp
			})

			sort.Slice(want, func(i, j int) bool {
				return want[i].Timestamp > want[j].Timestamp
			})

			got := mergePoints(dst, src)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("mergePoints() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func BenchmarkMergePoints(b *testing.B) {
	// For a realistic benchmark, src has a fixed size and contains 15 minutes of raw
	// data (1 point every 10s) and 1 days of aggregated data (1 point every 5m).
	nSrc := 15*(60/10) + 1*(24*60*60)/(5*60)

	for nDst := 10; nDst < 10000; nDst *= 2 {
		b.Run(fmt.Sprintf("best_%d", nDst), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				// Create the best case scenario for merge points:
				// no overlaps and all points in src are before the points in dst.
				benchmarkMergePoints(b, nSrc, nDst, 0, nSrc)
			}
		})

		b.Run(fmt.Sprintf("worst_%d", nDst), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				// Create the worst case scenario for merge points:
				// no overlap and all points in src are after the points in dst.
				benchmarkMergePoints(b, nSrc, nDst, nDst+1, 0)
			}
		})

		b.Run(fmt.Sprintf("overlap_%d", nDst), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				// Create dst to overlap with lah the points of src.
				benchmarkMergePoints(b, nSrc, nDst, 0, nSrc/2)
			}
		})
	}
}

func benchmarkMergePoints(b *testing.B, nSrc, nDst, srcTsOffset, dstTsOffset int) { //nolint:stylecheck
	b.Helper()

	src := make([]types.MetricPoint, 0, nSrc)
	dst := make([]types.MetricPoint, 0, nDst)

	for i := range nSrc {
		// src is sorted ascending order by timestamp.
		src = append(src, types.MetricPoint{
			Timestamp: int64(srcTsOffset + i),
			Value:     42,
		})
	}

	for i := range nDst {
		// dst is sorted in descending order by timestamp.
		dst = append(dst, types.MetricPoint{
			Timestamp: int64(dstTsOffset + nDst - i),
			Value:     42,
		})
	}

	mergePoints(dst, src)
}

package tsdb

import (
	"reflect"
	"squirreldb/types"
	"testing"
	"time"
)

func Test_filterPoints(t *testing.T) {
	type args struct {
		points        []types.MetricPoint
		fromTimestamp int64
		toTimestamp   int64
	}
	tests := []struct {
		name string
		args args
		want []types.MetricPoint
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
			if got := filterPoints(tt.args.points, tt.args.fromTimestamp, tt.args.toTimestamp); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterPoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

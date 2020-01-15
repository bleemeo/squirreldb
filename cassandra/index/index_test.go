package index

import (
	"reflect"
	"squirreldb/types"
	"testing"
)

func Benchmark_keyFromLabels(b *testing.B) {
	tests := []struct {
		name   string
		labels []types.MetricLabel
	}{
		{
			name: "simple",
			labels: []types.MetricLabel{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: []types.MetricLabel{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: []types.MetricLabel{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
				{Name: "label3", Value: "value3"},
				{Name: "label4", Value: "value4"},
				{Name: "label5", Value: "value5"},
				{Name: "label6", Value: "value6"},
				{Name: "label7", Value: "value7"},
				{Name: "label8", Value: "value8"},
				{Name: "label9", Value: "value9"},
				{Name: "label0", Value: "value0"},
			},
		},
		{
			name: "five-longer-labels",
			labels: []types.MetricLabel{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: []types.MetricLabel{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = keyFromLabels(tt.labels)
			}
		})
	}
}

func Test_timeToLiveFromLabels(t *testing.T) {
	tests := []struct {
		name       string
		labels     []types.MetricLabel
		want       int64
		wantLabels []types.MetricLabel
	}{
		{
			name: "no ttl",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
			want: 0,
			wantLabels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
				{Name: "__ttl__", Value: "3600"},
			},
			want: 3600,
			wantLabels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl2",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
			want: 3600,
			wantLabels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := timeToLiveFromLabels(&tt.labels)
			if got != tt.want {
				t.Errorf("timeToLiveFromLabels() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.labels, tt.wantLabels) {
				t.Errorf("timeToLiveFromLabels() labels = %v, want %v", tt.labels, tt.wantLabels)
			}
		})
	}
}

func Benchmark_timeToLiveFromLabels(b *testing.B) {
	tests := []struct {
		name       string
		labels     []types.MetricLabel
		wantTTL    int64
		wantLabels []types.MetricLabel
		wantErr    bool
	}{
		{
			name: "no ttl",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: []types.MetricLabel{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "12 labels no ttl",
			labels: []types.MetricLabel{
				{Name: "job", Value: "scrape"},
				{Name: "__name__", Value: "up"},
				{Name: "labels1", Value: "value1"},
				{Name: "labels2", Value: "value2"},
				{Name: "labels3", Value: "value3"},
				{Name: "labels4", Value: "value4"},
				{Name: "labels5", Value: "value5"},
				{Name: "labels6", Value: "value6"},
				{Name: "labels7", Value: "value7"},
				{Name: "labels8", Value: "value8"},
				{Name: "labels9", Value: "value9"},
				{Name: "labels10", Value: "value10"},
			},
		},
		{
			name: "12 labels ttl",
			labels: []types.MetricLabel{
				{Name: "job", Value: "scrape"},
				{Name: "__name__", Value: "up"},
				{Name: "labels1", Value: "value1"},
				{Name: "labels2", Value: "value2"},
				{Name: "labels3", Value: "value3"},
				{Name: "labels4", Value: "value4"},
				{Name: "labels5", Value: "value5"},
				{Name: "labels6", Value: "value6"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "labels7", Value: "value7"},
				{Name: "labels8", Value: "value8"},
				{Name: "labels9", Value: "value9"},
				{Name: "labels10", Value: "value10"},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				labelsIn := make([]types.MetricLabel, len(tt.labels))
				copy(labelsIn, tt.labels)
				_ = timeToLiveFromLabels(&labelsIn)
			}
		})
	}
}

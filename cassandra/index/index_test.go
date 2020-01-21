package index

import (
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func Benchmark_keyFromLabels(b *testing.B) {
	tests := []struct {
		name   string
		labels []*prompb.Label
	}{
		{
			name: "simple",
			labels: []*prompb.Label{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: []*prompb.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: []*prompb.Label{
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
			labels: []*prompb.Label{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: []*prompb.Label{
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
		labels     []*prompb.Label
		want       int64
		wantLabels []*prompb.Label
	}{
		{
			name: "no ttl",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
			want: 0,
			wantLabels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
				{Name: "__ttl__", Value: "3600"},
			},
			want: 3600,
			wantLabels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl2",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
			want: 3600,
			wantLabels: []*prompb.Label{
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
		labels     []*prompb.Label
		wantTTL    int64
		wantLabels []*prompb.Label
		wantErr    bool
	}{
		{
			name: "no ttl",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "12 labels no ttl",
			labels: []*prompb.Label{
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
			labels: []*prompb.Label{
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
				labelsIn := make([]*prompb.Label, len(tt.labels))
				copy(labelsIn, tt.labels)
				_ = timeToLiveFromLabels(&labelsIn)
			}
		})
	}
}

func Test_getLabelsValue(t *testing.T) {
	type args struct {
		labels []*prompb.Label
		name   string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 bool
	}{
		{
			name: "contains",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "up",
					},
					{
						Name:  "monitor",
						Value: "codelab",
					},
				},
				name: "monitor",
			},
			want:  "codelab",
			want1: true,
		},
		{
			name: "contains_empty_value",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "down",
					},
					{
						Name:  "job",
						Value: "",
					},
				},
				name: "job",
			},
			want:  "",
			want1: true,
		},
		{
			name: "no_contains",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "up",
					},
				},
				name: "monitor",
			},
			want:  "",
			want1: false,
		},
		{
			name: "labels_empty",
			args: args{
				labels: []*prompb.Label{},
				name:   "monitor",
			},
			want:  "",
			want1: false,
		},
		{
			name: "labels_nil",
			args: args{
				labels: nil,
				name:   "monitor",
			},
			want:  "",
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getLabelsValue(tt.args.labels, tt.args.name)
			if got != tt.want {
				t.Errorf("getLabelsValue() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("getLabelsValue() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_getMatchersValue(t *testing.T) {
	type args struct {
		matchers []*prompb.LabelMatcher
		name     string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 bool
	}{
		{
			name: "contains",
			args: args{
				matchers: []*prompb.LabelMatcher{
					{
						Name:  "__name__",
						Value: "up",
					},
					{
						Name:  "monitor",
						Value: "codelab",
					},
				},
				name: "monitor",
			},
			want:  "codelab",
			want1: true,
		},
		{
			name: "contains_empty_value",
			args: args{
				matchers: []*prompb.LabelMatcher{
					{
						Name:  "__name__",
						Value: "down",
					},
					{
						Name:  "job",
						Value: "",
					},
				},
				name: "job",
			},
			want:  "",
			want1: true,
		},
		{
			name: "no_contains",
			args: args{
				matchers: []*prompb.LabelMatcher{
					{
						Name:  "__name__",
						Value: "up",
					},
				},
				name: "monitor",
			},
			want:  "",
			want1: false,
		},
		{
			name: "labels_empty",
			args: args{
				matchers: []*prompb.LabelMatcher{},
				name:     "monitor",
			},
			want:  "",
			want1: false,
		},
		{
			name: "labels_nil",
			args: args{
				matchers: nil,
				name:     "monitor",
			},
			want:  "",
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getMatchersValue(tt.args.matchers, tt.args.name)
			if got != tt.want {
				t.Errorf("GetMatchersValue() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetMatchersValue() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_sortLabels(t *testing.T) {
	type args struct {
		labels []*prompb.Label
	}
	tests := []struct {
		name string
		args args
		want []*prompb.Label
	}{
		{
			name: "sorted",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "up",
					},
					{
						Name:  "monitor",
						Value: "codelab",
					},
				},
			},
			want: []*prompb.Label{
				{
					Name:  "__name__",
					Value: "up",
				},
				{
					Name:  "monitor",
					Value: "codelab",
				},
			},
		},
		{
			name: "no_sorted",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "monitor",
						Value: "codelab",
					},
					{
						Name:  "__name__",
						Value: "up",
					},
				},
			},
			want: []*prompb.Label{
				{
					Name:  "__name__",
					Value: "up",
				},
				{
					Name:  "monitor",
					Value: "codelab",
				},
			},
		},
		{
			name: "labels_empty",
			args: args{
				labels: []*prompb.Label{},
			},
			want: nil,
		},
		{
			name: "labels_nil",
			args: args{
				labels: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sortLabels(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SortLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stringFromLabelsCollision(t *testing.T) {
	tests := []struct {
		input1 []*prompb.Label
		input2 []*prompb.Label
	}{
		{
			input1: []*prompb.Label{
				{
					Name:  "label1",
					Value: "value1",
				},
				{
					Name:  "label2",
					Value: "value2",
				},
			},
			input2: []*prompb.Label{
				{
					Name:  "label1",
					Value: "value1,label2=value2",
				},
			},
		},
		{
			input1: []*prompb.Label{
				{
					Name:  "label1",
					Value: "value1",
				},
				{
					Name:  "label2",
					Value: "value2",
				},
			},
			input2: []*prompb.Label{
				{
					Name:  "label1",
					Value: `value1",label2="value2`,
				},
			},
		},
	}
	for _, tt := range tests {
		got1 := stringFromLabels(tt.input1)
		got2 := stringFromLabels(tt.input2)
		if got1 == got2 {
			t.Errorf("StringFromLabels(%v) == StringFromLabels(%v) want not equal", tt.input1, tt.input2)
		}
	}
}

func Test_stringFromLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels []*prompb.Label
		want   string
	}{
		{
			name: "simple",
			labels: []*prompb.Label{
				{Name: "test", Value: "value"},
			},
			want: `test="value"`,
		},
		{
			name: "two",
			labels: []*prompb.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			want: `label1="value1",label2="value2"`,
		},
		{
			name: "two-unordered",
			labels: []*prompb.Label{
				{Name: "label2", Value: "value2"},
				{Name: "label1", Value: "value1"},
			},
			want: `label2="value2",label1="value1"`,
		},
		{
			name: "need-quoting",
			labels: []*prompb.Label{
				{Name: "label1", Value: `value1",label2="value2`},
			},
			want: `label1="value1\",label2=\"value2"`,
		},
		{
			name: "need-quoting2",
			labels: []*prompb.Label{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
			want: `label1="value1\\\",label2=\\\"value2"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringFromLabels(tt.labels); got != tt.want {
				t.Errorf("StringFromLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_stringFromLabels(b *testing.B) {
	tests := []struct {
		name   string
		labels []*prompb.Label
	}{
		{
			name: "simple",
			labels: []*prompb.Label{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: []*prompb.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: []*prompb.Label{
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
			labels: []*prompb.Label{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: []*prompb.Label{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = stringFromLabels(tt.labels)
			}
		})
	}
}

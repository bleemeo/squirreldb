package index

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"
)

type mockIndex struct {
	postings map[string]map[string][]gouuid.UUID
	metrics  map[gouuid.UUID]map[string]string
}

func mockIndexFromMetrics(metrics map[gouuid.UUID]map[string]string) mockIndex {
	result := mockIndex{
		postings: make(map[string]map[string][]gouuid.UUID),
		metrics:  metrics,
	}
	for uuid, labels := range metrics {
		for k, v := range labels {
			if _, ok := result.postings[k]; !ok {
				result.postings[k] = make(map[string][]gouuid.UUID)
			}
			result.postings[k][v] = append(result.postings[k][v], uuid)
		}
	}
	return result
}

func (i mockIndex) LabelValues(name string) ([]string, error) {
	results := make([]string, len(i.postings[name]))
	n := 0

	for v := range i.postings[name] {
		results[n] = v
		n++
	}
	return results, nil
}

func (i mockIndex) LookupLabels(uuid gouuid.UUID) ([]*prompb.Label, error) {
	labelsMap := i.metrics[uuid]
	labels := make([]*prompb.Label, 0, len(labelsMap))
	for k, v := range labelsMap {
		labels = append(labels, &prompb.Label{
			Name:  k,
			Value: v,
		})
	}
	return labels, nil
}

func (i mockIndex) Postings(name string, value string) ([]gouuid.UUID, error) {
	if name == "" {
		resultsMap := make(map[gouuid.UUID]interface{}, len(i.postings))
		for _, values := range i.postings {
			for _, uuids := range values {
				for _, u := range uuids {
					resultsMap[u] = nil
				}
			}
		}
		results := make([]gouuid.UUID, len(resultsMap))
		n := 0
		for u := range resultsMap {
			results[n] = u
			n++
		}

		sort.Slice(results, func(i, j int) bool {
			return uuidIsLess(results[i], results[j])
		})

		return results, nil
	}

	values := i.postings[name]
	if value == "" {
		resultsMap := make(map[gouuid.UUID]interface{}, len(values))
		for _, uuids := range values {
			for _, u := range uuids {
				resultsMap[u] = nil
			}
		}
		results := make([]gouuid.UUID, len(resultsMap))
		n := 0
		for u := range resultsMap {
			results[n] = u
			n++
		}

		sort.Slice(results, func(i, j int) bool {
			return uuidIsLess(results[i], results[j])
		})

		return results, nil
	}

	results := make([]gouuid.UUID, len(values[value]))

	copy(results, values[value])

	sort.Slice(results, func(i, j int) bool {
		return uuidIsLess(results[i], results[j])
	})

	return results, nil
}

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

func Test_postingsForMatchers(t *testing.T) {
	metrics1 := map[gouuid.UUID]map[string]string{
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"): {
			"__name__": "up",
			"job":      "prometheus",
			"instance": "localhost:9090",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"): {
			"__name__": "up",
			"job":      "node_exporter",
			"instance": "localhost:9100",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"): {
			"__name__": "up",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"): {
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "0",
			"mode":     "idle",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"): {
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "0",
			"mode":     "user",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"): {
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "1",
			"mode":     "user",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000007"): {
			"__name__":   "node_filesystem_avail_bytes",
			"job":        "node_exporter",
			"instance":   "localhost:9100",
			"device":     "/dev/mapper/vg0-root",
			"fstype":     "ext4",
			"mountpoint": "/",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"): {
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "localhost:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "devel",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"): {
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "remote:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "production",
		},
		gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000010"): {
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "remote:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "production",
			"userID":      "42",
		},
	}
	index1 := mockIndexFromMetrics(metrics1)

	metrics2 := make(map[gouuid.UUID]map[string]string)

	for x := 0; x < 100; x++ {
		for y := 0; y < 100; y++ {
			uuid := fmt.Sprintf("00000000-0000-%04d-%04d-000000000000", x, y)
			metrics2[gouuid.FromStringOrNil(uuid)] = map[string]string{
				"__name__":   fmt.Sprintf("generated_%03d", x),
				"label_x":    fmt.Sprintf("%03d", x),
				"label_y":    fmt.Sprintf("%03d", y),
				"multiple_2": fmt.Sprintf("%v", y%2 == 0),
				"multiple_3": fmt.Sprintf("%v", y%3 == 0),
				"multiple_5": fmt.Sprintf("%v", y%5 == 0),
			}
		}
	}

	index2 := mockIndexFromMetrics(metrics2)

	type args struct {
		index    Index
		matchers []*prompb.LabelMatcher
	}
	tests := []struct {
		name     string
		index    Index
		matchers []*prompb.LabelMatcher
		want     []gouuid.UUID
		wantLen  int
	}{
		{
			name:  "eq",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "up",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
			},
		},
		{
			name:  "eq-eq",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "mode",
					Value: "user",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
			},
		},
		{
			name:  "eq-neq",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "mode",
					Value: "user",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
		},
		{
			name:  "eq-nolabel",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000007"),
			},
		},
		{
			name:  "eq-label",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "environment",
					Value: "",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000010"),
			},
		},
		{
			name:  "re",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "u.",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
			},
		},
		{
			name:  "re-re",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_cpu_.*",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "mode",
					Value: "^u.*",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
			},
		},
		{
			name:  "re-nre",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_(cpu|disk)_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "mode",
					Value: "u\\wer",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
		},
		{
			name:  "re-re_nolabel",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "environment",
					Value: "^$",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000007"),
			},
		},
		{
			name:  "re-re_label",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "^$",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000010"),
			},
		},
		{
			name:  "re-re*",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "environment",
					Value: ".*",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000007"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000010"),
			},
		},
		{
			name:  "re-nre*",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: ".*",
				},
			},
			want: []gouuid.UUID{},
		},
		{
			name:  "eq-nre_empty_and_devel",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "(|devel)",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000010"),
			},
		},
		{
			name:  "eq-nre-eq same label",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "^$",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "devel",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
			},
		},
		{
			name:  "eq-eq-no_label",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "production",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "userID",
					Value: "",
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
			},
		},
		{
			name:  "index2-eq",
			index: index2,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "generated_042",
				},
			},
			wantLen: 100,
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0042-0000-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0042-0001-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0042-0002-000000000000"),
				// [...]
			},
		},
		{
			name:  "index2-eq-eq",
			index: index2,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "generated_042",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
			},
			wantLen: 50,
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0042-0000-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0042-0002-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0042-0004-000000000000"),
				// [...]
			},
		},
		{
			name:  "index2-eq-neq",
			index: index2,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "multiple_2",
					Value: "false",
				},
			},
			wantLen: 5000,
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0000-0002-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0000-0004-000000000000"),
				// [...]
			},
		},
		{
			name:  "index2-eq-neq-2",
			index: index2,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "multiple_2",
					Value: "true",
				},
			},
			wantLen: 0,
			want:    []gouuid.UUID{},
		},
		{
			name:  "index2-re-neq-eq-neq",
			index: index2,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "generated_04.",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "__name__",
					Value: "generated_042",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "multiple_5",
					Value: "false",
				},
			},
			wantLen: 90,
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0040-0000-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0040-0010-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0040-0020-000000000000"),
				// [...]
			},
		},
		{
			name:  "index2-re-nre-eq-neq",
			index: index2,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "generated_04.",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "__name__",
					Value: "(generated_04(0|2)|)",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "multiple_5",
					Value: "false",
				},
			},
			wantLen: 80,
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0041-0000-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0041-0010-000000000000"),
				gouuid.FromStringOrNil("00000000-0000-0041-0020-000000000000"),
				// [...]
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := postingsForMatchers(tt.index, tt.matchers)
			if err != nil {
				t.Errorf("postingsForMatchers() error = %v", err)
				return
			}
			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}
			if len(got) != tt.wantLen {
				t.Errorf("postingsForMatchers() len()=%v, want %v", len(got), tt.wantLen)
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_intersectResult(t *testing.T) {
	tests := []struct {
		name  string
		lists [][]gouuid.UUID
		want  []gouuid.UUID
	}{
		{
			name: "two-same-list",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
		},
		{
			name: "two-list",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
		},
		{
			name: "two-list-2",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
			},
		},
		{
			name: "three-list",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000e"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
			},
		},
		{
			name: "three-list-2",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000e"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := intersectResult(tt.lists...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("intersectResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_unionResult(t *testing.T) {
	tests := []struct {
		name  string
		lists [][]gouuid.UUID
		want  []gouuid.UUID
	}{
		{
			name: "two-same-list",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
		},
		{
			name: "two-list-1",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
		},
		{
			name: "two-list-2",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
			},
		},
		{
			name: "two-list-2",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
			},
		},
		{
			name: "three-list",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000e"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000e"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
			},
		},
		{
			name: "three-list-2",
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000e"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000009"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000e"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unionResult(tt.lists...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unionResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_substractResult(t *testing.T) {
	tests := []struct {
		name  string
		main  []gouuid.UUID
		lists [][]gouuid.UUID
		want  []gouuid.UUID
	}{
		{
			name: "same-list",
			main: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
			},
			want: []gouuid.UUID{},
		},
		{
			name: "two-list",
			main: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
			},
		},
		{
			name: "two-list-2",
			main: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
			},
		},
		{
			name: "two-list-3",
			main: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
			},
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
			},
		},
		{
			name: "three-list",
			main: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000e"),
			},
			lists: [][]gouuid.UUID{
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000003"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000006"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000c"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
				{
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000005"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
					gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000f"),
				},
			},
			want: []gouuid.UUID{
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000002"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000004"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000008"),
				gouuid.FromStringOrNil("00000000-0000-0000-0000-00000000000e"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := substractResult(tt.main, tt.lists...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("substractResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

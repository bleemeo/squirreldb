package types

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"

	gouuid "github.com/gofrs/uuid"
)

func makePoints(size int) []MetricPoint {
	result := make([]MetricPoint, size)
	for i := 0; i < size; i++ {
		result[i].Timestamp = int64(1568706164 + i*10)
		result[i].Value = float64(i)
	}
	return result
}

func addDuplicate(input []MetricPoint, numberDuplicate int) []MetricPoint {
	duplicates := make([]int, numberDuplicate)
	for i := 0; i < numberDuplicate; i++ {
		duplicates[i] = rand.Intn(len(input))
	}
	sort.Ints(duplicates)
	result := make([]MetricPoint, len(input)+numberDuplicate)

	inputIndex := 0
	duplicatesIndex := 0

	for i := 0; i < len(input)+numberDuplicate; i++ {
		result[i] = input[inputIndex]
		if duplicatesIndex < len(duplicates) && inputIndex == duplicates[duplicatesIndex] {
			duplicatesIndex++
		} else {
			inputIndex++
		}
	}
	if duplicatesIndex != len(duplicates) || inputIndex != len(input) {
		panic("Unexpected value for inputIndex or duplicatesIndex")
	}
	return result
}

func shuffle(input []MetricPoint) []MetricPoint {
	rand.Shuffle(len(input), func(i, j int) {
		input[i], input[j] = input[j], input[i]
	})
	return input
}

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
			m := &MetricUUID{
				UUID: tt.fields.UUID,
			}
			if got := m.Uint64(); got != tt.want {
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
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 20,
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
					Timestamp: 10,
					Value:     20,
				},
				{
					Timestamp: 20,
					Value:     30,
				},
			},
		},
		{
			name: "no_duplicated_no_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 20,
						Value:     30,
					},
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 10,
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
					Timestamp: 10,
					Value:     20,
				},
				{
					Timestamp: 20,
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
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 20,
						Value:     30,
					},
					{
						Timestamp: 20,
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
					Timestamp: 10,
					Value:     20,
				},
				{
					Timestamp: 20,
					Value:     30,
				},
			},
		},
		{
			name: "duplicated_no_sorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 20,
						Value:     30,
					},
					{
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 0,
						Value:     10,
					},
					{
						Timestamp: 20,
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
					Timestamp: 10,
					Value:     20,
				},
				{
					Timestamp: 20,
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

func TestGetLabelsValue(t *testing.T) {
	type args struct {
		labels []MetricLabel
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
				labels: []MetricLabel{
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
				labels: []MetricLabel{
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
				labels: []MetricLabel{
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
				labels: []MetricLabel{},
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
			got, got1 := GetLabelsValue(tt.args.labels, tt.args.name)
			if got != tt.want {
				t.Errorf("GetLabelsValue() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetLabelsValue() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetMatchersValue(t *testing.T) {
	type args struct {
		matchers []MetricLabelMatcher
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
				matchers: []MetricLabelMatcher{
					{
						MetricLabel: MetricLabel{
							Name:  "__name__",
							Value: "up",
						},
					},
					{
						MetricLabel: MetricLabel{
							Name:  "monitor",
							Value: "codelab",
						},
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
				matchers: []MetricLabelMatcher{
					{
						MetricLabel: MetricLabel{
							Name:  "__name__",
							Value: "down",
						},
					},
					{
						MetricLabel: MetricLabel{
							Name:  "job",
							Value: "",
						},
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
				matchers: []MetricLabelMatcher{
					{
						MetricLabel: MetricLabel{
							Name:  "__name__",
							Value: "up",
						},
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
				matchers: []MetricLabelMatcher{},
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
			got, got1 := GetMatchersValue(tt.args.matchers, tt.args.name)
			if got != tt.want {
				t.Errorf("GetMatchersValue() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetMatchersValue() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestSortLabels(t *testing.T) {
	type args struct {
		labels []MetricLabel
	}
	tests := []struct {
		name string
		args args
		want []MetricLabel
	}{
		{
			name: "sorted",
			args: args{
				labels: []MetricLabel{
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
			want: []MetricLabel{
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
				labels: []MetricLabel{
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
			want: []MetricLabel{
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
				labels: []MetricLabel{},
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
			if got := SortLabels(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SortLabels() = %v, want %v", got, tt.want)
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
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 20,
						Value:     30,
					},
					{
						Timestamp: 30,
						Value:     40,
					},
					{
						Timestamp: 40,
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
					Timestamp: 10,
					Value:     20,
				},
				{
					Timestamp: 20,
					Value:     30,
				},
				{
					Timestamp: 30,
					Value:     40,
				},
				{
					Timestamp: 40,
					Value:     50,
				},
			},
		},
		{
			name: "unsorted",
			args: args{
				points: []MetricPoint{
					{
						Timestamp: 20,
						Value:     30,
					},
					{
						Timestamp: 40,
						Value:     50,
					},
					{
						Timestamp: 10,
						Value:     20,
					},
					{
						Timestamp: 30,
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
					Timestamp: 10,
					Value:     20,
				},
				{
					Timestamp: 20,
					Value:     30,
				},
				{
					Timestamp: 30,
					Value:     40,
				},
				{
					Timestamp: 40,
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

func TestUUIDFromString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    MetricUUID
		wantErr bool
	}{
		{
			name: "valid",
			args: args{
				s: "00000000-0000-0000-0000-000000000001",
			},
			want: MetricUUID{
				UUID: gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
			},
			wantErr: false,
		},
		{
			name: "valid_no_dash",
			args: args{
				s: "00000000000000000000000000000001",
			},
			want: MetricUUID{
				UUID: gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
			},
			wantErr: false,
		},
		{
			name: "invalid",
			args: args{
				s: "invalid",
			},
			want: MetricUUID{
				UUID: gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000000"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UUIDFromString(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("UUIDFromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UUIDFromString() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringFromLabelsCollision(t *testing.T) {
	tests := []struct {
		input1 []MetricLabel
		input2 []MetricLabel
	}{
		{
			input1: []MetricLabel{
				{
					Name:  "label1",
					Value: "value1",
				},
				{
					Name:  "label2",
					Value: "value2",
				},
			},
			input2: []MetricLabel{
				{
					Name:  "label1",
					Value: "value1,label2=value2",
				},
			},
		},
		{
			input1: []MetricLabel{
				{
					Name:  "label1",
					Value: "value1",
				},
				{
					Name:  "label2",
					Value: "value2",
				},
			},
			input2: []MetricLabel{
				{
					Name:  "label1",
					Value: `value1",label2="value2`,
				},
			},
		},
	}
	for _, tt := range tests {
		got1 := StringFromLabels(tt.input1)
		got2 := StringFromLabels(tt.input2)
		if got1 == got2 {
			t.Errorf("StringFromLabels(%v) == StringFromLabels(%v) want not equal", tt.input1, tt.input2)
		}
	}
}

func TestStringFromLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels []MetricLabel
		want   string
	}{
		{
			name: "simple",
			labels: []MetricLabel{
				{Name: "test", Value: "value"},
			},
			want: `test="value"`,
		},
		{
			name: "two",
			labels: []MetricLabel{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			want: `label1="value1",label2="value2"`,
		},
		{
			name: "two-unordered",
			labels: []MetricLabel{
				{Name: "label2", Value: "value2"},
				{Name: "label1", Value: "value1"},
			},
			want: `label2="value2",label1="value1"`,
		},
		{
			name: "need-quoting",
			labels: []MetricLabel{
				{Name: "label1", Value: `value1",label2="value2`},
			},
			want: `label1="value1\",label2=\"value2"`,
		},
		{
			name: "need-quoting2",
			labels: []MetricLabel{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
			want: `label1="value1\\\",label2=\\\"value2"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringFromLabels(tt.labels); got != tt.want {
				t.Errorf("StringFromLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkStringFromLabels(b *testing.B) {
	tests := []struct {
		name   string
		labels []MetricLabel
	}{
		{
			name: "simple",
			labels: []MetricLabel{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: []MetricLabel{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: []MetricLabel{
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
			labels: []MetricLabel{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: []MetricLabel{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = StringFromLabels(tt.labels)
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
			points: makePoints(30),
		},
		{
			name:   "no_duplicated_sorted_1000",
			points: makePoints(1000),
		},
		{
			name:   "no_duplicated_sorted_10000",
			points: makePoints(10000),
		},
		{
			name:   "duplicated_sorted_1100",
			points: addDuplicate(makePoints(1000), 100),
		},
		{
			name:   "duplicated_1100",
			points: shuffle(addDuplicate(makePoints(1000), 100)),
		},
		{
			name:   "two_duplicated_block_2000",
			points: append(makePoints(1000), makePoints(1000)...),
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

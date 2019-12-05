package types

import (
	gouuid "github.com/gofrs/uuid"
	"reflect"
	"testing"
)

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

func TestLabelsContains(t *testing.T) {
	type args struct {
		labels []MetricLabel
		name   string
	}
	tests := []struct {
		name  string
		args  args
		want  bool
		want1 string
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
			want:  true,
			want1: "codelab",
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
			want:  true,
			want1: "",
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
			want:  false,
			want1: "",
		},
		{
			name: "labels_empty",
			args: args{
				labels: []MetricLabel{},
				name:   "monitor",
			},
			want:  false,
			want1: "",
		},
		{
			name: "labels_nil",
			args: args{
				labels: nil,
				name:   "monitor",
			},
			want:  false,
			want1: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := ContainsLabels(tt.args.labels, tt.args.name)
			if got != tt.want {
				t.Errorf("ContainsLabels() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ContainsLabels() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestLabelsEqual(t *testing.T) {
	type args struct {
		labels    []MetricLabel
		reference []MetricLabel
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "equal",
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
				reference: []MetricLabel{
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
			want: true,
		},
		{
			name: "no_equal_same_length",
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
				reference: []MetricLabel{
					{
						Name:  "__name__",
						Value: "up",
					},
					{
						Name:  "job",
						Value: "",
					},
				},
			},
			want: false,
		},
		{
			name: "no_equal_no_same_length",
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
				reference: []MetricLabel{
					{
						Name:  "__name__",
						Value: "up",
					},
				},
			},
			want: false,
		},
		{
			name: "labels_empty",
			args: args{
				labels: []MetricLabel{},
				reference: []MetricLabel{
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
			want: false,
		},
		{
			name: "labels_nil",
			args: args{
				labels: nil,
				reference: []MetricLabel{
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
			want: false,
		},
		{
			name: "reference_empty",
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
				reference: []MetricLabel{},
			},
			want: false,
		},
		{
			name: "reference_nil",
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
				reference: nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EqualLabels(tt.args.labels, tt.args.reference); got != tt.want {
				t.Errorf("EqualLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLabelsFromMap(t *testing.T) {
	type args struct {
		m map[string]string
	}
	tests := []struct {
		name string
		args args
		want []MetricLabel
	}{
		{
			name: "map",
			args: args{
				m: map[string]string{
					"__name__": "up",
					"monitor":  "codelab",
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
			name: "map_empty",
			args: args{
				m: make(map[string]string),
			},
			want: nil,
		},
		{
			name: "map_nil",
			args: args{
				m: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LabelsFromMap(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LabelsFromMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLabelsFromMatchers(t *testing.T) {
	type args struct {
		matchers []MetricLabelMatcher
	}
	tests := []struct {
		name string
		args args
		want []MetricLabel
	}{
		{
			name: "matchers",
			args: args{
				matchers: []MetricLabelMatcher{
					{
						MetricLabel: MetricLabel{
							Name:  "__name__",
							Value: "up",
						},
						Type: 1,
					},
					{
						MetricLabel: MetricLabel{
							Name:  "monitor",
							Value: "codelab",
						},
						Type: 2,
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
			name: "matchers_empty",
			args: args{
				matchers: []MetricLabelMatcher{},
			},
			want: nil,
		},
		{
			name: "matchers_nil",
			args: args{
				matchers: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LabelsFromMatchers(tt.args.matchers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LabelsFromMatchers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLabelsSort(t *testing.T) {
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

func TestMapFromLabels(t *testing.T) {
	type args struct {
		labels []MetricLabel
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "labels",
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
			want: map[string]string{
				"__name__": "up",
				"monitor":  "codelab",
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
			if got := MapFromLabels(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MapFromLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPointsDeduplicate(t *testing.T) {
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
			want: nil,
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

func TestPointsSort(t *testing.T) {
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
			want: nil,
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
			if got := SortPoints(tt.args.points); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SortPoints() = %v, want %v", got, tt.want)
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

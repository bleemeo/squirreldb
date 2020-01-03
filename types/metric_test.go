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

func TestEqualLabels(t *testing.T) {
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

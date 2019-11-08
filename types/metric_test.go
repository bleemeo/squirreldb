package types

import (
	"github.com/gofrs/uuid"
	"testing"
)

func TestMetricLabels_Canonical(t *testing.T) {
	tests := []struct {
		name string
		m    MetricLabels
		want string
	}{
		{
			name: "ordered",
			m: MetricLabels{
				{
					Name:  "__name__",
					Value: "testing",
				},
				{
					Name:  "job",
					Value: "job_testing",
				},
				{
					Name:  "monitor",
					Value: "monitor_testing",
				},
			},
			want: `__name__="testing",job="job_testing",monitor="monitor_testing"`,
		},
		{
			name: "unordered",
			m: MetricLabels{
				{
					Name:  "job",
					Value: "job_testing",
				},
				{
					Name:  "__name__",
					Value: "testing",
				},
				{
					Name:  "monitor",
					Value: "monitor_testing",
				},
			},
			want: `__name__="testing",job="job_testing",monitor="monitor_testing"`,
		},
		{
			name: "quotes",
			m: MetricLabels{
				{
					Name:  "__name__",
					Value: "test\"ing",
				},
				{
					Name:  "job",
					Value: "job_test\"ing",
				},
				{
					Name:  "monitor",
					Value: "monitor_test\"ing",
				},
			},
			want: `__name__="test\"ing",job="job_test\"ing",monitor="monitor_test\"ing"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.Canonical(); got != tt.want {
				t.Errorf("Canonical() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetricLabels_Value(t *testing.T) {
	type args struct {
		name string
	}

	tests := []struct {
		name  string
		m     MetricLabels
		args  args
		want  string
		want1 bool
	}{
		{
			name: "existing_label",
			m: []MetricLabel{
				{
					Name:  "__name__",
					Value: "testing",
				},
			},
			args: args{
				name: "__name__",
			},
			want:  "testing",
			want1: true,
		},
		{
			name: "non_existing_label",
			m: []MetricLabel{
				{
					Name:  "__name__",
					Value: "testing",
				},
			},
			args: args{
				name: "non-existing",
			},
			want:  "",
			want1: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.m.Value(tt.args.name)
			if got != tt.want {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Value() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestMetricUUID_Uint64(t *testing.T) {
	type fields struct {
		UUID uuid.UUID
	}

	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		{
			name: "uuid_0",
			fields: fields{
				UUID: uuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
			},
			want: 1,
		},
		{
			name: "uuid_10",
			fields: fields{
				UUID: uuid.FromStringOrNil("00000000-0000-0000-0000-00000000000a"),
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
				t.Errorf("Int64() = %v, want %v", got, tt.want)
			}
		})
	}
}

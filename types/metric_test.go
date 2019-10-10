package types

import (
	"github.com/gofrs/uuid"
	"reflect"
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

func TestMetricLabels_UUID(t *testing.T) {
	tests := []struct {
		name string
		m    MetricLabels
		want MetricUUID
	}{
		{
			name: "valid_uuid",
			m: MetricLabels{
				{
					Name:  "__bleemeo_uuid__",
					Value: "abcdef01-2345-6789-abcd-ef0123456789",
				},
			},
			want: MetricUUID{
				UUID: uuid.FromStringOrNil("abcdef01-2345-6789-abcd-ef0123456789"),
			},
		},
		{
			name: "invalid_uuid",
			m: MetricLabels{
				{
					Name:  "__bleemeo_uuid__",
					Value: "i-am-an-invalid-uuid",
				},
			},
			want: MetricUUID{
				UUID: uuid.FromStringOrNil("00000000-0000-0000-0000-000000000000"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.UUID(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UUID() = %v, want %v", got, tt.want)
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
				UUID: uuid.FromStringOrNil("00000000-0000-0000-0000-000000000000"),
			},
			want: 0,
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

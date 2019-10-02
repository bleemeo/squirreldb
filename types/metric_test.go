package types

import (
	"github.com/gofrs/uuid"
	"reflect"
	"testing"
)

func TestMetric_CanonicalLabels(t *testing.T) {
	type fields struct {
		Labels map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "test_labels_without_quotes",
			fields: fields{Labels: map[string]string{
				"__name__": "testing",
				"job":      "testing_job",
				"monitor":  "testing_monitor",
			}},
			want: `__name__="testing",job="testing_job",monitor="testing_monitor"`,
		},
		{
			name: "test_labels_with_quotes",
			fields: fields{Labels: map[string]string{
				"__name__": "test\"ing",
				"job":      "testing\"job",
				"monitor":  "testing\"monitor",
			}},
			want: `__name__="test\"ing",job="testing\"job",monitor="testing\"monitor"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metric{
				Labels: tt.fields.Labels,
			}
			if got := m.CanonicalLabels(); got != tt.want {
				t.Errorf("CanonicalLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetric_UUID(t *testing.T) {
	type fields struct {
		Labels map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		want    MetricUUID
		wantErr bool
	}{
		{
			name: "test_uuid_label",
			fields: fields{Labels: map[string]string{
				"__uuid__": "abcdef01-2345-6789-abcd-ef0123456789",
			}},
			want: MetricUUID{
				UUID: uuid.FromStringOrNil("abcdef01-2345-6789-abcd-ef0123456789"),
			},
			wantErr: false,
		},
		{
			name: "test_invalid_uuid",
			fields: fields{Labels: map[string]string{
				"__uuid__": "invalid_uuid",
			}},
			want: MetricUUID{
				UUID: uuid.FromStringOrNil("00000000-0000-0000-0000-000000000000"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metric{
				Labels: tt.fields.Labels,
			}
			got, err := m.UUID()
			if (err != nil) != tt.wantErr {
				t.Errorf("UUID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UUID() got = %v, want %v", got, tt.want)
			}
		})
	}
}

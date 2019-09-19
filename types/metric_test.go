package types

import "testing"

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
			name: "labels_without_quotes",
			fields: fields{Labels: map[string]string{
				"__name__": "testing",
				"job":      "testing_job",
				"monitor":  "testing_monitor",
			}},
			want: `__name__="testing",job="testing_job",monitor="testing_monitor"`,
		},
		{
			name: "labels_with_quotes",
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

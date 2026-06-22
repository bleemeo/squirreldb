// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package requestappender

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

// recordingAppender is a storage.Appender that records the labels of the last
// Append call so tests can assert what the request appender forwards.
type recordingAppender struct {
	storage.Appender

	gotLabels labels.Labels
}

func (a *recordingAppender) Append(
	ref storage.SeriesRef,
	l labels.Labels,
	_ int64,
	_ float64,
) (storage.SeriesRef, error) {
	a.gotLabels = l

	return ref, nil
}

func (a *recordingAppender) AppendExemplar(
	ref storage.SeriesRef,
	_ labels.Labels,
	_ exemplar.Exemplar,
) (storage.SeriesRef, error) {
	return ref, nil
}

func (a *recordingAppender) AppendHistogram(
	ref storage.SeriesRef,
	_ labels.Labels,
	_ int64,
	_ *histogram.Histogram,
	_ *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return ref, nil
}

func (a *recordingAppender) UpdateMetadata(
	ref storage.SeriesRef,
	_ labels.Labels,
	_ metadata.Metadata,
) (storage.SeriesRef, error) {
	return ref, nil
}

func (a *recordingAppender) Commit() error                     { return nil }
func (a *recordingAppender) Rollback() error                   { return nil }
func (a *recordingAppender) SetOptions(*storage.AppendOptions) {}

// TestAppend_TenantEnforcement guards tenant isolation: the enforced tenant
// label must always be present with the enforced value on the forwarded
// series, whatever the client sends — including the case where the client sends
// the tenant label with an empty value (which previously deleted the enforced
// tenant, see the empty-value purge ordering in Append).
func TestAppend_TenantEnforcement(t *testing.T) {
	const (
		tenantName  = "__account_id"
		tenantValue = "tenant-A"
	)

	tests := []struct {
		name  string
		input labels.Labels
		want  labels.Labels
	}{
		{
			name:  "tenant absent from request is injected",
			input: labels.FromStrings("__name__", "up", "instance", "host1"),
			want:  labels.FromStrings("__account_id", tenantValue, "__name__", "up", "instance", "host1"),
		},
		{
			name:  "client-supplied tenant value is overridden",
			input: labels.FromStrings("__name__", "up", tenantName, "evil-tenant"),
			want:  labels.FromStrings("__account_id", tenantValue, "__name__", "up"),
		},
		{
			// Regression: an empty-valued tenant label in the request must not
			// delete the enforced tenant.
			name:  "empty client tenant value does not erase enforced tenant",
			input: labels.FromStrings("__name__", "up", tenantName, ""),
			want:  labels.FromStrings("__account_id", tenantValue, "__name__", "up"),
		},
		{
			name:  "empty non-tenant labels are dropped, tenant kept",
			input: labels.FromStrings("__name__", "up", "instance", "", tenantName, ""),
			want:  labels.FromStrings("__account_id", tenantValue, "__name__", "up"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inner := &recordingAppender{}
			app := NewAppender(context.Background(), inner, Options{
				TenantLabel: labels.Label{Name: tenantName, Value: tenantValue},
			})

			if _, err := app.Append(0, tt.input, 0, 0); err != nil {
				t.Fatalf("Append returned error: %v", err)
			}

			if !labels.Equal(inner.gotLabels, tt.want) {
				t.Fatalf("forwarded labels mismatch:\n got  %s\n want %s", inner.gotLabels, tt.want)
			}
		})
	}
}

// TestAppend_NoTenantConfigured ensures empty-valued labels are still dropped
// when no tenant enforcement is configured.
func TestAppend_NoTenantConfigured(t *testing.T) {
	inner := &recordingAppender{}
	app := NewAppender(context.Background(), inner, Options{})

	input := labels.FromStrings("__name__", "up", "instance", "", "job", "node")
	want := labels.FromStrings("__name__", "up", "job", "node")

	if _, err := app.Append(0, input, 0, 0); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	if !labels.Equal(inner.gotLabels, want) {
		t.Fatalf("forwarded labels mismatch:\n got  %s\n want %s", inner.gotLabels, want)
	}
}

func Test_validateLabels(t *testing.T) {
	tests := []struct {
		name    string
		labels  labels.Labels
		wantErr bool
	}{
		{
			name: "validMetricName",
			labels: labels.FromStrings(
				"__name__", "up",
				"__name__", "Up",
				"__name__", "__987daDp:fez",
				"__name__", ":8_987daDp:fez",
				"__name__", "utilisation mémoire",
			),
			wantErr: false,
		},
		{
			name: "validLabelName",
			labels: labels.FromStrings(
				"instance", "localhost:8000",
				"__bleemeo_account__", "320663cd-8c99-4c6a-878a-012bebeff9b1",
				"A1_", "a",
				"système", "a-b",
			),
			wantErr: false,
		},
		{
			name:    "invalidMetricNameEmpty",
			labels:  labels.FromStrings("__name__", ""),
			wantErr: true,
		},
		{
			name:    "invalidMetricNameNotUTF8",
			labels:  labels.FromStrings("__name__", "\xC0"),
			wantErr: true,
		},
		{
			name:    "invalidMetricNameContainsPipeChar",
			labels:  labels.FromStrings("__name__", "one|two"),
			wantErr: true,
		},
		{
			name:    "invalidLabelNameNotUTF8",
			labels:  labels.FromStrings("label-\xC0", "value"),
			wantErr: true,
		},
		{
			name:    "invalidLabelNameContainsPipeChar",
			labels:  labels.FromStrings("one|two", "value"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateLabels(tt.labels); (err == nil) == tt.wantErr {
				t.Fatalf("Failed to validate labels: wantErr=%v, err=%v", tt.wantErr, err)
			}
		})
	}
}

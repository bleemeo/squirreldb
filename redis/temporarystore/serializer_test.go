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

package temporarystore

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// makeLabels is a helper that builds a sorted labels.Labels from a flat
// list of alternating name/value strings.
func makeLabels(pairs ...string) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())
	for i := 0; i+1 < len(pairs); i += 2 {
		b.Set(pairs[i], pairs[i+1])
	}

	return b.Labels()
}

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		labels labels.Labels
	}{
		{
			name:   "empty",
			labels: labels.EmptyLabels(),
		},
		{
			name:   "single label",
			labels: makeLabels("__name__", "cpu_usage"),
		},
		{
			name: "typical metric",
			labels: makeLabels(
				"__name__", "cpu_usage",
				"instance", "host1:9100",
				"job", "node_exporter",
			),
		},
		{
			name: "value with backslash",
			labels: makeLabels(
				"__name__", "disk_usage",
				"path", `C:\Windows`,
			),
		},
		{
			name: "value with newline",
			labels: makeLabels(
				"__name__", "log_lines",
				"annotation", "first line\nsecond line",
			),
		},
		{
			name: "value with backslash and newline",
			labels: makeLabels(
				"__name__", "complex",
				"value", "line1\\\nline2",
			),
		},
		{
			name: "value containing equals sign",
			labels: makeLabels(
				"__name__", "http_requests",
				"url", "/api?foo=bar&baz=qux",
			),
		},
		{
			name: "value is empty string",
			labels: makeLabels(
				"__name__", "metric",
				"optional", "",
				"required", "present",
			),
		},
		{
			name: "many labels",
			labels: makeLabels(
				"__name__", "http_request_duration_seconds",
				"handler", "/api/v1/query",
				"instance", "prometheus:9090",
				"job", "prometheus",
				"le", "0.1",
				"method", "GET",
				"status_code", "200",
			),
		},
		{
			name: "value with only backslashes",
			labels: makeLabels(
				"__name__", "m",
				"p", `\\\\`,
			),
		},
		{
			name: "value with trailing backslash",
			labels: makeLabels(
				"__name__", "m",
				"p", `foo\`,
			),
		},
		{
			name: "value with consecutive newlines",
			labels: makeLabels(
				"__name__", "m",
				"v", "a\n\nb",
			),
		},
		{
			name: "label with double-underscore prefix",
			labels: makeLabels(
				"__account_id__", "tenant-42",
				"__name__", "up",
				"__replica__", "0",
			),
		},
		{
			// UTF-8 label names can be any string. A '=' in the name must not be
			// mistaken for the name/value separator.
			name: "name containing equals sign",
			labels: makeLabels(
				"__name__", "m",
				"a=b", "value",
			),
		},
		{
			name: "name containing newline",
			labels: makeLabels(
				"__name__", "m",
				"line1\nline2", "value",
			),
		},
		{
			name: "name containing backslash",
			labels: makeLabels(
				"__name__", "m",
				`back\slash`, "value",
			),
		},
		{
			name: "name and value both containing equals",
			labels: makeLabels(
				"__name__", "m",
				"k=ey", "va=lue",
			),
		},
		{
			name: "name is a single equals sign",
			labels: makeLabels(
				"=", "value",
				"__name__", "m",
			),
		},
		{
			name: "name with equals and value starting with equals",
			labels: makeLabels(
				"__name__", "m",
				"a=b", "=c",
			),
		},
		{
			name: "name with backslash-n literal (not a newline)",
			labels: makeLabels(
				"__name__", "m",
				`a\nb`, "value",
			),
		},
		{
			name: "name and value with quote",
			labels: makeLabels(
				"__name__", "a \"name\"",
				`a \"key'`, "the 'value\"",
			),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			serialized := serializeLabels(tc.labels)
			got := deserializeLabels(serialized)

			if !labels.Equal(got, tc.labels) {
				t.Errorf("round-trip failed\n  input:      %s\n  serialized: %q\n  got:        %s",
					tc.labels, serialized, got)
			}
		})
	}
}

func TestSerializeLabelsOutput(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		labels   labels.Labels
		wantText string
	}{
		{
			name:     "empty",
			labels:   labels.EmptyLabels(),
			wantText: "",
		},
		{
			name:     "single label",
			labels:   makeLabels("__name__", "up"),
			wantText: "__name__=up",
		},
		{
			name:     "two labels — sorted",
			labels:   makeLabels("__name__", "up", "job", "prometheus"),
			wantText: "__name__=up\njob=prometheus",
		},
		{
			name:     "backslash in value",
			labels:   makeLabels("__name__", "m", "p", `a\b`),
			wantText: `__name__=m` + "\n" + `p=a\\b`,
		},
		{
			name:     "newline in value",
			labels:   makeLabels("__name__", "m", "p", "a\nb"),
			wantText: "__name__=m\np=a\\nb",
		},
		{
			name:     "value with equals sign — not escaped",
			labels:   makeLabels("__name__", "m", "url", "x=y"),
			wantText: "__name__=m\nurl=x=y",
		},
		{
			name:     "name with equals sign — escaped",
			labels:   makeLabels("__name__", "m", "a=b", "v"),
			wantText: "__name__=m\n" + `a\=b=v`,
		},
		{
			name:     "name with backslash — escaped",
			labels:   makeLabels("__name__", "m", `a\b`, "v"),
			wantText: "__name__=m\n" + `a\\b=v`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := serializeLabels(tc.labels)
			if got != tc.wantText {
				t.Errorf("serializeLabels(%s)\n  got:  %q\n  want: %q", tc.labels, got, tc.wantText)
			}
		})
	}
}

func TestDeserializeLabelsEdgeCases(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  labels.Labels
	}{
		{
			name:  "empty string",
			input: "",
			want:  labels.EmptyLabels(),
		},
		{
			name:  "line without equals sign is skipped",
			input: "noequalssign\n__name__=up",
			want:  makeLabels("__name__", "up"),
		},
		{
			name:  "line starting with equals is skipped",
			input: "=badname\n__name__=up",
			want:  makeLabels("__name__", "up"),
		},
		{
			name:  "escaped backslash-n literal (not a newline)",
			input: "__name__=m\nv=hello\\nworld",
			want:  makeLabels("__name__", "m", "v", "hello\nworld"),
		},
		{
			name:  "double escaped backslash",
			input: `__name__=m` + "\n" + `p=a\\b`,
			want:  makeLabels("__name__", "m", "p", `a\b`),
		},
		{
			name:  "unknown escape sequence preserved",
			input: `__name__=m` + "\n" + `p=a\tb`,
			want:  makeLabels("__name__", "m", "p", `a\tb`),
		},
		{
			name:  "value containing equals sign",
			input: "__name__=m\nurl=foo=bar",
			want:  makeLabels("__name__", "m", "url", "foo=bar"),
		},
		{
			name:  "result is sorted regardless of input order",
			input: "zzz=last\naaa=first",
			want:  makeLabels("aaa", "first", "zzz", "last"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := deserializeLabels(tc.input)
			if !labels.Equal(got, tc.want) {
				t.Errorf("deserializeLabels(%q)\n  got:  %s\n  want: %s", tc.input, got, tc.want)
			}
		})
	}
}

// getBenchLabels returns typical Prometheus metric used across benchmarks.
func getBenchLabels() labels.Labels {
	return makeLabels(
		"__name__", "http_request_duration_seconds",
		"handler", "/api/v1/query_range",
		"instance", "prometheus:9090",
		"job", "prometheus",
		"le", "0.25",
		"method", "GET",
		"status_code", "200",
	)
}

func BenchmarkSerializeLabels(b *testing.B) {
	b.ReportAllocs()

	benchLabels := getBenchLabels()

	for b.Loop() {
		_ = serializeLabels(benchLabels)
	}
}

func BenchmarkDeserializeLabels(b *testing.B) {
	b.ReportAllocs()

	benchLabels := getBenchLabels()
	benchSerialized := serializeLabels(benchLabels)

	for b.Loop() {
		_ = deserializeLabels(benchSerialized)
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	b.ReportAllocs()

	benchLabels := getBenchLabels()

	for b.Loop() {
		_ = deserializeLabels(serializeLabels(benchLabels))
	}
}

func BenchmarkSerializeLabels_WithEscaping(b *testing.B) {
	l := makeLabels(
		"__name__", "log_metric",
		"annotation", "line1\\\nline2",
		"path", `C:\Program Files\app`,
	)

	b.ReportAllocs()

	for b.Loop() {
		_ = serializeLabels(l)
	}
}

func BenchmarkDeserializeLabels_WithEscaping(b *testing.B) {
	l := makeLabels(
		"__name__", "log_metric",
		"anno'ta\"tion", "line1\\\nline2",
		"path", `C:\Program Files\app`,
	)
	s := serializeLabels(l)

	b.ReportAllocs()

	for b.Loop() {
		_ = deserializeLabels(s)
	}
}

func BenchmarkSerializeLabels_Large(b *testing.B) {
	l := makeLabels(
		"__account_id__", "tenant-1234",
		"__name__", "http_request_duration_seconds",
		"cluster", "eu-west-1-prod",
		"endpoint", "/api/v1/query_range",
		"handler", "query_range",
		"instance", "thanos-query:10902",
		"job", "thanos-query",
		"le", "+Inf",
		"method", "GET",
		"namespace", "monitoring",
		"pod", "thanos-query-7d9b8c4f6d-xkqpt",
		"service", "thanos-query",
		"status_code", "200",
	)

	b.ReportAllocs()

	for b.Loop() {
		_ = serializeLabels(l)
	}
}

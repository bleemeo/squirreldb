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
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

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

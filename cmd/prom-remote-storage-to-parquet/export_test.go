// Copyright 2015-2024 Bleemeo
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
package main

import (
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func TestLabelsToText(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input          []prompb.Label
		expectedOutput string
	}{
		{
			input: []prompb.Label{
				{
					Name:  "__name__",
					Value: "cpu_used",
				},
				{
					Name:  "item",
					Value: "cpu-7",
				},
			},
			expectedOutput: `__name__="cpu_used",item="cpu-7"`,
		},
		{
			input: []prompb.Label{
				{
					Name:  "__name__",
					Value: "ops",
				},
				{
					Name:  "instance",
					Value: `srv-"g"`,
				},
			},
			expectedOutput: `__name__="ops",instance="srv-\"g\""`,
		},
	}

	for i, tc := range cases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			t.Parallel()

			output := labelsTextFromSlice(tc.input)
			if output != tc.expectedOutput {
				t.Fatalf("expected: %s, got: %s", tc.expectedOutput, output)
			}
		})
	}
}

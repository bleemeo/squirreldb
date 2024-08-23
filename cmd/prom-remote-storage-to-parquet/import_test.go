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

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/xitongsys/parquet-go/common"
)

func TestGetLabelsFromMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input            []*common.Tag
		expectedLabels   map[string][]prompb.Label
		expectedMatchers map[string][]*labels.Matcher
	}{
		{
			input: []*common.Tag{
				{
					InName: "parquet_key",
					ExName: `label="value"`,
				},
			},
			expectedLabels: map[string][]prompb.Label{
				"parquet_key": {
					{
						Name:  "label",
						Value: "value",
					},
				},
			},
			expectedMatchers: map[string][]*labels.Matcher{
				"parquet_key": {
					&labels.Matcher{
						Type:  labels.MatchEqual,
						Name:  "label",
						Value: "value",
					},
				},
			},
		},
		{
			input: []*common.Tag{
				{
					InName: "PARGO_PREFIX___name__6134container_cpu_used3444item6134cassandra34",
					ExName: `__name__="container_cpu_used",item="cassandra"`,
				},
				{
					InName: "PARGO_PREFIX___name__6134container_cpu_used3444item6134redis34",
					ExName: `__name__="container_cpu_used",item="redis"`,
				},
			},
			expectedLabels: map[string][]prompb.Label{
				"PARGO_PREFIX___name__6134container_cpu_used3444item6134cassandra34": {
					{
						Name:  "__name__",
						Value: "container_cpu_used",
					},
					{
						Name:  "item",
						Value: "cassandra",
					},
				},
				"PARGO_PREFIX___name__6134container_cpu_used3444item6134redis34": {
					{
						Name:  "__name__",
						Value: "container_cpu_used",
					},
					{
						Name:  "item",
						Value: "redis",
					},
				},
			},
			expectedMatchers: map[string][]*labels.Matcher{
				"PARGO_PREFIX___name__6134container_cpu_used3444item6134cassandra34": {
					&labels.Matcher{
						Type:  labels.MatchEqual,
						Name:  "__name__",
						Value: "container_cpu_used",
					},
					&labels.Matcher{
						Type:  labels.MatchEqual,
						Name:  "item",
						Value: "cassandra",
					},
				},
				"PARGO_PREFIX___name__6134container_cpu_used3444item6134redis34": {
					&labels.Matcher{
						Type:  labels.MatchEqual,
						Name:  "__name__",
						Value: "container_cpu_used",
					},
					&labels.Matcher{
						Type:  labels.MatchEqual,
						Name:  "item",
						Value: "redis",
					},
				},
			},
		},
	}

	for i, tc := range cases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			t.Parallel()

			// These two (empty) tags represent the root and the timestamp columns.
			infos := append([]*common.Tag{{}, {}}, tc.input...)

			lbls, matchers, err := getLabelsFromSchema(infos)
			if err != nil {
				t.Fatal("Unexpected error:", err)
			}

			if diff := cmp.Diff(tc.expectedLabels, lbls); diff != "" {
				t.Errorf("Unexpected labels:\n%s", diff)
			}

			if diff := cmp.Diff(tc.expectedMatchers, matchers, cmpopts.IgnoreFields(labels.Matcher{}, "re")); diff != "" {
				t.Errorf("Unexpected matchers:\n%s", diff)
			}
		})
	}
}

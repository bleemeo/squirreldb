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
	"github.com/xitongsys/parquet-go/parquet"
)

func ptr[T any](v T) *T {
	return &v
}

func TestGetLabelsFromMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input            []*parquet.KeyValue
		expectedLabels   map[string][]prompb.Label
		expectedMatchers map[string][]*labels.Matcher
	}{
		{
			input: []*parquet.KeyValue{
				{
					Key:   "parquet_key",
					Value: ptr(`label="value"`),
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
			input: []*parquet.KeyValue{
				{
					Key:   "PARGO_PREFIX___name__6134container_cpu_used3444item6134cassandra34",
					Value: ptr(`__name__="container_cpu_used",item="cassandra"`),
				},
				{
					Key:   "PARGO_PREFIX___name__6134container_cpu_used3444item6134redis34",
					Value: ptr(`__name__="container_cpu_used",item="redis"`),
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

			lbls, matchers := getLabelsFromMetadata(tc.input)
			if diff := cmp.Diff(tc.expectedLabels, lbls); diff != "" {
				t.Errorf("Unexpected labels:\n%s", diff)
			}

			if diff := cmp.Diff(tc.expectedMatchers, matchers, cmpopts.IgnoreFields(labels.Matcher{}, "re")); diff != "" {
				t.Errorf("Unexpected matchers:\n%s", diff)
			}
		})
	}
}

func TestDecodeRow(t *testing.T) {
	cases := []struct {
		row                any
		blacklistedColumns map[string]bool
		expectedTS         int64
		expectedValues     map[string]float64
	}{
		{
			row: struct {
				timestamp  int64
				m1, m2, m3 float64
			}{
				1,
				2, 4, 6,
			},
			expectedTS: int64(1),
			expectedValues: map[string]float64{
				"m1": 2, "m2": 4, "m3": 6,
			},
		},
		{
			row: struct {
				timestamp                                                          int64
				PARGO_PREFIX___name__6134container_cpu_used3444item6134cassandra34 float64 //nolint: revive,stylecheck
				PARGO_PREFIX___name__6134container_cpu_used3444item6134redis34     float64 //nolint: revive,stylecheck
			}{
				1723464000000,
				26.465028355393066,
				12.72315815942222,
			},
			expectedTS: 1723464000000,
			expectedValues: map[string]float64{
				"PARGO_PREFIX___name__6134container_cpu_used3444item6134cassandra34": 26.465028355393066,
				"PARGO_PREFIX___name__6134container_cpu_used3444item6134redis34":     12.72315815942222,
			},
		},
		{
			row: struct {
				timestamp      int64
				colOne, colTwo float64
			}{
				timestamp: 1723563670000,
				colOne:    1.1,
				colTwo:    2.2,
			},
			blacklistedColumns: map[string]bool{
				"colOne": true,
			},
			expectedTS: 1723563670000,
			expectedValues: map[string]float64{
				"colTwo": 2.2,
			},
		},
	}

	for i, tc := range cases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			t.Parallel()

			ts, values := decodeRow(tc.row, tc.blacklistedColumns)
			if ts != tc.expectedTS {
				t.Errorf("Expected ts %d, got %d", tc.expectedTS, ts)
			}

			if diff := cmp.Diff(tc.expectedValues, values); diff != "" {
				t.Errorf("Unexpected values:\n%s", diff)
			}
		})
	}
}

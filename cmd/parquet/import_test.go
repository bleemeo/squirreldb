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
	"github.com/prometheus/prometheus/prompb"
	"github.com/xitongsys/parquet-go/parquet"
)

func ptr[T any](v T) *T {
	return &v
}

func TestGetLabelsFromMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input          []*parquet.KeyValue
		expectedOutput map[string][]prompb.Label
	}{
		{
			input: []*parquet.KeyValue{
				{
					Key:   "parquet_key",
					Value: ptr(`label="value"`),
				},
			},
			expectedOutput: map[string][]prompb.Label{
				"parquet_key": {
					{
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
			expectedOutput: map[string][]prompb.Label{
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
		},
	}

	for i, tc := range cases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			t.Parallel()

			output, _ := getLabelsFromMetadata(tc.input) // TODO: verify blacklist
			if diff := cmp.Diff(tc.expectedOutput, output); diff != "" {
				t.Fatalf("Unexpected result:\n%s", diff)
			}
		})
	}
}

func TestDecodeRow(t *testing.T) {
	cases := []struct {
		row            any
		expectedTS     int64
		expectedValues map[string]float64
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
	}

	for i, tc := range cases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			t.Parallel()

			ts, values := decodeRow(tc.row, map[string]bool{}) // TODO: provide a blacklist
			if ts != tc.expectedTS {
				t.Errorf("Expected ts %d, got %d", tc.expectedTS, ts)
			}

			if diff := cmp.Diff(tc.expectedValues, values); diff != "" {
				t.Errorf("Unexpected result:\n%s", diff)
			}
		})
	}
}

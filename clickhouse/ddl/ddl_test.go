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

package ddl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestReplicatedEngine(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		clusterName string
		table       string
		engine      string
		engineArgs  []string
		want        string
	}{
		{
			name:        "single node, no args",
			clusterName: "",
			table:       "tsdb_data",
			engine:      "MergeTree",
			want:        "MergeTree",
		},
		{
			name:        "single node, with args",
			clusterName: "",
			table:       "telemetry_cluster_id",
			engine:      "ReplacingMergeTree",
			engineArgs:  []string{"version"},
			want:        "ReplacingMergeTree(version)",
		},
		{
			name:        "cluster, no args",
			clusterName: "main_cluster",
			table:       "tsdb_data",
			engine:      "MergeTree",
			want:        "ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/tsdb_data', '{replica}')",
		},
		{
			name:        "cluster, with args injects path before args",
			clusterName: "main_cluster",
			table:       "telemetry_cluster_id",
			engine:      "ReplacingMergeTree",
			engineArgs:  []string{"version"},
			want:        "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/telemetry_cluster_id', '{replica}', version)", //nolint:lll
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := replicatedEngine(tc.clusterName, tc.table, tc.engine, tc.engineArgs...); got != tc.want {
				t.Errorf("replicatedEngine(...) = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOnCluster(t *testing.T) {
	t.Parallel()

	if got := OnCluster(""); got != "" {
		t.Errorf("OnCluster(\"\") = %q, want empty", got)
	}

	if got := OnCluster("main_cluster"); got != " ON CLUSTER main_cluster" {
		t.Errorf("OnCluster = %q", got)
	}
}

func TestEngineMatchesMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		engine    string
		clustered bool
		want      bool
	}{
		{"MergeTree", false, true},
		{"AggregatingMergeTree", false, true},
		{"ReplicatedMergeTree", true, true},
		{"ReplicatedAggregatingMergeTree", true, true},
		// The mismatches: a single-node table used in cluster mode replicates
		// nothing, a replicated one used without ON CLUSTER only ever gets DDL on
		// the node SquirrelDB happened to reach.
		{"MergeTree", true, false},
		{"ReplicatedReplacingMergeTree", false, false},
	}

	for _, tc := range cases {
		if got := EngineMatchesMode(tc.engine, tc.clustered); got != tc.want {
			t.Errorf("EngineMatchesMode(%q, %t) = %t, want %t", tc.engine, tc.clustered, got, tc.want)
		}
	}
}

// TestLogEngineMismatch checks the operator gets a named, actionable error for each
// direction, and silence when the engines match — the whole point of the check is
// that a cluster-mode SquirrelDB writing to non-replicated tables cannot stay quiet.
func TestLogEngineMismatch(t *testing.T) {
	t.Parallel()

	objects := []Object{
		{Name: "tsdb_data", Replicated: true},
		{Name: "state", Replicated: true},
		{Name: "datapoint_input"},
	}

	cases := []struct {
		name        string
		clusterName string
		engines     map[string]string
		wantLogged  []string
		wantQuiet   bool
	}{
		{
			name:        "cluster mode, tables not replicated",
			clusterName: "main_cluster",
			engines: map[string]string{
				"tsdb_data": "MergeTree", "state": "ReplicatedReplacingMergeTree", "datapoint_input": "Null",
			},
			wantLogged: []string{"tsdb_data (MergeTree)", "migrate_to_cluster"},
		},
		{
			name:    "single-node mode, tables replicated",
			engines: map[string]string{"tsdb_data": "ReplicatedMergeTree", "datapoint_input": "Null"},
			wantLogged: []string{
				"tsdb_data (ReplicatedMergeTree)", "AS NOT REPLICATED",
			},
		},
		{
			name:        "engines match",
			clusterName: "main_cluster",
			engines: map[string]string{
				"tsdb_data": "ReplicatedMergeTree", "state": "ReplicatedReplacingMergeTree",
				"datapoint_input": "Null",
			},
			wantQuiet: true,
		},
		{
			name:        "absent tables are not a mismatch",
			clusterName: "main_cluster",
			engines:     map[string]string{},
			wantQuiet:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			logEngineMismatch(zerolog.New(&buf), objects, tc.engines, tc.clusterName)

			got := buf.String()

			if tc.wantQuiet && got != "" {
				t.Fatalf("expected no log, got %s", got)
			}

			for _, want := range tc.wantLogged {
				if !strings.Contains(got, want) {
					t.Errorf("log does not mention %q: %s", want, got)
				}
			}
		})
	}
}

// TestSortingKey checks a DDL sorting key normalizes to the form system.tables
// reports it in (sorting_key, e.g. "tenant, metric_name, id"): the migration
// compares them to choose between attaching and copying a table's partitions.
func TestSortingKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ddl  string
		want string
	}{
		{
			name: "tuple",
			ddl: "CREATE TABLE t (a UInt8)\nENGINE = MergeTree\nPRIMARY KEY (a)\n" +
				"ORDER BY (tenant, metric_name, id, tags)\nTTL x",
			want: "tenant,metric_name,id,tags",
		},
		{
			name: "single column, indented",
			ddl:  "\n\t\tCREATE TABLE t (k String)\n\t\tENGINE = ReplacingMergeTree(version)\n\t\tORDER BY k",
			want: "k",
		},
		{
			name: "single column in parentheses",
			ddl:  "CREATE TABLE t (name String)\nENGINE = ReplacingMergeTree(version)\nORDER BY (name)",
			want: "name",
		},
		{
			name: "no ORDER BY",
			ddl:  "CREATE TABLE t (a UInt8)\nENGINE = Null",
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := SortingKey(tc.ddl); got != tc.want {
				t.Errorf("SortingKey = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNormalizeSortingKey(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		// As system.tables reports them. ORDER BY (name) is "name" on ClickHouse
		// 26.3 but "(name)" on 26.8 and 26.9.
		"tenant, metric_name, id": "tenant,metric_name,id",
		"name":                    "name",
		"(name)":                  "name",
		"":                        "",
		// An expression keeps its parentheses: it compares unequal to its DDL
		// form at worst, which callers treat as the safe case.
		"(toDate(t), id)": "(toDate(t),id)",
		"toDate(t), id":   "toDate(t),id",
	}

	for in, want := range cases {
		if got := NormalizeSortingKey(in); got != want {
			t.Errorf("NormalizeSortingKey(%q) = %q, want %q", in, got, want)
		}
	}
}

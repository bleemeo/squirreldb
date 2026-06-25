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

import "testing"

func TestApply(t *testing.T) {
	t.Parallel()

	const template = "CREATE TABLE x{{ON_CLUSTER}} (...) ENGINE = MergeTree"

	cases := []struct {
		name        string
		clusterName string
		want        string
	}{
		{
			name:        "single node strips ON CLUSTER",
			clusterName: "",
			want:        "CREATE TABLE x (...) ENGINE = MergeTree",
		},
		{
			name:        "cluster injects ON CLUSTER",
			clusterName: "main_cluster",
			want:        "CREATE TABLE x ON CLUSTER main_cluster (...) ENGINE = MergeTree",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := Apply(template, tc.clusterName); got != tc.want {
				t.Errorf("Apply(...) = %q, want %q", got, tc.want)
			}
		})
	}
}

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

			if got := ReplicatedEngine(tc.clusterName, tc.table, tc.engine, tc.engineArgs...); got != tc.want {
				t.Errorf("ReplicatedEngine(...) = %q, want %q", got, tc.want)
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

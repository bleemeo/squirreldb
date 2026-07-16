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

//nolint:lll
package telemetrystore

import "testing"

const (
	clusterDDLCluster = `
		CREATE TABLE IF NOT EXISTS telemetry_cluster_id ON CLUSTER test_cluster (
			k String,
			cluster_id String,
			version DateTime64(3) DEFAULT now64(3)
		)
		ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/telemetry_cluster_id', '{replica}', version)
		ORDER BY k`

	nodeIDsDDLCluster = `
		CREATE TABLE IF NOT EXISTS telemetry_node_ids ON CLUSTER test_cluster (
			telemetry_id String,
			owner String,
			lease_until DateTime64(3),
			version DateTime64(3) DEFAULT now64(3)
		)
		ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/telemetry_node_ids', '{replica}', version)
		ORDER BY telemetry_id`
)

// TestSchemaObjectsCluster locks the cluster DDL that ddl.ForCluster derives from
// the single-node templates. This IS the reviewed cluster schema: any change to
// the DDL (or the transform) must be reflected here on purpose — a schema change
// is never casual, it needs a migration.
func TestSchemaObjectsCluster(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"telemetry_cluster_id": clusterDDLCluster,
		"telemetry_node_ids":   nodeIDsDDLCluster,
	}

	for _, o := range SchemaObjects("test_cluster") {
		if !o.Replicated {
			t.Errorf("%s should be Replicated", o.Name)
		}

		if o.DDL != want[o.Name] {
			t.Errorf("%s cluster DDL =\n%q\nwant\n%q", o.Name, o.DDL, want[o.Name])
		}

		delete(want, o.Name)
	}

	for name := range want {
		t.Errorf("missing object %q from SchemaObjects", name)
	}

	// Single-node mode uses each template verbatim (valid single-node SQL).
	singleNode := map[string]string{"telemetry_cluster_id": clusterDDL, "telemetry_node_ids": nodeIDsDDL}
	for _, o := range SchemaObjects("") {
		if o.DDL != singleNode[o.Name] {
			t.Errorf("%s single-node DDL =\n%q\nwant the template verbatim\n%q", o.Name, o.DDL, singleNode[o.Name])
		}
	}
}

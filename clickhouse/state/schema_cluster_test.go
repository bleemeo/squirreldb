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

package state

import "testing"

const stateDDLCluster = `
	CREATE TABLE IF NOT EXISTS state ON CLUSTER test_cluster (
		name String,
		value String,
		version DateTime64(3) DEFAULT now64(3)
	)
	ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/state', '{replica}', version)
	ORDER BY (name)`

// TestSchemaObjectsCluster locks the cluster DDL that ddl.ForCluster derives from
// the single-node stateDDL. This IS the reviewed cluster schema: any change to
// stateDDL (or the transform) must be reflected here on purpose — a schema change
// is never casual, it needs a migration.
func TestSchemaObjectsCluster(t *testing.T) {
	t.Parallel()

	objs := SchemaObjects("test_cluster")
	if len(objs) != 1 {
		t.Fatalf("got %d objects, want 1", len(objs))
	}

	if objs[0].Name != "state" || !objs[0].Replicated {
		t.Errorf("object = {Name:%q Replicated:%v}, want {state true}", objs[0].Name, objs[0].Replicated)
	}

	if objs[0].DDL != stateDDLCluster {
		t.Errorf("state cluster DDL =\n%q\nwant\n%q", objs[0].DDL, stateDDLCluster)
	}

	// Single-node mode uses the template verbatim (it is valid single-node SQL).
	if got := SchemaObjects("")[0].DDL; got != stateDDL {
		t.Errorf("state single-node DDL =\n%q\nwant the stateDDL template verbatim\n%q", got, stateDDL)
	}
}

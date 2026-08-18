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

package backend

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"
)

// migrationObjects is a two-replicable-tables schema plus a write-path object, the
// shape the real schema has (see allSchemaObjects).
//
//nolint:gochecknoglobals
var migrationObjects = []ddl.Object{
	{Name: "tsdb_data", Replicated: true},
	{Name: "state", Replicated: true},
	{Name: "datapoint_input"},
}

// TestPlanMigration covers each per-table state the endpoint can be called in: it
// is re-callable, so it must resume from a rename done but not the create, from a
// create done but not the attach, and refuse the states it cannot interpret.
func TestPlanMigration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		engines map[string]string
		want    []tableStep
		wantErr error
	}{
		{
			name: "single node, nothing migrated yet",
			engines: map[string]string{
				"tsdb_data": "MergeTree", "state": "ReplacingMergeTree", "datapoint_input": "Null",
			},
			want: []tableStep{
				{Table: "tsdb_data", Source: "tsdb_data_pre_cluster", Rename: true, Create: true},
				{Table: "state", Source: "state_pre_cluster", Rename: true, Create: true},
			},
		},
		{
			name: "renamed but not created yet",
			engines: map[string]string{
				"tsdb_data_pre_cluster": "MergeTree", "state": "ReplicatedReplacingMergeTree",
			},
			want: []tableStep{{Table: "tsdb_data", Source: "tsdb_data_pre_cluster", Create: true}},
		},
		{
			name: "created but not attached yet",
			engines: map[string]string{
				"tsdb_data": "ReplicatedMergeTree", "tsdb_data_pre_cluster": "MergeTree",
				"state": "ReplicatedReplacingMergeTree",
			},
			want: []tableStep{{Table: "tsdb_data", Source: "tsdb_data_pre_cluster"}},
		},
		{
			name: "fully migrated and cleaned up",
			engines: map[string]string{
				"tsdb_data": "ReplicatedMergeTree", "state": "ReplicatedReplacingMergeTree",
				"datapoint_input": "Null",
			},
			want: nil,
		},
		{
			name:    "a table this deployment never had is only created",
			engines: map[string]string{"tsdb_data": "ReplicatedMergeTree"},
			want:    []tableStep{{Table: "state", Create: true}},
		},
		{
			name: "two datasets for the same table",
			engines: map[string]string{
				"tsdb_data": "MergeTree", "tsdb_data_pre_cluster": "MergeTree",
			},
			wantErr: errTwoDatasets,
		},
		{
			name:    "an already-replicated source is an impossible state",
			engines: map[string]string{"tsdb_data_pre_cluster": "ReplicatedMergeTree"},
			wantErr: errReplicatedSource,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			steps, err := planMigration(migrationObjects, tc.engines)

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}

				if steps != nil {
					t.Errorf("steps = %v, want none on error", steps)
				}

				return
			}

			if err != nil {
				t.Fatalf("planMigration: %v", err)
			}

			if len(steps) != len(tc.want) {
				t.Fatalf("steps = %+v, want %+v", steps, tc.want)
			}

			for i, step := range steps {
				if !reflect.DeepEqual(step, tc.want[i]) {
					t.Errorf("step %d = %+v, want %+v", i, step, tc.want[i])
				}
			}
		})
	}
}

// TestHoldsSingleNodeData checks the host scan sees data to migrate in both the
// not-yet-renamed and the mid-migration state, and not on a fully replicated node —
// picking the wrong host would run the rename where the data is not.
func TestHoldsSingleNodeData(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		engines map[string]string
		want    bool
	}{
		{
			name:    "not migrated yet",
			engines: map[string]string{"tsdb_data": "MergeTree"},
			want:    true,
		},
		{
			name:    "mid-migration leftover",
			engines: map[string]string{"tsdb_data": "ReplicatedMergeTree", "tsdb_data_pre_cluster": "MergeTree"},
			want:    true,
		},
		{
			name: "fully replicated node",
			engines: map[string]string{
				"tsdb_data": "ReplicatedMergeTree", "state": "ReplicatedReplacingMergeTree",
			},
		},
		{
			name:    "empty node",
			engines: map[string]string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := holdsSingleNodeData(migrationObjects, tc.engines); got != tc.want {
				t.Errorf("holdsSingleNodeData = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestMigrationTableNames(t *testing.T) {
	t.Parallel()

	want := []string{
		"tsdb_data", "tsdb_data_pre_cluster",
		"state", "state_pre_cluster",
	}

	got := migrationTableNames(migrationObjects)

	if len(got) != len(want) {
		t.Fatalf("migrationTableNames = %v, want %v", got, want)
	}

	for i, name := range got {
		if name != want[i] {
			t.Errorf("name %d = %q, want %q", i, name, want[i])
		}
	}
}

func TestTableStepDataTable(t *testing.T) {
	t.Parallel()

	// Before the rename the partitions are still under the final name.
	pending := tableStep{Table: "tsdb_data", Source: "tsdb_data_pre_cluster", Rename: true}
	if got := pending.dataTable(); got != "tsdb_data" {
		t.Errorf("dataTable before rename = %q, want tsdb_data", got)
	}

	renamed := tableStep{Table: "tsdb_data", Source: "tsdb_data_pre_cluster"}
	if got := renamed.dataTable(); got != "tsdb_data_pre_cluster" {
		t.Errorf("dataTable after rename = %q, want tsdb_data_pre_cluster", got)
	}
}

// TestSplitSchema pins what the migration is allowed to drop: the write path must
// be EXACTLY these objects, in this order (datapoint_input first, see
// droppableWritePath), and everything else must be a replicable table. Adding a
// non-replicated table that stores something — a node-local schema-version table,
// say — must fail here and in splitSchema rather than get dropped silently.
func TestSplitSchema(t *testing.T) {
	t.Parallel()

	wantWritePath := []string{"datapoint_input", "mv_datapoint_data", "mv_datapoint_agg_15m"}

	writePath, replicable, err := splitSchema(allSchemaObjects(Options{ClusterName: "main_cluster"}))
	if err != nil {
		t.Fatalf("splitSchema: %v", err)
	}

	if got := names(writePath); !slices.Equal(got, wantWritePath) {
		t.Errorf("write path = %v, want exactly %v", got, wantWritePath)
	}

	for _, o := range replicable {
		if !o.Replicated {
			t.Errorf("%s is in the replicable set but is not replicated", o.Name)
		}
	}

	if len(replicable) == 0 {
		t.Error("no replicable table, the migration would have nothing to do")
	}
}

// TestSplitSchemaRefusesUnknownObject checks the guard itself: a non-replicated
// object nobody declared droppable stops the migration.
func TestSplitSchemaRefusesUnknownObject(t *testing.T) {
	t.Parallel()

	objects := append(allSchemaObjects(Options{ClusterName: "main_cluster"}),
		ddl.Object{Name: "schema_migration_state", DDL: "CREATE TABLE schema_migration_state ..."})

	if _, _, err := splitSchema(objects); !errors.Is(err, errUnclassifiedObject) {
		t.Errorf("err = %v, want errUnclassifiedObject", err)
	}
}

func names(objects []ddl.Object) []string {
	out := make([]string, len(objects))
	for i, o := range objects {
		out[i] = o.Name
	}

	return out
}

// TestPlanSources checks the cleanup work list covers both the sources already
// emptied by an earlier call and those the migration is about to empty, and skips
// the steps that only create a table.
func TestPlanSources(t *testing.T) {
	t.Parallel()

	plan := migrationPlan{
		Steps: []tableStep{
			{Table: "tsdb_data", Source: "tsdb_data_pre_cluster", Rename: true, Create: true},
			{Table: "brand_new", Create: true},
		},
		CleanupPending: []string{"state_pre_cluster"},
	}

	want := []string{"state_pre_cluster", "tsdb_data_pre_cluster"}

	got := plan.sources()
	if len(got) != len(want) {
		t.Fatalf("sources = %v, want %v", got, want)
	}

	for i, source := range got {
		if source != want[i] {
			t.Errorf("source %d = %q, want %q", i, source, want[i])
		}
	}
}

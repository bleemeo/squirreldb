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

// Package ddl helps the ClickHouse stores produce cluster-aware DDL from a
// single template, so the same schema works on a single node and on a cluster.
//
// A DDL template carries the {{ON_CLUSTER}} placeholder right after the created
// object name. On a cluster it becomes " ON CLUSTER <name>" so the statement
// runs on every node; on a single node it vanishes.
//
// The MergeTree-family engine clause is built by ReplicatedEngine, not by a
// placeholder, because the replica path/name must be injected inside the engine
// parentheses (before any engine argument such as the ReplacingMergeTree
// version column).
//
// On a cluster, Replicated*MergeTree engines are created with an EXPLICIT,
// UUID-free Keeper path derived from the table name
// ('/clickhouse/tables/{shard}/{database}/<table>', '{replica}'). This is
// deliberate: the default_replica_path embeds the table {uuid}, so a rebuilt
// replica that re-creates the table would get a fresh uuid -> a different
// Keeper path -> a forked, split-brain table. A stable, name-based path makes
// the re-created table re-attach to the existing data instead.
//
// CONSEQUENCE: the table name is part of the replication identity, so these
// tables must NEVER be RENAMEd (a rename would still use the same the Keeper
// path and break creation of a new table with initial name).
// See examples/squirreldb_ha_clickhouse for the node-recovery runbook.
package ddl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/rs/zerolog"
)

// codeReplicaAlreadyExists is the ClickHouse error code (REPLICA_ALREADY_EXISTS)
// returned when re-creating a replicated table whose replica is still registered
// in Keeper — i.e. the table's data already exists cluster-wide and this is a
// degraded/recovering node, not a fresh bootstrap.
const codeReplicaAlreadyExists = 253

// Object is a schema object (table, view) to ensure exists. DDL is the full
// CREATE statement with {{ON_CLUSTER}} and the engine already applied. Replicated
// marks the MergeTree-family tables (those a SYSTEM DROP REPLICA applies to),
// versus the Null table and materialized view which are not replicated.
type Object struct {
	Name       string
	DDL        string
	Replicated bool
}

// ReplicaExistsError is returned by CreateTables when a table cannot be created
// because its replica is still registered in Keeper (a lost/rebuilt replica that
// needs SYSTEM DROP REPLICA first). The caller turns it into an actionable hint.
type ReplicaExistsError struct {
	Table string
}

func (e *ReplicaExistsError) Error() string {
	return fmt.Sprintf(
		"table %s already exists on another replica in Keeper but not on this node "+
			"(lost/rebuilt replica — SYSTEM DROP REPLICA needed)", e.Table)
}

// Apply fills the {{ON_CLUSTER}} placeholder of a template. An empty clusterName
// yields plain single-node DDL.
func Apply(template, clusterName string) string {
	return strings.ReplaceAll(template, "{{ON_CLUSTER}}", OnCluster(clusterName))
}

// ReplicatedEngine builds a MergeTree-family engine clause. On a single node it
// is the plain engine (e.g. "MergeTree", "ReplacingMergeTree(version)"). On a
// cluster it is the Replicated* variant with an explicit, UUID-free Keeper path
// derived from the table name, so a rebuilt replica re-creating the table
// re-attaches to the same data instead of forking into a split-brain table.
//
// The path uses the {shard} and {replica} macros (from the server config) and
// the implicit {database} macro. Because the table name is embedded in the
// path, these tables must NEVER be RENAMEd.
//
//	ReplicatedEngine("", "tsdb_data", "MergeTree")
//	  -> "MergeTree"
//	ReplicatedEngine("c", "tsdb_data", "MergeTree")
//	  -> "ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/tsdb_data', '{replica}')"
//	ReplicatedEngine("", "x", "ReplacingMergeTree", "version")
//	  -> "ReplacingMergeTree(version)"
//	ReplicatedEngine("c", "x", "ReplacingMergeTree", "version")
//	  -> "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/x', '{replica}', version)"
func ReplicatedEngine(clusterName, table, engine string, engineArgs ...string) string {
	if clusterName == "" {
		if len(engineArgs) == 0 {
			return engine
		}

		return engine + "(" + strings.Join(engineArgs, ", ") + ")"
	}

	args := append([]string{
		"'/clickhouse/tables/{shard}/{database}/" + table + "'",
		"'{replica}'",
	}, engineArgs...)

	return "Replicated" + engine + "(" + strings.Join(args, ", ") + ")"
}

// EnsureTables creates the given objects if they are missing. It is safe to call
// on every startup and never blocks startup on a degraded node:
//
//   - Objects that already exist are skipped, so a steady-state restart issues no
//     DDL at all (and a restart while a node is being rebuilt skips creation too,
//     because the routing connection checks existence against a healthy host).
//   - A missing object is created (a genuine bootstrap). A REPLICA_ALREADY_EXISTS
//     error means the object's data already lives in Keeper but not on the node
//     that ran the CREATE (a degraded/recovering node, not a bootstrap): it is
//     logged and tolerated so startup proceeds.
//   - Any other creation error is returned (a real bootstrap failure the operator
//     must see, e.g. a timed-out ON CLUSTER DDL on a fresh cluster).
func EnsureTables(ctx context.Context, conn driver.Conn, logger zerolog.Logger, objects []Object) error {
	names := make([]string, len(objects))
	for i, o := range objects {
		names[i] = o.Name
	}

	present, err := existingTables(ctx, conn, names)
	if err != nil {
		// Cannot determine what exists; fall through and attempt creation, which
		// is idempotent (CREATE ... IF NOT EXISTS).
		logger.Warn().Err(err).Msg("could not list existing ClickHouse tables; will attempt creation")

		present = map[string]bool{}
	}

	missing := make([]Object, 0, len(objects))

	for _, o := range objects {
		if !present[o.Name] {
			missing = append(missing, o)
		}
	}

	// Startup tolerates REPLICA_ALREADY_EXISTS: it means the table exists
	// cluster-wide (this node is degraded/recovering) — not a reason to fail boot.
	return createObjects(ctx, conn, logger, missing, true)
}

// CreateTables runs the CREATE ... IF NOT EXISTS statement of every object,
// whether or not it already exists. Unlike EnsureTables (the startup path, which
// skips existing tables), it is meant to be triggered on demand to (re)create the
// schema — e.g. to re-attach a rebuilt node after its stale replica has been
// dropped: the ON CLUSTER statement is a no-op on the healthy nodes and re-creates
// the table on the rebuilt one. It STOPS at the first REPLICA_ALREADY_EXISTS
// (returning *ReplicaExistsError), so the caller can tell the operator to run
// SYSTEM DROP REPLICA rather than letting a dependent object fail with a confusing
// UNKNOWN_TABLE.
func CreateTables(ctx context.Context, conn driver.Conn, logger zerolog.Logger, objects []Object) error {
	return createObjects(ctx, conn, logger, objects, false)
}

func createObjects(
	ctx context.Context, conn driver.Conn, logger zerolog.Logger, objects []Object, tolerateReplicaExists bool,
) error {
	for _, o := range objects {
		if err := conn.Exec(ctx, o.DDL); err != nil {
			if ex, ok := errors.AsType[*proto.Exception](err); ok && ex.Code == codeReplicaAlreadyExists {
				if !tolerateReplicaExists {
					return &ReplicaExistsError{Table: o.Name}
				}

				logger.Warn().Str("table", o.Name).Msg(
					"ClickHouse table already exists in Keeper but not on this node " +
						"(degraded/recovering node?); continuing — see recovery runbook")

				continue
			}

			return fmt.Errorf("create %s: %w", o.Name, err)
		}

		logger.Debug().Str("table", o.Name).Msg("ClickHouse table created")
	}

	return nil
}

// existingTables returns, among names, those that exist in the current database.
func existingTables(ctx context.Context, conn driver.Conn, names []string) (map[string]bool, error) {
	rows, err := conn.Query(ctx,
		"SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN (?)", names)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	present := make(map[string]bool, len(names))

	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		present[name] = true
	}

	return present, rows.Err()
}

// OnCluster returns the " ON CLUSTER <name>" clause (empty for a single node),
// for statements built without a template (e.g. DROP).
func OnCluster(clusterName string) string {
	if clusterName == "" {
		return ""
	}

	return " ON CLUSTER " + clusterName
}

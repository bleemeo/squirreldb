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
// A DDL template is written as plain, copy-pasteable single-node SQL (a real
// MergeTree-family engine, no ON CLUSTER). ForCluster derives the cluster form:
// it inserts " ON CLUSTER <name>" after the created object so the statement runs
// on every node, and rewrites the engine into its Replicated* variant. An empty
// clusterName returns the template verbatim (the single-node form). The cluster
// form of each store's real schema is asserted in its SchemaObjects test, so the
// generated cluster DDL is under review without a second copy of the schema.
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
	"regexp"
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

// replicatedPrefix is the prefix ClickHouse gives the replicated variant of every
// MergeTree-family engine.
const replicatedPrefix = "Replicated"

// Object is a schema object (table, view) to ensure exists. DDL is the full
// CREATE statement, already run through ForCluster. Replicated marks the
// MergeTree-family tables (those a SYSTEM DROP REPLICA applies to), versus the
// Null table and materialized view which are not replicated.
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

// createObjectRe captures the created object's name in a CREATE TABLE / CREATE
// MATERIALIZED VIEW statement: it is where " ON CLUSTER" is inserted and the
// source of the replicated-table Keeper path. Anchored at a line start; the name
// is an unqualified identifier (the schemas use no db-qualified names).
var createObjectRe = regexp.MustCompile(
	`(?m)^\s*CREATE\s+(?:TABLE|MATERIALIZED\s+VIEW)\s+(?:IF NOT EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)`)

// mergeTreeEngineRe matches a MergeTree-family ENGINE clause (optional args), so
// it can be swapped for its Replicated* variant. Anchored at a line start so a
// MergeTree name inside a comment is not matched; a non-MergeTree engine (Null)
// and the TO-target materialized view (no ENGINE clause) never match, so they
// keep their single-node form and only receive the ON CLUSTER clause.
var mergeTreeEngineRe = regexp.MustCompile(`(?m)^(\s*)ENGINE\s*=\s*(\w*MergeTree)(?:\(([^)]*)\))?`)

// ForCluster turns a single-node DDL template into cluster DDL: it inserts
// " ON CLUSTER <name>" after the created object and rewrites a MergeTree-family
// engine into its Replicated* variant (with the name-based Keeper path, see
// replicatedEngine). An empty clusterName returns the template unchanged — it is
// already valid single-node SQL.
func ForCluster(template, clusterName string) string {
	if clusterName == "" {
		return template
	}

	loc := createObjectRe.FindStringSubmatchIndex(template)
	if loc == nil {
		// Programming error: a DDL template must open with CREATE TABLE/VIEW.
		panic("ddl: ForCluster: no CREATE TABLE/MATERIALIZED VIEW in template")
	}

	name := template[loc[2]:loc[3]]

	// Insert ON CLUSTER right after the object name (loc[3] is the name's end).
	result := template[:loc[3]] + OnCluster(clusterName) + template[loc[3]:]

	// Rewrite the MergeTree-family engine, if any, into its Replicated* variant.
	return mergeTreeEngineRe.ReplaceAllStringFunc(result, func(match string) string {
		sub := mergeTreeEngineRe.FindStringSubmatch(match)
		indent, engine, argList := sub[1], sub[2], sub[3]

		var args []string
		if strings.TrimSpace(argList) != "" {
			args = splitEngineArgs(argList)
		}

		return indent + "ENGINE = " + replicatedEngine(clusterName, name, engine, args...)
	})
}

// splitEngineArgs splits an engine argument list on commas, trimming each arg.
func splitEngineArgs(argList string) []string {
	args := strings.Split(argList, ",")
	for i := range args {
		args[i] = strings.TrimSpace(args[i])
	}

	return args
}

// replicatedEngine builds a MergeTree-family engine clause. On a single node it
// is the plain engine (e.g. "MergeTree", "ReplacingMergeTree(version)"). On a
// cluster it is the Replicated* variant with an explicit, UUID-free Keeper path
// derived from the table name, so a rebuilt replica re-creating the table
// re-attaches to the same data instead of forking into a split-brain table.
//
// The path uses the {shard} and {replica} macros (from the server config) and
// the implicit {database} macro. Because the table name is embedded in the
// path, these tables must NEVER be RENAMEd.
//
//	replicatedEngine("", "tsdb_data", "MergeTree")
//	  -> "MergeTree"
//	replicatedEngine("c", "tsdb_data", "MergeTree")
//	  -> "ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/tsdb_data', '{replica}')"
//	replicatedEngine("", "x", "ReplacingMergeTree", "version")
//	  -> "ReplacingMergeTree(version)"
//	replicatedEngine("c", "x", "ReplacingMergeTree", "version")
//	  -> "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/x', '{replica}', version)"
func replicatedEngine(clusterName, table, engine string, engineArgs ...string) string {
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

	return replicatedPrefix + engine + "(" + strings.Join(args, ", ") + ")"
}

// EngineMatchesMode reports whether an engine, as named by system.tables (e.g.
// "MergeTree", "ReplicatedAggregatingMergeTree"), matches the mode in use:
// Replicated* on a cluster, plain on a single node.
//
// Only meaningful for the replicable tables (Object.Replicated): the Null table
// and the materialized views keep the same engine in both modes.
func EngineMatchesMode(engine string, clustered bool) bool {
	return strings.HasPrefix(engine, replicatedPrefix) == clustered
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
//
// An existing table whose engine does not match clusterName is logged (see
// logEngineMismatch) but never a boot failure: the operator needs a running
// SquirrelDB to run the migration endpoint that fixes it.
func EnsureTables(
	ctx context.Context, conn driver.Conn, logger zerolog.Logger, objects []Object, clusterName string,
) error {
	names := make([]string, len(objects))
	for i, o := range objects {
		names[i] = o.Name
	}

	engines, err := TableEngines(ctx, conn, names)
	if err != nil {
		// Cannot determine what exists; fall through and attempt creation, which
		// is idempotent (CREATE ... IF NOT EXISTS).
		logger.Warn().Err(err).Msg("could not list existing ClickHouse tables; will attempt creation")

		engines = map[string]string{}
	}

	missing := make([]Object, 0, len(objects))

	for _, o := range objects {
		if _, ok := engines[o.Name]; !ok {
			missing = append(missing, o)
		}
	}

	logEngineMismatch(logger, objects, engines, clusterName)

	// Startup tolerates REPLICA_ALREADY_EXISTS: it means the table exists
	// cluster-wide (this node is degraded/recovering) — not a reason to fail boot.
	return createObjects(ctx, conn, logger, missing, true)
}

// logEngineMismatch reports the existing tables whose engine contradicts
// clusterName. Without it a cluster-mode SquirrelDB pointed at tables created by a
// single-node one finds every table present, creates nothing, and writes to
// non-replicated tables — replicating nothing, silently.
func logEngineMismatch(logger zerolog.Logger, objects []Object, engines map[string]string, clusterName string) {
	clustered := clusterName != ""

	var notReplicated, replicated []string

	for _, o := range objects {
		engine, exists := engines[o.Name]
		if !o.Replicated || !exists || EngineMatchesMode(engine, clustered) {
			continue
		}

		if clustered {
			notReplicated = append(notReplicated, o.Name+" ("+engine+")")
		} else {
			replicated = append(replicated, o.Name+" ("+engine+")")
		}
	}

	if len(notReplicated) > 0 {
		logger.Error().Msgf(
			"cluster_name is set (%s) but these ClickHouse tables are NOT replicated: %s. "+
				"Their data stays on one node — POST /debug/clickhouse/migrate_to_cluster to migrate them "+
				"(see the migration runbook)",
			clusterName, strings.Join(notReplicated, ", "))
	}

	if len(replicated) > 0 {
		logger.Error().Msgf(
			"cluster_name is empty but these ClickHouse tables are replicated: %s. "+
				"SquirrelDB will use them without ON CLUSTER DDL, so schema changes reach only one node — "+
				"set cluster_name back, or convert each table with ATTACH TABLE ... AS NOT REPLICATED",
			strings.Join(replicated, ", "))
	}
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

// TableEngines returns the engine of each of names that exists in the current
// database (absent from the map means the table does not exist). The engine is the
// system.tables name, e.g. "MergeTree", "ReplicatedAggregatingMergeTree",
// "MaterializedView" — compare it with EngineMatchesMode.
func TableEngines(ctx context.Context, conn driver.Conn, names []string) (map[string]string, error) {
	rows, err := conn.Query(ctx,
		"SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name IN (?)", names)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	engines := make(map[string]string, len(names))

	for rows.Next() {
		var name, engine string

		if err := rows.Scan(&name, &engine); err != nil {
			return nil, err
		}

		engines[name] = engine
	}

	return engines, rows.Err()
}

// orderByRe captures a DDL template's ORDER BY expression, up to the end of its
// line. Anchored at a line start like mergeTreeEngineRe. Only meant for a table
// (Object.Replicated): the SELECT of a materialized view could hold another one.
var orderByRe = regexp.MustCompile(`(?m)^\s*ORDER BY\s+(.+)$`)

// SortingKey returns the sorting key a CREATE TABLE statement declares,
// normalized by NormalizeSortingKey so it compares with the system.tables one
// (TableSortingKeys). Empty when the statement has no ORDER BY at a line start (a
// Null table).
func SortingKey(createDDL string) string {
	m := orderByRe.FindStringSubmatch(createDDL)
	if m == nil {
		return ""
	}

	return NormalizeSortingKey(m[1])
}

// NormalizeSortingKey makes a DDL sorting key and a system.tables one comparable:
// whitespace is dropped and a column tuple loses its parentheses (ClickHouse
// reports ORDER BY (a, b) as "a, b"), so both give "a,b". It only has to be exact
// for the plain column lists the schemas use: a key it fails to normalize
// compares unequal, which callers must treat as the safe case.
func NormalizeSortingKey(key string) string {
	key = strings.Join(strings.Fields(key), "")

	if strings.HasPrefix(key, "(") && strings.HasSuffix(key, ")") && strings.Count(key, "(") == 1 {
		key = key[1 : len(key)-1]
	}

	return key
}

// TableSortingKeys returns the sorting key (system.tables sorting_key, normalized
// by NormalizeSortingKey) of each of names that exists in the current database. A
// table without one (Null, MaterializedView) maps to "".
func TableSortingKeys(ctx context.Context, conn driver.Conn, names []string) (map[string]string, error) {
	rows, err := conn.Query(ctx,
		"SELECT name, sorting_key FROM system.tables WHERE database = currentDatabase() AND name IN (?)", names)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make(map[string]string, len(names))

	for rows.Next() {
		var name, key string

		if err := rows.Scan(&name, &key); err != nil {
			return nil, err
		}

		keys[name] = NormalizeSortingKey(key)
	}

	return keys, rows.Err()
}

// OnCluster returns the " ON CLUSTER <name>" clause (empty for a single node),
// for statements built without a template (e.g. DROP).
func OnCluster(clusterName string) string {
	if clusterName == "" {
		return ""
	}

	return " ON CLUSTER " + clusterName
}

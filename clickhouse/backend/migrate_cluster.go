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

// Single-node → cluster migration: rename each single-node table to
// <table>_pre_cluster, create the replicated one under the original name, and attach
// the source's partitions to it (hardlinks, so no data is copied).
//
// A source whose sorting key differs from the replicated table's cannot be attached
// (ClickHouse: "Tables have different ordering"). That is a tsdb_tags created before
// its ORDER BY gained tags (see clickhouse/tsdb tsdbTagsDDL): its partitions are
// copied with INSERT ... SELECT instead, then detached like an attached one. It is
// the series index, one row per series, so the copy is small next to tsdb_data. A
// copy is idempotent, so the per-partition resumability is kept: re-copying a
// partition whose detach did not happen only adds duplicate rows of the same key,
// which the AggregatingMergeTree merges into one (min/max of equal values), and
// which reads already tolerate (they dedup by id, as for a re-registered series).
//
// The usual recipe (create <table>_replicated, attach, rename onto <table>) is
// unusable here: it renames a REPLICATED table, whose Keeper path embeds the name
// (see clickhouse/ddl). Renaming the single-node table has no such problem.
//
// State is derived from ClickHouse on each call, never stored, so the endpoint
// resumes wherever it stopped (see planMigration).

package backend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/bleemeo/squirreldb/clickhouse/ddl"
	"github.com/bleemeo/squirreldb/clickhouse/routing"
)

// preClusterSuffix names the renamed single-node table, which only exists between
// the rename and the cleanup that drops it.
const preClusterSuffix = "_pre_cluster"

var (
	errNotClustered       = errors.New("cluster_name is not set: nothing to migrate to")
	errTwoDatasets        = errors.New("two datasets for the same table, refusing to guess which one to keep")
	errDataOnSeveralHosts = errors.New("single-node tables found on more than one host, refusing to merge them")
	errReplicatedSource   = errors.New("a _pre_cluster table is replicated, which a migration never produces")
	errNoCluster          = errors.New("cluster_name is unknown to ClickHouse (system.clusters)")
	errNoMacros           = errors.New("the shard and replica macros are not both defined (system.macros)")
	errHostNotInCluster   = errors.New("the host holding the single-node tables is not a member of the cluster")
)

// tableStep is what one table still needs to be a replicated table holding its
// single-node data. A table needing nothing has no step at all, so an empty plan
// means the migration is done.
type tableStep struct {
	Table string `json:"table"`
	// Source is the renamed single-node table the partitions come from, empty when
	// there is no data to move (a table this deployment never had).
	Source string `json:"source,omitempty"`
	Rename bool   `json:"rename"`
	Create bool   `json:"create"`
	// Partitions are Source's active partition ids, in attach order, and Bytes the disk
	// they hold. Empty once every partition has been attached and detached.
	Partitions []string `json:"partitions,omitempty"`
	Bytes      uint64   `json:"bytes,omitempty"`
	// Copy is set when Source's sorting key differs from the replicated table's, so
	// its partitions are copied (INSERT ... SELECT) instead of attached; Rows is the
	// row count of Source's active parts (see planCopies).
	Copy bool   `json:"copy,omitempty"`
	Rows uint64 `json:"rows,omitempty"`
}

// dataTable is where the step's partitions currently live: still under the final
// name when the rename is pending, under Source afterwards.
func (s tableStep) dataTable() string {
	if s.Rename {
		return s.Table
	}

	return s.Source
}

// migrationPlan is the whole work left, as derived from one host.
type migrationPlan struct {
	// Host is the address holding the single-node data — the only host the rename
	// and the partition attaches can run on.
	Host  string      `json:"host"`
	Steps []tableStep `json:"steps"`
	// CleanupPending are the emptied <table>_pre_cluster tables left as a rollback
	// net. Dropping them is opt-in (?cleanup=1).
	CleanupPending []string `json:"cleanup_pending,omitempty"`
}

// empty reports whether nothing is left to migrate. A plan with only a pending
// cleanup is empty: the write path must not be interrupted for a drop.
func (p migrationPlan) empty() bool {
	return len(p.Steps) == 0
}

// planMigration derives, per table, what is left to do from the engines of the
// final and <table>_pre_cluster names on one host. Mixed states are normal — the
// endpoint can be interrupted anywhere and re-called.
//
//	<table>          <table>_pre_cluster   step
//	non-replicated   absent                rename, create, attach
//	absent           non-replicated        create, attach   (interrupted mid-way)
//	replicated       non-replicated        attach           (interrupted mid-way)
//	replicated       absent                nothing (done, or already cleaned up)
//	absent           absent                create           (a table this
//	                                       deployment never had, e.g. added by a
//	                                       newer SquirrelDB)
//	non-replicated   present               error: two datasets
//	any              replicated            error: impossible state
//
//nolint:dupword
func planMigration(objects []ddl.Object, engines map[string]string) ([]tableStep, error) {
	var steps []tableStep

	for _, o := range objects {
		if !o.Replicated {
			// Only a replicable table can hold single-node data to migrate; splitSchema
			// decides what may happen to the rest.
			continue
		}

		source := o.Name + preClusterSuffix
		final, hasFinal := engines[o.Name]
		src, hasSource := engines[source]

		switch {
		case hasSource && ddl.EngineMatchesMode(src, true):
			return nil, fmt.Errorf("%w: %s is %s", errReplicatedSource, source, src)
		case hasFinal && hasSource && !ddl.EngineMatchesMode(final, true):
			return nil, fmt.Errorf("%w: %s (%s) and %s (%s)", errTwoDatasets, o.Name, final, source, src)
		case hasFinal && ddl.EngineMatchesMode(final, true) && !hasSource:
			// Done: replicated, and its source is gone (attached then cleaned up).
		case hasSource:
			// The rename already happened; create what is missing, then attach.
			steps = append(steps, tableStep{
				Table:  o.Name,
				Source: source,
				Create: !hasFinal,
			})
		case hasFinal:
			// The single-node table still holds the data under its final name.
			steps = append(steps, tableStep{Table: o.Name, Source: source, Rename: true, Create: true})
		default:
			// Neither exists: nothing to move, the table just has to be created.
			steps = append(steps, tableStep{Table: o.Name, Create: true})
		}
	}

	// A source with no partition left is a rollback net waiting for ?cleanup=1;
	// scanPartitions moves it out of the steps once it confirms it is empty.
	return steps, nil
}

// planCopies sets Copy on the steps whose source cannot be attached to the
// replicated table because their sorting keys differ. keys are the normalized
// system.tables sorting keys of the host (ddl.TableSortingKeys). The replicated
// table's key is read from ClickHouse once it exists, and parsed from its DDL
// before (so a dry run shows the copy too).
//
// A key that cannot be determined leaves the step an attach: should the keys
// really differ, ClickHouse refuses the ATTACH, which changes nothing and is
// re-callable — never a silent loss.
func planCopies(objects []ddl.Object, steps []tableStep, keys map[string]string) {
	byName := make(map[string]ddl.Object, len(objects))
	for _, o := range objects {
		byName[o.Name] = o
	}

	for i, step := range steps {
		if step.Source == "" {
			continue
		}

		sourceKey := keys[step.dataTable()]
		if sourceKey == "" {
			continue
		}

		var targetKey string

		if !step.Rename && !step.Create {
			targetKey = keys[step.Table]
		} else {
			targetKey = ddl.SortingKey(byName[step.Table].DDL)
		}

		if targetKey == "" {
			continue
		}

		steps[i].Copy = sourceKey != targetKey
	}
}

// findMigrationHost returns the host holding the single-node tables, and the
// engines it reports for the final and _pre_cluster names. It errors when several
// hosts hold single-node data (two datasets cannot be merged) and returns an empty
// address when no host does (nothing to migrate).
func findMigrationHost(
	ctx context.Context, routingConn *routing.Conn, objects []ddl.Object,
) (string, map[string]string, error) {
	var (
		addr    string
		engines map[string]string
	)

	names := migrationTableNames(objects)

	for _, host := range routingConn.Hosts() {
		conn, err := routingConn.OnHost(host.Address)
		if err != nil {
			return "", nil, err
		}

		hostEngines, err := ddl.TableEngines(ctx, conn, names)
		if err != nil {
			// A host we cannot read could be the one holding the data.
			return "", nil, fmt.Errorf("list tables on %s: %w", host.Address, err)
		}

		if !holdsSingleNodeData(objects, hostEngines) {
			continue
		}

		if addr != "" {
			return "", nil, fmt.Errorf("%w: %s and %s", errDataOnSeveralHosts, addr, host.Address)
		}

		addr, engines = host.Address, hostEngines
	}

	return addr, engines, nil
}

// migrationTableNames lists the names planMigration needs the engine of: the
// replicable tables and their _pre_cluster counterparts.
func migrationTableNames(objects []ddl.Object) []string {
	names := make([]string, 0, 2*len(objects))

	for _, o := range objects {
		if o.Replicated {
			names = append(names, o.Name, o.Name+preClusterSuffix)
		}
	}

	return names
}

// holdsSingleNodeData reports whether the host has data to migrate: a replicable
// table that is not replicated, or a leftover _pre_cluster one.
func holdsSingleNodeData(objects []ddl.Object, engines map[string]string) bool {
	for _, o := range objects {
		if !o.Replicated {
			continue
		}

		if _, ok := engines[o.Name+preClusterSuffix]; ok {
			return true
		}

		if engine, ok := engines[o.Name]; ok && !ddl.EngineMatchesMode(engine, true) {
			return true
		}
	}

	return false
}

// preflight checks, before anything is renamed, that the cluster DDL the migration
// is about to run can succeed: an unknown cluster or a missing macro would fail the
// CREATE after the rename, leaving the schema half-migrated.
func (b *Backend) preflight(ctx context.Context, conn driver.Conn) error {
	var clusterNodes, localNodes uint64

	err := conn.QueryRow(ctx,
		"SELECT count(), countIf(is_local) FROM system.clusters WHERE cluster = ?", b.options.ClusterName,
	).Scan(&clusterNodes, &localNodes)
	if err != nil {
		return fmt.Errorf("read system.clusters: %w", err)
	}

	if clusterNodes == 0 {
		return fmt.Errorf("%w: %s", errNoCluster, b.options.ClusterName)
	}

	// ON CLUSTER runs on the cluster's members only, so migrating from a non-member
	// would create the tables everywhere EXCEPT where the data is.
	if localNodes == 0 {
		return fmt.Errorf("%w: %s", errHostNotInCluster, b.options.ClusterName)
	}

	var macros uint64

	err = conn.QueryRow(ctx,
		"SELECT count() FROM system.macros WHERE macro IN ('shard', 'replica')",
	).Scan(&macros)
	if err != nil {
		return fmt.Errorf("read system.macros: %w", err)
	}

	if macros != 2 {
		return errNoMacros
	}

	// The replicated tables are unusable without Keeper; reading its root is the
	// cheapest proof the server can reach it.
	var keeperNodes uint64

	if err := conn.QueryRow(ctx, "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&keeperNodes); err != nil {
		return fmt.Errorf("read Keeper through system.zookeeper (no keeper configured?): %w", err)
	}

	return nil
}

// scanPartitions fills each step's partitions (and the disk and rows they hold)
// from the table currently holding its data, and moves the steps whose source is
// empty — everything attached (or copied) and detached already — to the pending
// cleanup.
func scanPartitions(
	ctx context.Context, conn driver.Conn, plan *migrationPlan,
) error {
	steps := make([]tableStep, 0, len(plan.Steps))

	for _, step := range plan.Steps {
		if step.Source != "" {
			parts, err := activePartitions(ctx, conn, step.dataTable())
			if err != nil {
				return err
			}

			step.Partitions, step.Bytes, step.Rows = parts.ids, parts.bytes, parts.rows

			if len(parts.ids) == 0 && !step.Rename && !step.Create {
				// Nothing left to attach and the table exists: only the emptied source
				// remains, waiting for ?cleanup=1.
				plan.CleanupPending = append(plan.CleanupPending, step.Source)

				continue
			}
		}

		steps = append(steps, step)
	}

	plan.Steps = steps

	return nil
}

// partitionSet is a table's active partitions: their ids, oldest first, and the
// disk and rows of their parts.
type partitionSet struct {
	ids   []string
	bytes uint64
	rows  uint64
}

// activePartitions returns the table's active partitions. An absent table has no
// part, hence no partition.
func activePartitions(ctx context.Context, conn driver.Conn, table string) (partitionSet, error) {
	var set partitionSet

	rows, err := conn.Query(ctx, `
		SELECT partition_id, sum(bytes_on_disk), sum(rows)
		FROM system.parts
		WHERE database = currentDatabase() AND table = ? AND active
		GROUP BY partition_id
		ORDER BY partition_id`, table)
	if err != nil {
		return set, fmt.Errorf("list partitions of %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			partition    string
			bytes, count uint64
		)

		if err := rows.Scan(&partition, &bytes, &count); err != nil {
			return set, fmt.Errorf("scan partitions of %s: %w", table, err)
		}

		set.ids = append(set.ids, partition)
		set.bytes += bytes
		set.rows += count
	}

	return set, rows.Err()
}

// migrateClusterHandler plans (and, unless ?dry_run, performs) the single-node →
// cluster migration.
func (b *Backend) migrateClusterHandler(w http.ResponseWriter, r *http.Request) {
	if b.options.ReadOnly {
		http.Error(w, "read-only mode: not migrating", http.StatusForbidden)

		return
	}

	if b.options.ClusterName == "" {
		http.Error(w, errNotClustered.Error(), http.StatusBadRequest)

		return
	}

	b.l.Lock()
	routingConn := b.routing
	b.l.Unlock()

	if routingConn == nil {
		http.Error(w, ErrNotStarted.Error(), http.StatusServiceUnavailable)

		return
	}

	objects := allSchemaObjects(b.options)

	addr, engines, err := findMigrationHost(r.Context(), routingConn, objects)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)

		return
	}

	if addr == "" {
		fmt.Fprintln(w, "Nothing to migrate: no host holds single-node tables.")

		return
	}

	plan := migrationPlan{Host: addr}

	plan.Steps, err = planMigration(objects, engines)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)

		return
	}

	hostConn, err := routingConn.OnHost(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	if err := b.preflight(r.Context(), hostConn); err != nil {
		http.Error(w, "pre-flight check failed, nothing was changed: "+err.Error(), http.StatusPreconditionFailed)

		return
	}

	keys, err := ddl.TableSortingKeys(r.Context(), hostConn, migrationTableNames(objects))
	if err != nil {
		http.Error(w, "read the sorting keys: "+err.Error(), http.StatusInternalServerError)

		return
	}

	planCopies(objects, plan.Steps, keys)

	if err := scanPartitions(r.Context(), hostConn, &plan); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	writePlan(w, plan, b.options.ClusterName)

	if r.URL.Query().Has("dry_run") {
		fmt.Fprintln(w, "\nDry run: nothing was changed.")

		return
	}

	cleanup := r.URL.Query().Has("cleanup")

	// A plan with nothing to migrate must not go through runMigration: it would drop
	// and re-create the write path — a write outage — for no reason.
	if !plan.empty() {
		if err := b.runMigration(r.Context(), w, hostConn, plan, objects); err != nil {
			fmt.Fprintf(w, "\nMIGRATION FAILED: %s\n\n%s", err, b.migrationFailureHint())

			return
		}

		fmt.Fprintln(w, "\nEvery table is replicated and the write path is back: writes resume, "+
			"the WAL drains on its own.")
	}

	if !cleanup {
		return
	}

	if err := cleanupSources(r.Context(), w, hostConn, plan.sources()); err != nil {
		fmt.Fprintf(w, "\nCLEANUP FAILED: %s\n", err)

		return
	}
}

// writePlan reports the work the plan describes, in the order it happens.
func writePlan(w http.ResponseWriter, plan migrationPlan, clusterName string) {
	fmt.Fprintf(w, "Migration to cluster %s, from host %s.\n\n", clusterName, plan.Host)

	if plan.empty() {
		fmt.Fprintln(w, "Nothing to migrate: every table is already replicated.")
	}

	for _, step := range plan.Steps {
		actions := make([]string, 0, 3)

		if step.Rename {
			actions = append(actions, "rename to "+step.Source)
		}

		if step.Create {
			actions = append(actions, "create replicated")
		}

		switch {
		case len(step.Partitions) == 0:
		case step.Copy:
			actions = append(actions, fmt.Sprintf(
				"copy %d row(s) (%s), sorting key differs from the replicated table, from %d partition(s): %s",
				step.Rows, humanBytes(step.Bytes), len(step.Partitions), strings.Join(step.Partitions, ", ")))
		default:
			actions = append(actions, fmt.Sprintf("attach %d partition(s) (%s): %s",
				len(step.Partitions), humanBytes(step.Bytes), strings.Join(step.Partitions, ", ")))
		}

		fmt.Fprintf(w, "%s: %s\n", step.Table, strings.Join(actions, ", "))
	}

	if len(plan.CleanupPending) > 0 {
		fmt.Fprintf(w, "\nMigrated sources kept as a rollback net: %s.\n"+
			"They share their data with the migrated tables today, but that sharing ends as merges and TTL "+
			"rewrite the tables — expect them to grow into a full duplicate. Drop them with ?cleanup=1 once "+
			"you have checked the migrated data.\n", strings.Join(plan.CleanupPending, ", "))
	}
}

// humanBytes formats a byte count for the migration report.
func humanBytes(bytes uint64) string {
	const unit = 1024

	value, units := float64(bytes), []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}

	i := 0
	for value >= unit && i < len(units)-1 {
		value /= unit
		i++
	}

	return fmt.Sprintf("%.1f %s", value, units[i])
}

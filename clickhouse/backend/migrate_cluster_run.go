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

// The mutating half of the single-node → cluster migration (see migrate_cluster.go
// for the plan it works from).
//
// The write path is dropped FIRST, so the drainer's inserts fail and points stay in
// the WAL, and re-created LAST. A failure therefore leaves writes DOWN on purpose:
// letting them into the new replicated tables would strand them on a rollback.

package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/bleemeo/squirreldb/clickhouse/ddl"
)

// Deadlines for the two statements run on a context detached from the request.
const (
	detachTimeout = time.Minute
	mergesTimeout = 10 * time.Second
)

var (
	errSourceNotEmpty     = errors.New("source table still holds active partitions, refusing to drop it")
	errNoColumns          = errors.New("no column to copy")
	errUnclassifiedObject = errors.New(
		"schema object is neither replicable nor a known droppable one, so the migration cannot know " +
			"whether dropping it loses data (see splitSchema)")
)

// sources lists every <table>_pre_cluster the plan involves: those already emptied
// and those the migration is about to empty. It is the cleanup's work list.
func (p migrationPlan) sources() []string {
	sources := make([]string, 0, len(p.Steps)+len(p.CleanupPending))
	sources = append(sources, p.CleanupPending...)

	for _, step := range p.Steps {
		if step.Source != "" {
			sources = append(sources, step.Source)
		}
	}

	return sources
}

// runMigration performs the plan on the host holding the single-node data.
func (b *Backend) runMigration(
	ctx context.Context, w io.Writer, conn driver.Conn, plan migrationPlan, objects []ddl.Object,
) error {
	writePath, replicable, err := splitSchema(objects)
	if err != nil {
		return err
	}

	progressf(w, "\nDropping the write path: writes to ClickHouse now fail, points stay in the WAL.\n")

	if err := b.dropWritePath(ctx, conn, writePath); err != nil {
		return err
	}

	if err := b.renameSources(ctx, w, conn, plan.Steps); err != nil {
		return err
	}

	progressf(w, "Creating the replicated tables.\n")

	if err := b.createSchema(ctx, conn, replicable); err != nil {
		return err
	}

	if err := b.attachAll(ctx, w, conn, plan.Steps); err != nil {
		return err
	}

	progressf(w, "Re-creating the write path.\n")

	return b.createSchema(ctx, conn, writePath)
}

// splitSchema separates the objects the migration drops and re-creates from the
// replicable tables it migrates, and errors on anything it cannot classify.
func splitSchema(objects []ddl.Object) (writePath, replicable []ddl.Object, err error) {
	// List droppable by name to be sure we don't delete unwanted object in future
	// schema evolution.
	// Currently object is droppable iff ddl.Object.Replicated is false. If this
	// fact become false and an object can't be classified, an error is returned.
	droppableWritePath := []string{"datapoint_input", "mv_datapoint_data", "mv_datapoint_agg_15m"}

	byName := make(map[string]ddl.Object, len(objects))

	for _, o := range objects {
		if o.Replicated {
			replicable = append(replicable, o)

			continue
		}

		if !slices.Contains(droppableWritePath, o.Name) {
			return nil, nil, fmt.Errorf("%w: %s", errUnclassifiedObject, o.Name)
		}

		byName[o.Name] = o
	}

	// In list order, not schema order: this file owns the drop/create order.
	for _, name := range droppableWritePath {
		if o, ok := byName[name]; ok {
			writePath = append(writePath, o)
		}
	}

	return writePath, replicable, nil
}

// dropWritePath drops the droppable objects in splitSchema's order, datapoint_input
// first: it is a Null table, so while it exists without its materialized views every
// INSERT succeeds and discards its points (and the drainer then trims the WAL).
// Dropping it first turns writes into failures the drainer retries.
//
// check_table_dependencies=0 because the views it feeds still exist at that point.
func (b *Backend) dropWritePath(ctx context.Context, conn driver.Conn, writePath []ddl.Object) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"check_table_dependencies": 0,
	}))

	onCluster := ddl.OnCluster(b.options.ClusterName)

	for _, o := range writePath {
		// SYNC because the Atomic database defers a plain DROP, and the re-CREATE at the
		// end of the migration would then hit a table that still exists.
		stmt := "DROP TABLE IF EXISTS " + o.Name + onCluster + " SYNC"
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}

	return nil
}

// renameSources moves the single-node tables out of the way, in ONE statement, and
// without ON CLUSTER since only this host has them. The never-RENAME rule of
// clickhouse/ddl does not apply: these tables have no Keeper path yet.
func (b *Backend) renameSources(ctx context.Context, w io.Writer, conn driver.Conn, steps []tableStep) error {
	pairs := make([]string, 0, len(steps))

	for _, step := range steps {
		if step.Rename {
			pairs = append(pairs, step.Table+" TO "+step.Source)
		}
	}

	if len(pairs) == 0 {
		return nil
	}

	progressf(w, "Renaming %d single-node table(s) out of the way.\n", len(pairs))

	stmt := "RENAME TABLE " + strings.Join(pairs, ", ")
	if err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("%s: %w", stmt, err)
	}

	return nil
}

// createSchema runs the CREATE of each object (ON CLUSTER, so every node gets it),
// turning a lingering replica into the actionable SYSTEM DROP REPLICA hint.
func (b *Backend) createSchema(ctx context.Context, conn driver.Conn, objects []ddl.Object) error {
	err := ddl.CreateTables(ctx, conn, b.logger, objects)

	if _, ok := errors.AsType[*ddl.ReplicaExistsError](err); ok {
		return fmt.Errorf("%w\n\n%s", err, b.dropReplicaRunbook())
	}

	return withSchemaHint(err)
}

// attachAll moves every planned partition to its replicated table — attached, or
// copied for a step with Copy — with merges stopped on the sources so their parts
// cannot change underneath.
func (b *Backend) attachAll(ctx context.Context, w io.Writer, conn driver.Conn, steps []tableStep) error {
	var err error

	stopped := make([]string, 0, len(steps))

	defer func() {
		for _, table := range stopped {
			b.startMerges(ctx, conn, table)
		}
	}()

	for _, step := range steps {
		if len(step.Partitions) == 0 {
			continue
		}

		// The rename already happened, so the data is under Source whatever the step's
		// (now stale) Rename flag says.
		if err := b.stopMerges(ctx, conn, step.Source); err != nil {
			return err
		}

		stopped = append(stopped, step.Source)

		var columns []string

		if step.Copy {
			columns, err = insertableColumns(ctx, conn, step.Source)
			if err != nil {
				return err
			}
		}

		for i, partition := range step.Partitions {
			// The only safe place to stop: everything before is attached AND detached.
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("cancelled after %d/%d partition(s) of %s: %w",
					i, len(step.Partitions), step.Table, err)
			}

			t0 := time.Now()
			verb := "attached"

			if step.Copy {
				verb = "copied"
				err = b.copyPartition(ctx, conn, step, columns, partition)
			} else {
				err = b.attachPartition(ctx, conn, step, partition)
			}

			if err != nil {
				return err
			}

			progressf(w, "%s: %s partition %s (%d/%d) in %s\n",
				step.Table, verb, partition, i+1, len(step.Partitions),
				time.Since(t0).Truncate(time.Millisecond))
		}
	}

	return nil
}

// attachPartition hardlinks one partition of the source into the replicated table,
// then detaches it from the source to record that it is done.
//
// The DETACH runs on a detached context because its precondition is an ATTACH that
// RETURNED: once that holds, the mark must be written even if the caller hung up, or
// a re-call attaches the partition twice. It must NEVER run after a cancelled ATTACH
// (not atomic) — that would drop the parts it never copied.
func (b *Backend) attachPartition(
	ctx context.Context, conn driver.Conn, step tableStep, partition string,
) error {
	attach := "ALTER TABLE " + step.Table + " ATTACH PARTITION ID ? FROM " + step.Source
	if err := conn.Exec(ctx, attach, partition); err != nil {
		return fmt.Errorf("attach partition %s of %s: %w", partition, step.Source, err)
	}

	detachCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachTimeout)
	defer cancel()

	detach := "ALTER TABLE " + step.Source + " DETACH PARTITION ID ?"
	if err := conn.Exec(detachCtx, detach, partition); err != nil {
		return fmt.Errorf("detach the attached partition %s of %s "+
			"(re-run to finish; the partition may end up attached twice, see the runbook): %w",
			partition, step.Source, err)
	}

	return nil
}

// copyPartition copies one partition of the source into the replicated table, for
// a source whose sorting key differs (see migrate_cluster.go), then detaches it from
// the source to record that it is done, like attachPartition.
//
// Unlike an ATTACH, the copy is safe to repeat: a copy interrupted (or whose detach
// failed) is copied again on the next call, and the duplicate rows merge. The
// DETACH still must never follow a failed or cancelled INSERT, which may have
// copied only part of the partition.
func (b *Backend) copyPartition(
	ctx context.Context, conn driver.Conn, step tableStep, columns []string, partition string,
) error {
	list := strings.Join(columns, ", ")

	insert := "INSERT INTO " + step.Table + " (" + list + ") SELECT " + list +
		" FROM " + step.Source + " WHERE _partition_id = ?"
	if err := conn.Exec(ctx, insert, partition); err != nil {
		return fmt.Errorf("copy partition %s of %s: %w", partition, step.Source, err)
	}

	detachCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachTimeout)
	defer cancel()

	detach := "ALTER TABLE " + step.Source + " DETACH PARTITION ID ?"
	if err := conn.Exec(detachCtx, detach, partition); err != nil {
		return fmt.Errorf("detach the copied partition %s of %s (re-run to finish, it copies it again): %w",
			partition, step.Source, err)
	}

	return nil
}

// insertableColumns lists the table's columns an INSERT can write (not
// MATERIALIZED, ALIAS or EPHEMERAL), quoted, in table order: the copy names them
// rather than relying on SELECT * matching the target's column order.
func insertableColumns(ctx context.Context, conn driver.Conn, table string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT name
		FROM system.columns
		WHERE database = currentDatabase() AND table = ?
			AND default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL')
		ORDER BY position`, table)
	if err != nil {
		return nil, fmt.Errorf("list columns of %s: %w", table, err)
	}
	defer rows.Close()

	var columns []string

	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan columns of %s: %w", table, err)
		}

		columns = append(columns, "`"+strings.ReplaceAll(name, "`", "\\`")+"`")
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list columns of %s: %w", table, err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: %s", errNoColumns, table)
	}

	return columns, nil
}

func (b *Backend) stopMerges(ctx context.Context, conn driver.Conn, table string) error {
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
		return fmt.Errorf("stop merges on %s: %w", table, err)
	}

	return nil
}

// startMerges undoes stopMerges on the same host (the stop is per server), on a
// detached context so a failed migration does not leave merges off.
func (b *Backend) startMerges(ctx context.Context, conn driver.Conn, table string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mergesTimeout)
	defer cancel()

	if err := conn.Exec(ctx, "SYSTEM START MERGES "+table); err != nil {
		b.logger.Warn().Err(err).Msgf("could not restart merges on %s (SYSTEM START MERGES %s)", table, table)
	}
}

// cleanupSources drops the emptied <table>_pre_cluster tables (the rollback net),
// re-checking each is empty: an active partition means data nothing attached.
func cleanupSources(ctx context.Context, w io.Writer, conn driver.Conn, sources []string) error {
	for _, source := range sources {
		parts, err := activePartitions(ctx, conn, source)
		if err != nil {
			return err
		}

		if len(parts.ids) > 0 {
			return fmt.Errorf("%w: %s has %d", errSourceNotEmpty, source, len(parts.ids))
		}

		// No ON CLUSTER: the source only ever existed on this host.
		stmt := "DROP TABLE IF EXISTS " + source + " SYNC"
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}

		progressf(w, "dropped %s\n", source)
	}

	return nil
}

// migrationFailureHint tells the operator what a failed migration left, and the two
// ways out.
func (b *Backend) migrationFailureHint() string {
	return fmt.Sprintf(`Writes to ClickHouse are still DOWN (the write path is dropped); points pile up in the WAL.
That is deliberate — it keeps a rollback possible — so decide quickly, either:
  - finish: call this endpoint again, it resumes where it stopped (or POST
    /debug/clickhouse/create_tables to bring writes back right now, on the (partially migrated) replicated tables);
  - roll back, per table: DROP TABLE <replicated_table> ON CLUSTER %s SYNC, then
    ALTER TABLE <table>%s ATTACH PARTITION ID '<id>' for each partition under its detached/
    directory, RENAME TABLE <table>%s TO <table>, and restart SquirrelDB with no cluster_name
    followed by POST /debug/clickhouse/create_tables.
`, b.options.ClusterName, preClusterSuffix, preClusterSuffix)
}

// progressf reports one step, flushed so a long migration is followed live.
func progressf(w io.Writer, format string, args ...any) {
	fmt.Fprintf(w, format, args...)

	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

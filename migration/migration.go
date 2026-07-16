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

// Package migration copies raw time series data out of one SquirrelDB backend
// (the source, Config.Backend) into another (the target, Config.ExtraWriteBackend)
// — the Cassandra→ClickHouse migration. It is a plain export→import loop over the
// source's export.RawExporter, checkpointing its resumable position into the
// target's state store so it can resume after an interruption. Live writes are
// meant to be dual-written concurrently (the daemon in dual-write mode); this
// tool only backfills history.
package migration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/export"
	"github.com/bleemeo/squirreldb/types"
	"golang.org/x/time/rate"

	"github.com/rs/zerolog"
)

// progressStateKey is the state-store key under which the full-migration
// checkpoint (the exporter's serialized resume position) is stored.
const progressStateKey = "migration_progress"

// dualWriteBoundaryKey is the state-store key under which the backfill's upper
// bound (the dual-write start, as Unix milliseconds) is persisted. It is written
// once at the start of a fresh migration and reused on every resume, so the whole
// backfill migrates against one consistent boundary. See Options.DualWriteStart.
const dualWriteBoundaryKey = "migration_dual_write_start"

// progressLogInterval is how often overall progress is logged.
const progressLogInterval = 15 * time.Second

// checkpointInterval is how often the resume position is persisted while
// running. Saving every chunk would hammer the state table; the final position
// is always saved on exit (see the deferred save in Run), so this interval only
// bounds how much is re-done after a hard crash (not a graceful stop).
const checkpointInterval = 30 * time.Second

// checkpointSaveTimeout bounds the final checkpoint save on exit, which runs on
// a fresh context (the run context is usually already cancelled by then).
const checkpointSaveTimeout = 10 * time.Second

// defaultBatchPoints is the batch size (in points) used when Options.BatchPoints
// is unset. It trades this process's memory for fewer, larger store inserts.
const defaultBatchPoints = 1_000_000

var (
	errSourceNotExporter = errors.New("source backend does not support raw export")
	errTargetNotState    = errors.New("migration target is not a State backend")
	errTargetNotImporter = errors.New("migration target does not support raw import")
)

// Options configures a migration run.
type Options struct {
	// RepairFrom and RepairTo bound the migrated point timestamps (inclusive) for a
	// REPAIR run — e.g. re-copying a range that a best-effort dual-write dropped.
	// Setting either makes this a repair run: it starts fresh over that window and
	// does not read or write the full-migration checkpoint or the dual-write
	// boundary. Both zero = a normal (resumable) backfill.
	RepairFrom time.Time
	RepairTo   time.Time
	// DualWriteStart is the backfill's upper bound: only points strictly before it
	// are migrated, because the daemon's dual-write owns everything at or after it
	// (set it to when the last mono-write instance became dual-write). On a fresh
	// migration, zero means "now()"; the chosen value is persisted and reused on
	// every resume — including of the current, still-growing shard — so the whole
	// backfill uses one consistent boundary. Ignored on a resume.
	DualWriteStart time.Time
	// ImportMutableLabels, when set, copies the mutable labels (and the telemetry
	// cluster ID) from the source into the target at the end of a graceful run,
	// overwriting the target's — see importMutableLabels.
	ImportMutableLabels bool
	// BatchPoints is the number of points accumulated (across chunks) before one
	// store insert. Bigger = fewer, larger inserts (ClickHouse makes one part per
	// insert and dislikes many small ones) at the cost of more memory in this
	// process. It is a point count (not a chunk count) so memory stays bounded
	// regardless of how large individual series are.
	BatchPoints int
	// ThrottleWrites caps the rate of store inserts per second (0 = unlimited).
	// The relevant limit for the ClickHouse target is inserts/s (it must keep up
	// merging parts), not points/s — so the operator throttles writes and sizes
	// throughput with BatchPoints.
	ThrottleWrites float64
}

// Run performs the migration described by opts, reading from sq.Backend and
// writing to sq.MigrateToBackend. It returns nil on completion or graceful
// interruption (ctx cancelled), with the checkpoint persisted.
func Run( //nolint:maintidx
	ctx context.Context,
	shutdownCtx context.Context,
	sq *daemon.SquirrelDB,
	opts Options,
	logger zerolog.Logger,
) error {
	exporter, target, stateStore, err := resolve(ctx, sq)
	if err != nil {
		return err
	}

	targetTSDB, err := target.TSDB(ctx)
	if err != nil {
		return fmt.Errorf("open target TSDB: %w", err)
	}

	// The migrator writes directly to the target (bypassing the WAL): the source
	// is the durable copy and the checkpoint covers resume, so the WAL's buffering
	// only throttles the backfill.
	importer, ok := targetTSDB.(export.RawImporter)
	if !ok {
		return errTargetNotImporter
	}

	repair := !opts.RepairFrom.IsZero() || !opts.RepairTo.IsZero()

	from, err := resumePosition(ctx, exporter, stateStore, repair, logger)
	if err != nil {
		return err
	}

	// A repair run migrates exactly its [RepairFrom, RepairTo] window and touches
	// no checkpoint. A normal backfill runs from the beginning of time up to the
	// dual-write boundary (the upper bound at/after which dual-write is
	// authoritative), resolved once and persisted so resumes stay consistent.
	exportStart, exportEnd := opts.RepairFrom, opts.RepairTo

	if !repair {
		exportEnd, err = resolveDualWriteBoundary(ctx, stateStore, opts.DualWriteStart, logger)
		if err != nil {
			return err
		}
	}

	it, err := exporter.ExportRawData(ctx, export.ExportOptions{Start: exportStart, End: exportEnd, From: from})
	if err != nil {
		return fmt.Errorf("start export: %w", err)
	}

	defer it.Close()

	// Burst of up to 10 seconds' worth of inserts, but at least 1: a burst of 0 makes
	// the limiter reject every Wait(n=1), so max(1, ...) is required.
	pacer := rate.NewLimiter(rate.Limit(opts.ThrottleWrites), max(1, int(opts.ThrottleWrites*10)))

	if opts.ThrottleWrites <= 0 {
		// Special case of no rate limit
		pacer = rate.NewLimiter(rate.Inf, 1)
	}

	batchLimit := opts.BatchPoints
	if batchLimit <= 0 {
		batchLimit = defaultBatchPoints
	}

	var (
		chunkCount, writeCount, points int64
		latest                         export.Position // last FLUSHED chunk's resume position (checkpoint)
		lastRead                       export.Position // last read chunk's position (progress log only)
		unsaved                        bool            // latest has not been persisted yet
		lastLogPoints                  int64           // points count at the previous progress log (for the instant rate)
		batch                          []export.ExportChunk
		batchPoints                    int
		dataMinTS, dataMaxTS           int64 // exact timestamp span of the migrated points (final log)
		sawPoints                      bool  // whether dataMin/MaxTS have been set
	)

	// flush imports the accumulated batch as ONE store insert (few, large parts)
	// and advances the resume point to the batch's last chunk.
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := importer.ImportRawData(shutdownCtx, batch); err != nil {
			return fmt.Errorf("import batch of %d series: %w", len(batch), err)
		}

		latest = batch[len(batch)-1].Position
		unsaved = true

		// Release the flushed chunks' point slices before reusing the backing array.
		// batch[:0] alone keeps every ExportChunk (hence its Points) reachable in the
		// array's tail until a later append overwrites that slot — and the array grows
		// to the largest batch ever seen, so smaller later batches leave stale chunks
		// from several past generations pinned there, holding multiples of one batch's
		// points live. Zeroing the whole capacity drops those references (no realloc).
		clear(batch[:cap(batch)])
		batch = batch[:0]
		batchPoints = 0
		writeCount++

		return nil
	}

	// Always persist the final position on the way out, including after a graceful
	// cancel — so a graceful stop loses nothing. The run context is usually already
	// cancelled here, hence a fresh (detached) context.
	defer func() {
		if repair || !unsaved {
			return
		}

		saveCtx, cancel := context.WithTimeout(shutdownCtx, checkpointSaveTimeout)
		defer cancel()

		if err := saveCheckpoint(saveCtx, stateStore, latest); err != nil {
			logger.Error().Err(err).Msg("save final migration checkpoint")
		}
	}()

	migrationStart := time.Now()
	lastLog := migrationStart
	lastCheckpoint := migrationStart

	// withStats attaches the run totals to a log event — counts, wall-clock
	// duration, overall throughput, and the exact time span migrated — so every
	// end-of-run log (complete / interrupted / failed) reports the same stats.
	withStats := func(event *zerolog.Event) *zerolog.Event {
		d := time.Since(migrationStart)

		event = event.
			Int64("chunk", chunkCount).
			Int64("writes", writeCount).
			Int64("points", points).
			Str("duration", d.Round(time.Second).String()).
			Int64("points_per_second", ratePerSecond(points, d)).
			Bool("repair", repair)

		if sawPoints {
			event = event.
				Str("data_from", time.UnixMilli(dataMinTS).Format(time.RFC3339)).
				Str("data_to", time.UnixMilli(dataMaxTS).Format(time.RFC3339))
		}

		return event
	}

	// fail logs the run stats at error level and returns err, so a failed migration
	// still reports how far it got. The checkpoint (deferred) holds the last flushed
	// position, so a re-run resumes from there regardless of where the error hit.
	fail := func(err error) error {
		withStats(logger.Error().Err(err)).Msg("migration failed")

		return err
	}

	for it.Next(ctx) {
		chunk := it.Chunk()

		// Flush BEFORE adding when this chunk would push the batch over the limit,
		// so a batch (hence one store insert) never exceeds batchLimit points. The
		// insert block size is set to the batch's point count (see ImportRawData),
		// so an over-limit batch would be split into several parts.
		if len(batch) > 0 && batchPoints+len(chunk.Points) > batchLimit {
			if err := pacer.Wait(ctx); err != nil {
				if ctx.Err() != nil {
					break
				}

				return fmt.Errorf("pacer.Wait failed: %w", err)
			}

			if err := flush(); err != nil {
				return fail(err)
			}
		}

		batch = append(batch, chunk)
		batchPoints += len(chunk.Points)
		chunkCount++
		points += int64(len(chunk.Points))
		lastRead = chunk.Position

		// Track the exact timestamp span of the migrated data for the final log. A
		// chunk's points are time-sorted, so its extremes are the two ends — take
		// min/max of them to stay correct regardless of the sort direction.
		if n := len(chunk.Points); n > 0 {
			lo := min(chunk.Points[0].Timestamp, chunk.Points[n-1].Timestamp)
			hi := max(chunk.Points[0].Timestamp, chunk.Points[n-1].Timestamp)

			if !sawPoints || lo < dataMinTS {
				dataMinTS = lo
			}

			if !sawPoints || hi > dataMaxTS {
				dataMaxTS = hi
			}

			sawPoints = true
		}

		// A single chunk at/over the limit is flushed on its own right away.
		if batchPoints >= batchLimit {
			if err := pacer.Wait(ctx); err != nil {
				if ctx.Err() != nil {
					break
				}

				return fmt.Errorf("pacer.Wait failed: %w", err)
			}

			if err := flush(); err != nil {
				return fail(err)
			}
		}

		// Persist periodically (best-effort): the deferred save is authoritative,
		// this only bounds hard-crash re-work. Only a flushed position is saved
		// (guarded by unsaved, set in flush). A write error is logged, not fatal.
		if !repair && unsaved && time.Since(lastCheckpoint) >= checkpointInterval {
			if err := saveCheckpoint(shutdownCtx, stateStore, latest); err != nil {
				logger.Warn().Err(err).Msg("save migration checkpoint (will retry)")
			} else {
				unsaved = false
			}

			lastCheckpoint = time.Now()
		}

		if elapsed := time.Since(lastLog); elapsed >= progressLogInterval {
			// points_per_second is the INSTANT rate since the previous log (not the
			// global average). migrated_back_to marches from now toward the oldest
			// data (export is newest-first), so an operator who knows when their data
			// starts can gauge progress. It is formatted explicitly because the global
			// zerolog time format is unix-ms (unreadable for a data timestamp).
			logger.Info().
				Int64("chunk", chunkCount).
				Int64("writes", writeCount).
				Int64("points", points).
				Int64("points_per_second", int64(float64(points-lastLogPoints)/elapsed.Seconds())).
				Str("migrated_back_to", lastRead.MigratedBackTo().Format(time.DateOnly)).
				Msg("migration in progress")

			lastLog = time.Now()
			lastLogPoints = points
		}
	}

	// Flush the final partial batch (also on a graceful cancel — shutdownCtx is
	// still alive, so nothing accumulated is lost).
	if err := flush(); err != nil {
		return fail(err)
	}

	// Tell a graceful end (export exhausted, or ctx cancelled by a first Ctrl-C)
	// from a real export error: the metadata import and the interrupted/complete
	// logs below only apply to a graceful end.
	interrupted := false

	if err := it.Err(); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return fail(fmt.Errorf("export: %w", err))
		}

		interrupted = true
	}

	// Import the mutable labels (and telemetry cluster ID) at the end of a graceful
	// run — on completion OR a first Ctrl-C, hence shutdownCtx (which outlives the
	// run ctx). It overwrites the target's, so an operator who first ran migrate
	// with writes still flowing can re-run it with mutable-label writes stopped: the
	// raw backfill resumes to a no-op and this re-imports an exact copy.
	if opts.ImportMutableLabels {
		if err := importMutableLabels(shutdownCtx, sq, logger); err != nil {
			return fail(fmt.Errorf("import mutable labels: %w", err))
		}
	}

	if interrupted {
		withStats(logger.Info()).Msg("migration interrupted; progress is checkpointed, re-run to resume")
	} else {
		withStats(logger.Info()).Msg("migration complete")
	}

	return nil
}

// ratePerSecond returns count per second over d, guarding a non-positive duration
// (a sub-tick run) so it never divides by zero.
func ratePerSecond(count int64, d time.Duration) int64 {
	if d <= 0 {
		return 0
	}

	return int64(float64(count) / d.Seconds())
}

type StateStoreBackend interface {
	StateStore(ctx context.Context) (types.State, error)
}

// resolve validates and returns the source exporter, the target backend and its
// state store.
func resolve(
	ctx context.Context,
	sq *daemon.SquirrelDB,
) (export.RawExporter, daemon.Backend, types.State, error) {
	source, err := sq.Backend(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open source backend: %w", err)
	}

	exporter, ok := source.(export.RawExporter)
	if !ok {
		return nil, nil, nil, errSourceNotExporter
	}

	target, err := sq.MigrateToBackend(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open target backend: %w", err)
	}

	stBackend, ok := target.(StateStoreBackend)
	if !ok {
		return nil, nil, nil, errTargetNotState
	}

	stateStore, err := stBackend.StateStore(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open target state store: %w", err)
	}

	return exporter, target, stateStore, nil
}

// resumePosition reads the checkpoint (full migration only) and turns it into an
// exporter resume position. Repair runs start fresh.
func resumePosition(
	ctx context.Context,
	exporter export.RawExporter,
	stateStore types.State,
	repair bool,
	logger zerolog.Logger,
) (export.Position, error) {
	if repair {
		return nil, nil //nolint:nilnil // a nil position means "start from the beginning"; not an error
	}

	var raw []byte

	found, err := stateStore.Read(ctx, progressStateKey, &raw)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}

	if !found {
		return nil, nil //nolint:nilnil // no checkpoint yet means "start from the beginning"; not an error
	}

	from, err := exporter.ParsePosition(raw)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint %q: %w", raw, err)
	}

	logger.Info().
		Str("resuming_from", from.MigratedBackTo().Format(time.RFC3339)).
		Msg("resuming migration from the saved checkpoint")

	return from, nil
}

func saveCheckpoint(ctx context.Context, stateStore types.State, pos export.Position) error {
	text, err := pos.MarshalText()
	if err != nil {
		return fmt.Errorf("serialize checkpoint: %w", err)
	}

	if err := stateStore.Write(ctx, progressStateKey, string(text)); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}

// resolveDualWriteBoundary returns the backfill's upper bound (see
// Options.DualWriteStart). On the first run it picks the boundary — the given
// flag value, or now() if unset — and persists it. On a resume it returns the
// persisted value and ignores the flag (warning if one was passed), so the whole
// backfill migrates against one consistent boundary.
func resolveDualWriteBoundary(
	ctx context.Context,
	stateStore types.State,
	flagValue time.Time,
	logger zerolog.Logger,
) (time.Time, error) {
	var storedUnixMs int64

	found, err := stateStore.Read(ctx, dualWriteBoundaryKey, &storedUnixMs)
	if err != nil {
		return time.Time{}, fmt.Errorf("read dual-write boundary: %w", err)
	}

	if found {
		bound := time.UnixMilli(storedUnixMs)

		if !flagValue.IsZero() && !flagValue.Equal(bound) {
			logger.Warn().
				Str("ignored", flagValue.Format(time.RFC3339)).
				Str("using", bound.Format(time.RFC3339)).
				Msg("--dual-write-start ignored: resuming a migration, keeping the boundary chosen at its start")
		}

		return bound, nil
	}

	bound := flagValue
	if bound.IsZero() {
		bound = time.Now()
	}

	if err := stateStore.Write(ctx, dualWriteBoundaryKey, bound.UnixMilli()); err != nil {
		return time.Time{}, fmt.Errorf("persist dual-write boundary: %w", err)
	}

	logger.Info().
		Str("dual_write_start", bound.Format(time.RFC3339)).
		Msg("backfill will migrate points before this instant (dual-write owns the rest)")

	return bound, nil
}

// importMutableLabels copies the mutable labels from the source into the target,
// overwriting the target's (mutable.Provider.Import drops existing entries first).
// It then carries over the telemetry cluster ID (but not node IDs, which would
// clash if the source cluster keeps running). Used for the migration's metadata,
// which the raw-data backfill does not touch.
func importMutableLabels(ctx context.Context, sq *daemon.SquirrelDB, logger zerolog.Logger) error {
	source, err := sq.Backend(ctx)
	if err != nil {
		return fmt.Errorf("open source backend: %w", err)
	}

	target, err := sq.MigrateToBackend(ctx)
	if err != nil {
		return fmt.Errorf("open target backend: %w", err)
	}

	srcProvider, err := source.MutableProvider(ctx)
	if err != nil {
		return fmt.Errorf("source mutable provider: %w", err)
	}

	tgtProvider, err := target.MutableProvider(ctx)
	if err != nil {
		return fmt.Errorf("target mutable provider: %w", err)
	}

	var buf bytes.Buffer

	if err := srcProvider.Dump(ctx, &buf); err != nil {
		return fmt.Errorf("dump source mutable labels: %w", err)
	}

	// Import drops the target's existing labels first, so this is an overwrite.
	if err := tgtProvider.Import(ctx, &buf, io.Discard, false); err != nil {
		return fmt.Errorf("import mutable labels into target: %w", err)
	}

	if err := copyTelemetryClusterID(ctx, source, target, logger); err != nil {
		return err
	}

	logger.Info().Msg("imported mutable labels and telemetry cluster ID into the target")

	return nil
}

// copyTelemetryClusterID carries the source's telemetry cluster ID into the
// target, so the cluster keeps its identity across the cutover. Node IDs are left
// alone on purpose: copying them would make a still-running source cluster and the
// target share IDs and clash. The target store must support setting the cluster ID
// otherwise this is a no-op with a warning.
func copyTelemetryClusterID(ctx context.Context, source, target daemon.Backend, logger zerolog.Logger) error {
	tgtTelemetry, err := target.TelemetryStore(ctx)
	if err != nil {
		return fmt.Errorf("target telemetry store: %w", err)
	}

	setter, ok := tgtTelemetry.(interface {
		SetClusterID(ctx context.Context, id string) error
	})
	if !ok {
		logger.Warn().Msg("target telemetry store cannot set the cluster ID; skipping cluster-ID import")

		return nil
	}

	srcTelemetry, err := source.TelemetryStore(ctx)
	if err != nil {
		return fmt.Errorf("source telemetry store: %w", err)
	}

	id, err := srcTelemetry.ClusterID(ctx)
	if err != nil {
		return fmt.Errorf("read source cluster ID: %w", err)
	}

	if err := setter.SetClusterID(ctx, id); err != nil {
		return fmt.Errorf("set target cluster ID: %w", err)
	}

	return nil
}

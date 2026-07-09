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

// Package export defines a backend-agnostic way to stream a TSDB's raw data out,
// one series-chunk at a time, with a resumable position. It exists so callers —
// the Cassandra→ClickHouse migrator, or a plain data exporter — do not depend on
// a specific backend's internals: a migration is just an export piped into an
// import. A backend (currently the Cassandra backend) implements RawExporter.
package export

import (
	"context"
	"encoding"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

// RawExporter streams raw time series data out of a backend.
type RawExporter interface {
	// ExportRawData returns an iterator over raw series-chunks, newest-first. See
	// ExportOptions for the time filter and resume point.
	ExportRawData(ctx context.Context, opts ExportOptions) (RawDataIterator, error)
	// ParsePosition reconstructs a Position previously serialized with its
	// MarshalText (e.g. a resume cursor read back from a state store) so it can be
	// passed as ExportOptions.From. Only positions produced by this exporter are
	// accepted.
	ParsePosition(text []byte) (Position, error)
}

// RawImporter writes raw series-chunks into a backend — the write side that
// mirrors RawExporter, so a migration is an export piped into an import.
//
// It bypasses any live-write buffering (e.g. the ClickHouse WAL): a migration's
// durable copy is the source, and the checkpoint covers resume, so the WAL's
// durability/HA role is unnecessary and only throttles the backfill. The import
// writes directly and synchronously to the store.
type RawImporter interface {
	// ImportRawData writes a batch of series-chunks (each chunk's Labels, Points
	// and WriteTTL; Positions are ignored) as ONE store write. Callers batch many
	// chunks per call so the store gets few, large writes — column stores like
	// ClickHouse create one part per insert and choke on many small ones. Each
	// chunk's time range is recorded so a chunk of the same series in another
	// window still widens that series' indexed range.
	//
	// The call consumes the batch: an implementation may take over each chunk's
	// Points (clearing chunks[i].Points) to release the source as it writes, so
	// callers must not read chunks' Points afterwards.
	ImportRawData(ctx context.Context, chunks []ExportChunk) error
}

// Position is an opaque, serializable resume cursor produced by an exporter. The
// caller only persists it (via MarshalText) and replays it (via the exporter's
// ParsePosition); its concrete shape is the producing backend's business.
type Position interface {
	encoding.TextMarshaler
	// MigratedBackTo is the oldest instant back to which the export is complete —
	// everything more recent than it has been fully exported — for progress
	// reporting. Export is newest-first, so it moves backward toward the oldest
	// data. It is the far (older) edge of the completed range, not the position
	// currently being worked on, so it never overstates progress.
	MigratedBackTo() time.Time
}

// ExportOptions parameterizes an export.
type ExportOptions struct {
	// Start and End bound the exported point timestamps (inclusive). The zero
	// time means unbounded on that side. Used both for a full export (unbounded)
	// and to repair a specific range (e.g. a best-effort dual-write drop window).
	Start time.Time
	End   time.Time
	// From resumes the export strictly after this position (the Position of the
	// last chunk successfully imported). Nil starts from the beginning (the newest
	// data).
	From Position
}

// ExportChunk is every point of one series within one time bucket.
type ExportChunk struct {
	Labels labels.Labels
	Points []types.MetricPoint
	// WriteTTL is the retention the data was written with, relative to each
	// point's timestamp — i.e. a point expires at (its timestamp + WriteTTL). The
	// importer applies it the same way (e.g. ClickHouse row_ttl_days), so the
	// migrated point expires when the source would have expired it, independently
	// of when the migration runs. Zero means "no explicit TTL — use the
	// destination default".
	WriteTTL time.Duration
	// Position is the resume cursor AFTER this chunk: persist it once the chunk is
	// imported, and pass it back as ExportOptions.From to resume here.
	Position Position
}

// RawDataIterator streams ExportChunks. Usage:
//
//	for it.Next(ctx) {
//		chunk := it.Chunk()
//		// import chunk, then persist chunk.Position
//	}
//	if err := it.Err(); err != nil { ... }
//	it.Close()
type RawDataIterator interface {
	// Next advances to the next chunk, returning false when the export is done or
	// on error (check Err). It does backend I/O, hence the context.
	Next(ctx context.Context) bool
	// Chunk returns the current chunk; valid only after Next returned true.
	Chunk() ExportChunk
	// Err returns the first error encountered, if any.
	Err() error
	// Close releases any resources held by the iterator.
	Close() error
}

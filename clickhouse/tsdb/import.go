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

package tsdb

import (
	"context"
	"time"

	"github.com/bleemeo/squirreldb/export"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var _ export.RawImporter = (*ClickHouseTSDB)(nil)

// importFlushPoints is the point count after which ImportRawData streams an
// intermediate block within the insert (see writeSeries). It bounds clickhouse-go's
// in-memory columnar copy without changing the number of parts (all blocks stay in
// one insert, squashed server-side). This is the knob to tune against a real
// ClickHouse — larger = fewer, bigger blocks (more memory, less overhead).
const importFlushPoints = 500_000

// ImportRawData writes a batch of series-chunks directly to ClickHouse in ONE
// insert, bypassing the WAL (see export.RawImporter). The migration source is
// the durable copy and the checkpoint covers resume, so the WAL's buffering is
// unnecessary and only throttles the backfill; this writes straight to
// datapoint_input + tsdb_tags.
//
// It raises max_insert_block_size (per-insert, so nothing global/table changes)
// to the batch's point count: the ARRAY JOIN in the datapoint_input→tsdb_data MV
// produces one output row per point, and ClickHouse cuts a new part every
// max_insert_block_size rows — so with the default (~1M) a batch of N points
// makes ~N/1M parts. Making the block cover the whole batch yields ~one part per
// insert (per partition) instead. The caller keeps a batch ≤ its --batch-size,
// which therefore also bounds this block size (memory vs. parts trade-off).
//
// Unlike the live drain path it registers the series in tsdb_tags on EVERY call
// (writeAllRegistrations, not the seen-cache registerSeries): the same series is
// imported once per time-shard, and each registration must widen the series'
// indexed min/max range — the AggregatingMergeTree keeps the overall extent — so
// a time-filtered read still finds the series across every migrated shard.
func (c *ClickHouseTSDB) ImportRawData(ctx context.Context, chunks []export.ExportChunk) error {
	// Each chunk is already one series' points, so build the seriesBatch list
	// directly instead of going through groupBySeries — that would copy every
	// point into freshly concatenated slices (a needless ~16 B/point). We take
	// over chunk.Points (moving it, not copying) so writeSeries can release each
	// series' points as clickhouse-go copies them into its column buffers, instead
	// of keeping the whole source batch live alongside the full columnar copy for
	// the length of the insert. This consumes the input: the caller's chunk.Points
	// are cleared (the migration loop discards the batch right after this returns).
	// The key is left empty: it only feeds the live drain's seen-cache, and
	// writeAllRegistrations (the import registrar) never reads it. A series repeated
	// within one batch yields two rows, which is correct — the MV and the
	// AggregatingMergeTree coalesce them downstream.
	series := make([]*seriesBatch, 0, len(chunks))

	var points uint64

	for i := range chunks {
		if len(chunks[i].Points) == 0 {
			continue
		}

		points += uint64(len(chunks[i].Points))

		series = append(series, &seriesBatch{
			labels: chunks[i].Labels,
			ttl:    int64(chunks[i].WriteTTL / time.Second),
			points: chunks[i].Points,
		})

		chunks[i].Points = nil
	}

	if points == 0 {
		return nil
	}

	// Coalesce the whole batch into as few parts as possible (ideally one per
	// touched partition). The datapoint_input→tsdb_data MV emits one row per point
	// (ARRAY JOIN); its output-block size — hence the part size — is governed by
	// the *_for_materialized_views squash thresholds, NOT by max_insert_block_size
	// (which only bounds the final write). We raise all three to cover the batch so
	// the MV output is squashed into a single block; the MergeTree then still cuts
	// one part per partition the batch spans. Set per-insert only (nothing global).
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_insert_block_size":                              points,
		"min_insert_block_size_rows_for_materialized_views":  points,
		"min_insert_block_size_bytes_for_materialized_views": points * 64, // generous byte ceiling (row ≈ 45 B)
	}))

	return c.writeSeries(ctx, series, c.writeAllRegistrations, importFlushPoints)
}

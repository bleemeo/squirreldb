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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/index"
	"github.com/bleemeo/squirreldb/cassandra/tsdb"
	"github.com/bleemeo/squirreldb/export"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

// The Cassandra backend is a raw-data exporter (used by the Cassandra→ClickHouse
// migration). It streams one series-chunk at a time, newest time-shard first and
// ascending series id within a shard, wrapping the migration-read accessors.
var _ export.RawExporter = (*Backend)(nil)

var (
	errNotCassandraTSDB = errors.New("backend TSDB is not a Cassandra TSDB")
	errForeignPosition  = errors.New("export position was not produced by the Cassandra exporter")
)

// exportPosition is the Cassandra exporter's concrete export.Position: the time
// shard (its start) and the int64 series id of the last chunk produced. It is
// opaque to the caller, which only persists MarshalText and replays it via
// ParsePosition.
type exportPosition struct {
	Shard    time.Time `json:"shard"`
	SeriesID int64     `json:"series_id"`
}

// MigratedBackTo is the far (older) edge of the shard this position is in — i.e.
// its start plus the shard width. When the export reaches a shard it is still
// working through it, so only data newer than the shard's end is guaranteed
// exported; returning the shard's end (not its start) is what keeps progress from
// being overstated by one shard.
func (p *exportPosition) MigratedBackTo() time.Time {
	return p.Shard.Add(index.MigrationShardWidth)
}

// MarshalText serializes the position (JSON). It marshals a plain alias so it
// does not recurse through this MarshalText.
func (p *exportPosition) MarshalText() ([]byte, error) {
	type plain exportPosition

	return json.Marshal((*plain)(p))
}

// ParsePosition reconstructs a position serialized by exportPosition.MarshalText.
func (b *Backend) ParsePosition(text []byte) (export.Position, error) {
	var p exportPosition

	if err := json.Unmarshal(text, &p); err != nil {
		return nil, err
	}

	return &p, nil
}

// ExportRawData returns an iterator over the raw data, newest-first. It builds
// the TSDB/index if needed, then plans the shard list (descending, filtered by
// the time window and the resume point).
func (b *Backend) ExportRawData(ctx context.Context, opts export.ExportOptions) (export.RawDataIterator, error) {
	tsdbIface, err := b.TSDB(ctx)
	if err != nil {
		return nil, err
	}

	cassTSDB, ok := tsdbIface.(*tsdb.CassandraTSDB)
	if !ok {
		return nil, errNotCassandraTSDB
	}

	idx := b.migrationIndex()
	if idx == nil {
		return nil, ErrNotStarted
	}

	var from *exportPosition

	if opts.From != nil {
		p, ok := opts.From.(*exportPosition)
		if !ok {
			return nil, errForeignPosition
		}

		from = p
	}

	startMs := int64(math.MinInt64)
	if !opts.Start.IsZero() {
		startMs = opts.Start.UnixMilli()
	}

	endMs := int64(math.MaxInt64)
	if !opts.End.IsZero() {
		endMs = opts.End.UnixMilli()
	}

	ascShards, err := idx.MigrationShards(ctx)
	if err != nil {
		return nil, err
	}

	// Descending (newest-first), keeping only shards that overlap [start, end] and
	// — when resuming — are not newer than the resume shard (those are done).
	now := time.Now()
	shards := make([]int32, 0, len(ascShards))

	var skippedDead int

	for _, shard := range slices.Backward(ascShards) {
		shardStart, shardEnd := index.ShardTimeRange(shard)

		if shardStart.UnixMilli() > endMs || shardEnd.UnixMilli()-1 < startMs {
			continue
		}

		if from != nil && shardStart.After(from.Shard) {
			continue
		}

		// Skip dead shards: a zero (pre-expiration-feature) or past index-expiration
		// means the raw data is already gone — the index expiration is set to outlive
		// the data — so the shard would yield only empty reads. Skipping them here (vs.
		// discovering the emptiness one series at a time) avoids hammering Cassandra
		// with tens of thousands of point-less reads that, producing no writes, are not
		// throttled by the write pacer.
		expiration, err := idx.MigrationShardExpiration(ctx, shard)
		if err != nil {
			return nil, fmt.Errorf("shard %s expiration: %w", shardStart.Format(time.DateOnly), err)
		}

		// An expiration == zero occur on shard created before 25.01.31.100908 (commit f38b938).
		// This commit introduced a bug in expirationShardLabel serialization:
		//   - shard created before use `{__shard|expiration="N"}`
		//   - shard created after use `{"__shard|expiration"="N"}` (key is quoted)
		//   - Due to this change, the metric that was used to store the shard expiration get lost,
		//     and we return zero.
		//
		// The migration ignore those shard because:
		//   - Processing them will significantly slow down migration and hammer Cassandra for unless query
		//     - Production had ~57 shard in this situation
		//     - Without this, migration would try to access every metrics for each shard, resulting in thousand of
		//       TSDB requests which all will return 0 points
		//     - And because the throttle is only applied when on write (so when point are returned), this won't be
		//       throttled
		//   - All known deployment use this version from the beginning or for more than highest TTL
		if expiration.IsZero() || !expiration.After(now) {
			skippedDead++

			continue
		}

		shards = append(shards, shard)
	}

	b.logger.Info().
		Int("shards", len(shards)).
		Int("skipped_dead_shards", skippedDead).
		Msg("planned migration export shards")

	return &cassandraRawIterator{
		index:   idx,
		tsdb:    cassTSDB,
		from:    from,
		startMs: startMs,
		endMs:   endMs,
		shards:  shards,
	}, nil
}

// cassandraRawIterator streams ExportChunks over a planned shard list. Per shard
// it resolves every series' labels in one batch, then yields the raw points of
// each series (skipping series with no point in the window).
type cassandraRawIterator struct {
	index   *index.CassandraIndex
	tsdb    *tsdb.CassandraTSDB
	from    *exportPosition // resume point; nil = from the beginning
	startMs int64
	endMs   int64

	shards   []int32 // descending; remaining shards to load
	shardIdx int

	// current shard state
	shardStart   time.Time
	fromMs       int64 // read window clamped to [start,end] ∩ shard
	toMs         int64
	seriesIDs    []types.MetricID
	seriesLabels []labels.Labels
	seriesIdx    int

	current export.ExportChunk
	err     error
}

func (it *cassandraRawIterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}

	for {
		if ctx.Err() != nil {
			it.err = ctx.Err()

			return false
		}

		// Yield the next non-empty series of the current shard.
		for it.seriesIdx < len(it.seriesIDs) {
			id := it.seriesIDs[it.seriesIdx]
			lbls := it.seriesLabels[it.seriesIdx]
			it.seriesIdx++

			points, ttlSeconds, err := it.tsdb.MigrationReadRaw(ctx, id, it.fromMs, it.toMs)
			if err != nil {
				it.err = err

				return false
			}

			if len(points) == 0 {
				continue
			}

			it.current = export.ExportChunk{
				Labels:   lbls,
				Points:   points,
				WriteTTL: reconstructWriteTTL(ttlSeconds, points, time.Now()),
				Position: &exportPosition{Shard: it.shardStart, SeriesID: int64(id)},
			}

			return true
		}

		// Current shard exhausted: load the next one, or finish.
		if !it.loadNextShard(ctx) {
			return false
		}
	}
}

// loadNextShard advances to the next shard with at least one series to export,
// resolving its series ids (applying the resume filter) and their labels. It
// returns false when no shard is left or on error (it.err set).
func (it *cassandraRawIterator) loadNextShard(ctx context.Context) bool {
	for it.shardIdx < len(it.shards) {
		shard := it.shards[it.shardIdx]
		it.shardIdx++

		shardStart, shardEnd := index.ShardTimeRange(shard)

		fromMs := max(shardStart.UnixMilli(), it.startMs)
		toMs := min(shardEnd.UnixMilli()-1, it.endMs)

		ids, err := it.index.MigrationSeriesInShard(ctx, shard)
		if err != nil {
			it.err = err

			return false
		}

		// Resume: in the shard we stopped in, keep only series after the last one done.
		if it.from != nil && shardStart.Equal(it.from.Shard) {
			ids = idsAfter(ids, types.MetricID(it.from.SeriesID))
		}

		if len(ids) == 0 {
			continue
		}

		lbls, err := it.index.MigrationLabels(ctx, ids)
		if err != nil {
			it.err = err

			return false
		}

		it.shardStart = shardStart
		it.fromMs = fromMs
		it.toMs = toMs
		it.seriesIDs = ids
		it.seriesLabels = lbls
		it.seriesIdx = 0

		return true
	}

	return false
}

func (it *cassandraRawIterator) Chunk() export.ExportChunk {
	return it.current
}

func (it *cassandraRawIterator) Err() error {
	return it.err
}

func (it *cassandraRawIterator) Close() error {
	return nil
}

// reconstructWriteTTL turns Cassandra's remaining TTL (seconds left from now for
// the chunk's rows) into the TTL the data was written with, relative to the point
// timestamp: original retention ≈ remaining + age, where age is measured from the
// newest point (closest to when the row was written). This is ~constant per
// series, so the destination's per-metric TTL stays low-cardinality. A
// non-positive remaining (no TTL on the source) maps to 0 = "destination default".
func reconstructWriteTTL(ttlSeconds int64, points []types.MetricPoint, now time.Time) time.Duration {
	if ttlSeconds <= 0 || len(points) == 0 {
		return 0
	}

	// MigrationReadRaw returns points newest-first (mergePoints sorts descending), so
	// points[0] is the newest — the row closest to when it was written.
	newest := points[0].Timestamp

	age := max(now.Sub(time.UnixMilli(newest)), 0)

	return time.Duration(ttlSeconds)*time.Second + age
}

// idsAfter returns the suffix of the ascending ids slice strictly greater than
// after.
func idsAfter(ids []types.MetricID, after types.MetricID) []types.MetricID {
	i := sort.Search(len(ids), func(i int) bool { return ids[i] > after })

	return ids[i:]
}

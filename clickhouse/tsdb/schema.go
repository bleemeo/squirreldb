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
	"math"
	"time"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"
)

// tsdbDataDDL is the table holding the raw points.
//
// row_ttl_days carries a per-metric TTL (in days), always set by the write path
// (mv_datapoint_data). The TTL clause below expires each row on its own
// row_ttl_days, so there is no hardcoded/global TTL value in the schema.
//
// tenant is carried on every point row so the read path can filter it (see
// pointsQuery). The series id already includes the tenant (sipHash128), so two
// tenants normally have distinct ids; the tenant column is defence in depth,
// confining the blast radius of any id collision (adversarial or accidental) to
// a single tenant instead of leaking one tenant's points into another's query.
//
// tenant LEADS the ORDER BY (like tsdb_tags): almost every read is tenant-scoped
// (WHERE tenant = ?), so a leading tenant prunes the primary index to one
// tenant's range. timestamp stays LAST so the Delta codec still sees the
// increasing per-series timestamps within an id (a (tenant, id) order without it
// would scatter timestamps and wreck compression).
//
// insert_hour is the wall-clock time of the INSERT batch that produced the row,
// truncated to the hour. It serves only as a read-side tiebreaker so that when two rows
// share the same (id, timestamp) with different values (a client re-writing a
// corrected value) the read keeps the most recently written one (last-write-wins).
// The only assumption of this system, is that corrected values aren't sent
// within the same hour. You need to way at least one hour.
//
// The truncation to the hour (rather than the minute or 10s) is an empirical tradeoff:
//   - 10 seconds is quiet good... IF you send point also every 10 seconds, because then
//     the delta codec always see the same delta of 10 seconds.
//   - 1 minutes isn't good with a 10 seconds metric period, because the delta codec will
//     now see 5 times 0 seconds of delta, then 60 seconds which compressed poorly.
//   - 1 hour hope to be a good tradeoff, covering any metric resolution.
//
// PARTITION BY is a 7-day interval anchored on the Unix epoch (in UTC), NOT
// toStartOfWeek: the migration reads Cassandra shard by shard, and those shards
// are 7-day windows anchored on the epoch too (see cassandra/index shardForTime).
// toStartOfWeek anchors on Sunday, so an epoch-aligned shard straddled two
// calendar-week partitions — one migration insert then wrote one part per
// partition. Matching the anchor makes a shard fall in a single partition, so a
// migration insert produces a single part. Granularity (≈7 days) is unchanged
// for the live workload (TTL is per-row, pruning is by timestamp either way).
const tsdbDataDDL = `
CREATE TABLE IF NOT EXISTS tsdb_data
(
    id UUID,
    tenant LowCardinality(String) CODEC(ZSTD(1)),
    timestamp DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    value Float64 CODEC(ZSTD(1)),
    row_ttl_days LowCardinality(UInt16) CODEC(ZSTD(1)),
    insert_hour DateTime CODEC(Delta(4), ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toStartOfInterval(timestamp, INTERVAL 7 DAY, 'UTC')
ORDER BY (tenant, id, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(row_ttl_days)
SETTINGS allow_suspicious_low_cardinality_types = 1`

// tsdbTagsDDL holds the series (id <-> labels) index. The series id is
// sipHash128(metric_name, tenant, tags): it includes the tenant so two tenants
// with otherwise-identical labels get distinct ids. No tenant label name appears
// in the schema — the write path splits the tenant out (see seriesToInput).
//
// The registration path (eager_register.go) writes this table directly (no MV).
// max_expiration is the latest point expiration (point timestamp + per-metric
// TTL), aggregated by max: the index row is dropped (TTL max_expiration) once all
// its points have expired, with each metric's own TTL — no hardcoded value. It is
// rounded up to the day (few distinct values -> better compression) and includes
// a margin covering the re-register gap so the index never expires before its
// data. min_time/max_time bound the series activity (used to filter label
// queries); max_time carries the same gap margin.
const tsdbTagsDDL = `
CREATE TABLE IF NOT EXISTS tsdb_tags
(
    id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, tenant, tags)),
    metric_name LowCardinality(String),
    tenant LowCardinality(String) CODEC(ZSTD(1)),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))),
    max_expiration SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY (tenant, metric_name)
ORDER BY (tenant, metric_name, id)
TTL max_expiration`

// SQL write path: points are inserted into the Null-engine datapoint_input
// table, and a materialized view fans them out into tsdb_data, computing the
// series id as reinterpretAsUUID(sipHash128(metric_name, tenant, tags)).
//
// One input row carries a whole series' points as parallel timestamps/values
// arrays, NOT one row per point: the series identity (metric_name, tenant, tags)
// then appears once per series instead of once per point. clickhouse-go buffers
// the whole INSERT block uncompressed client-side (the tag values are plain
// String — not dictionary-encoded like the LowCardinality columns), so a per-point
// layout duplicated those bytes thousands of times during a backlog catch-up,
// driving the process to hundreds of MiB. The MV re-expands the arrays with
// ARRAY JOIN, so tsdb_data still gets one row per point.
//
// Column types match tsdb_tags so the id computed here equals the one computed by
// the tsdb_tags DEFAULT for an eager registration of the same series.
// datapoint_input is a Null-engine table (a stateless insert entry point): it is
// created ON CLUSTER so it exists on every node, but is never Replicated.
const datapointInputDDL = `
CREATE TABLE IF NOT EXISTS datapoint_input
(
    metric_name LowCardinality(String),
    tenant LowCardinality(String),
    tags Map(LowCardinality(String), String),
    timestamps Array(DateTime64(3)),
    values Array(Float64),
    row_ttl_days UInt16,
    insert_hour DateTime
)
ENGINE = Null`

// mvDatapointDataDDL expands each series row into one tsdb_data row per point.
// The parallel ARRAY JOIN zips timestamps and values element-wise (they always
// have the same length, see sqlWriter.Write). The id is computed once per input
// row (per series) and repeated across its points by the join.
//
// insert_hour is passed through from datapoint_input: the write path stamps one
// hour-truncated time per batch. now() here would re-evaluate per inserted block,
// not once per batch (the import splits a batch into several blocks).
const mvDatapointDataDDL = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_datapoint_data TO tsdb_data
(
    id UUID,
    tenant LowCardinality(String),
    timestamp DateTime64(3),
    value Float64,
    row_ttl_days UInt16,
    insert_hour DateTime
) AS
SELECT
    reinterpretAsUUID(sipHash128(metric_name, tenant, tags)) AS id,
    tenant,
    timestamp,
    value,
    row_ttl_days,
    insert_hour
FROM datapoint_input
ARRAY JOIN
    timestamps AS timestamp,
    values AS value`

// SchemaObjects returns the ClickHouse objects owned by the TSDB store. The
// templates are single-node SQL; ddl.ForCluster derives the cluster form (ON
// CLUSTER + Replicated* engine), asserted in TestSchemaObjectsCluster — do NOT
// hand-copy the cluster DDL. Exposed so the backend can create the whole schema
// up front and derive the expected-table set, independently of whether the store
// is ever instantiated.
func SchemaObjects(clusterName string) []ddl.Object {
	return []ddl.Object{
		{Name: "tsdb_data", DDL: ddl.ForCluster(tsdbDataDDL, clusterName), Replicated: true},
		{Name: "tsdb_tags", DDL: ddl.ForCluster(tsdbTagsDDL, clusterName), Replicated: true},
		{Name: "datapoint_input", DDL: ddl.ForCluster(datapointInputDDL, clusterName)},
		{Name: "mv_datapoint_data", DDL: ddl.ForCluster(mvDatapointDataDDL, clusterName)},
	}
}

// storeMinTimeMillis and storeMaxTimeMillis are the bounds, in Unix
// milliseconds, of the timestamps the store can represent: the tsdb_data and
// tsdb_tags timestamp columns are DateTime64(3), whose range is
// [1900-01-01, 2299-12-31]. A point's timestamp is an int64 of milliseconds,
// which spans a much wider range, so the boundaries matter:
//   - reads clamp their query window to this range (clampStoreTime), since a
//     window edge slightly outside it changes nothing;
//   - writes reject an out-of-range timestamp (see appender.Append) rather than
//     clamp it, which would silently store the point at the wrong time.
//
//nolint:gochecknoglobals
var (
	storeMinTimeMillis = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	storeMaxTimeMillis = time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC).UnixMilli()
)

// ttlDaysOf converts a TTL duration to whole days, clamped to [0, MaxUint16] — the
// width of the row_ttl_days column. There is no "unlimited" retention: the TTL
// clause is unconditional, so 0 days expires rows immediately; use a large value
// (the ~179-year cap, i.e. MaxUint16 days) for near-permanent retention.
func ttlDaysOf(d time.Duration) uint16 {
	days := math.Ceil(float64(d) / float64(24*time.Hour))

	switch {
	case days < 0:
		return 0
	case days > math.MaxUint16:
		return math.MaxUint16
	default:
		return uint16(days)
	}
}

// ttlDays returns the configured global TTL in whole days (clamped, see ttlDaysOf).
func (c *ClickHouseTSDB) ttlDays() uint16 {
	return ttlDaysOf(c.opts.DefaultTimeToLive)
}

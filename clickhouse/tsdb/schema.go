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
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"
)

// tsdbDataDDL is the table holding the raw points.
//
// row_ttl_days carries a per-metric TTL (in days), always set by the write path
// (mv_datapoint_data). {{TTL}} is the TTL clause based on it (removed when no TTL
// is configured); there is no hardcoded TTL value in the schema.
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
// inserted_at is the wall-clock time of the INSERT batch that produced the row
// (set by the MV, see mvDatapointDataDDL). It is NOT part of the ORDER BY — it
// serves only as a read-side tiebreaker so that when two rows share the same
// (id, timestamp) with different values (a client re-writing a corrected value,
// e.g. bleemeo-forecast overwriting its 1h overlap every 24h), the read keeps
// the most recently written one (last-write-wins, see pointsQuery /
// dedupSortedPoints). Existing rows predating this column read back as epoch 0
// (DEFAULT), so any later write wins.
//
// It uses Delta(8),ZSTD like timestamp (NOT plain ZSTD, which cost ~40x more):
// rows are stored ordered by (tenant, id, timestamp), and within a series run
// inserted_at is near-constant — a point is inserted shortly after its own
// timestamp (real-time scrape) or a whole batch shares one now64() (forecast /
// backfill of future timestamps). So the intra-series deltas are tiny, exactly
// like timestamp; only the per-series boundary produces a large (negligible) one.
const tsdbDataDDL = `
CREATE TABLE IF NOT EXISTS tsdb_data{{ON_CLUSTER}}
(
    id UUID,
    tenant LowCardinality(String) CODEC(ZSTD(1)),
    timestamp DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    value Float64 CODEC(ZSTD(1)),
    row_ttl_days LowCardinality(UInt16) CODEC(ZSTD(1)),
    inserted_at DateTime64(3) DEFAULT toDateTime64(0, 3) CODEC(Delta(8), ZSTD(1))
)
ENGINE = {{ENGINE}}
PARTITION BY toStartOfWeek(timestamp)
ORDER BY (tenant, id, timestamp){{TTL}}
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
CREATE TABLE IF NOT EXISTS tsdb_tags{{ON_CLUSTER}}
(
    id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, tenant, tags)),
    metric_name LowCardinality(String),
    tenant LowCardinality(String) CODEC(ZSTD(1)),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))),
    max_expiration SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = {{ENGINE}}
PRIMARY KEY (tenant, metric_name)
ORDER BY (tenant, metric_name, id){{TTL}}`

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
CREATE TABLE IF NOT EXISTS datapoint_input{{ON_CLUSTER}}
(
    metric_name LowCardinality(String),
    tenant LowCardinality(String),
    tags Map(LowCardinality(String), String),
    timestamps Array(DateTime64(3)),
    values Array(Float64),
    row_ttl_days UInt16
)
ENGINE = Null`

// mvDatapointDataDDL expands each series row into one tsdb_data row per point.
// The parallel ARRAY JOIN zips timestamps and values element-wise (they always
// have the same length, see sqlWriter.Write). The id is computed once per input
// row (per series) and repeated across its points by the join.
//
// inserted_at is now64(3), evaluated once per INSERT (now() is constant within a
// query) so every point of a batch shares the same value. It is the read-side
// last-write-wins tiebreaker for duplicate (id, timestamp) rows (see tsdbDataDDL
// and pointsQuery).
const mvDatapointDataDDL = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_datapoint_data{{ON_CLUSTER}} TO tsdb_data
(
    id UUID,
    tenant LowCardinality(String),
    timestamp DateTime64(3),
    value Float64,
    row_ttl_days UInt16,
    inserted_at DateTime64(3)
) AS
SELECT
    reinterpretAsUUID(sipHash128(metric_name, tenant, tags)) AS id,
    tenant,
    timestamp,
    value,
    row_ttl_days,
    now64(3) AS inserted_at
FROM datapoint_input
ARRAY JOIN
    timestamps AS timestamp,
    values AS value`

// SchemaObjects returns the ClickHouse objects owned by the TSDB store, engine
// and per-row TTL filled in. Exposed so the backend can create the whole schema
// up front and derive the expected-table set, independently of whether the store
// is ever instantiated.
func SchemaObjects(clusterName string, defaultTTL time.Duration) []ddl.Object {
	days := ttlDaysOf(defaultTTL)

	return []ddl.Object{
		{Name: "tsdb_data", DDL: ddl.Apply(tsdbDataStatement(clusterName, days), clusterName), Replicated: true},
		{Name: "tsdb_tags", DDL: ddl.Apply(tsdbTagsStatement(clusterName, days), clusterName), Replicated: true},
		{Name: "datapoint_input", DDL: ddl.Apply(datapointInputDDL, clusterName)},
		{Name: "mv_datapoint_data", DDL: ddl.Apply(mvDatapointDataDDL, clusterName)},
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

// ttlDaysOf converts a TTL duration to whole days (0 when no TTL is configured).
func ttlDaysOf(d time.Duration) int {
	return max(int(d/(24*time.Hour)), 0)
}

// ttlDays returns the configured global TTL in days (0 when no TTL is configured).
func (c *ClickHouseTSDB) ttlDays() int {
	return ttlDaysOf(c.opts.DefaultTimeToLive)
}

// tsdbDataStatement fills the tsdb_data {{ENGINE}} and {{TTL}} placeholders. The
// per-row TTL clause is based on row_ttl_days (each metric keeps its own TTL)
// and omitted when no TTL is configured.
func tsdbDataStatement(clusterName string, ttlDays int) string {
	var ttlClause string

	if ttlDays > 0 {
		ttlClause = "\nTTL toDateTime(timestamp) + toIntervalDay(row_ttl_days)"
	}

	stmt := strings.Replace(tsdbDataDDL, "{{TTL}}", ttlClause, 1)

	return strings.Replace(stmt, "{{ENGINE}}", ddl.ReplicatedEngine(clusterName, "tsdb_data", "MergeTree"), 1)
}

// tsdbTagsStatement fills the tsdb_tags {{ENGINE}} and {{TTL}} placeholders: the
// index row expires once its latest point has expired (TTL max_expiration, the
// per-metric expiration baked in by the write path). The TTL clause is omitted
// when no TTL is configured.
func tsdbTagsStatement(clusterName string, ttlDays int) string {
	var ttlClause string

	if ttlDays > 0 {
		ttlClause = "\nTTL max_expiration"
	}

	stmt := strings.Replace(tsdbTagsDDL, "{{TTL}}", ttlClause, 1)

	return strings.Replace(stmt, "{{ENGINE}}", ddl.ReplicatedEngine(clusterName, "tsdb_tags", "AggregatingMergeTree"), 1)
}

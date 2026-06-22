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
	"fmt"
	"strings"
	"time"
)

// tsdbDataDDL is the table holding the raw points.
//
// row_ttl_days carries a per-metric TTL (in days), always set by the write path
// (mv_datapoint_data). {{TTL}} is the TTL clause based on it (removed when no TTL
// is configured); there is no hardcoded TTL value in the schema.
const tsdbDataDDL = `
CREATE TABLE IF NOT EXISTS tsdb_data
(
    id UUID,
    timestamp DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    value Float64 CODEC(ZSTD(1)),
    row_ttl_days LowCardinality(UInt16) CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toStartOfWeek(timestamp)
ORDER BY (id, timestamp){{TTL}}
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
ORDER BY (tenant, metric_name, id){{TTL}}`

// SQL write path: points are inserted into the Null-engine datapoint_input
// table, and two materialized views fan them out into the data and tags tables,
// computing the series id as reinterpretAsUUID(sipHash128(metric_name, tenant, tags)).
//
// Column types match tsdb_tags so the id computed here equals the one computed by
// the tsdb_tags DEFAULT for an eager registration of the same series.
const datapointInputDDL = `
CREATE TABLE IF NOT EXISTS datapoint_input
(
    metric_name LowCardinality(String),
    tenant LowCardinality(String),
    tags Map(LowCardinality(String), String),
    timestamp DateTime64(3),
    value Float64,
    row_ttl_days UInt16
)
ENGINE = Null`

const mvDatapointDataDDL = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_datapoint_data TO tsdb_data
(
    id UUID,
    timestamp DateTime64(3),
    value Float64,
    row_ttl_days UInt16
) AS
SELECT
    reinterpretAsUUID(sipHash128(metric_name, tenant, tags)) AS id,
    timestamp,
    value,
    row_ttl_days
FROM datapoint_input`

// createSchema creates the ClickHouse tables if they don't exist yet.
//
// tsdb_data is fed by points (datapoint_input + mv_datapoint_data); tsdb_tags is
// written directly by the registration path (see eager_register.go), not by a
// materialized view.
//
// Creation is idempotent (CREATE TABLE IF NOT EXISTS), so the TTL clause is
// only applied when a table is created for the first time. Changing the
// configured TTL afterwards does not alter an existing table.
func (c *ClickHouseTSDB) createSchema(ctx context.Context) error {
	statements := []struct {
		name string
		ddl  string
	}{
		{"tsdb_data", c.tsdbDataStatement()},
		{"tsdb_tags", c.tsdbTagsStatement()},
		{"datapoint_input", datapointInputDDL},
		{"mv_datapoint_data", mvDatapointDataDDL},
	}

	for _, stmt := range statements {
		if err := c.conn.Exec(ctx, stmt.ddl); err != nil {
			return fmt.Errorf("create table %s: %w", stmt.name, err)
		}

		c.logger.Debug().Msgf("ClickHouse table %s is ready", stmt.name)
	}

	return nil
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

// ttlDays returns the configured global TTL in days (0 when no TTL is configured).
func (c *ClickHouseTSDB) ttlDays() int {
	return max(int(c.opts.DefaultTimeToLive/(24*time.Hour)), 0)
}

// tsdbDataStatement fills the tsdb_data {{TTL}} placeholder with a per-row TTL
// clause based on row_ttl_days (each metric keeps its own TTL). The clause is
// omitted when no TTL is configured.
func (c *ClickHouseTSDB) tsdbDataStatement() string {
	var ttlClause string

	if c.ttlDays() > 0 {
		ttlClause = "\nTTL toDateTime(timestamp) + toIntervalDay(row_ttl_days)"
	}

	return strings.Replace(tsdbDataDDL, "{{TTL}}", ttlClause, 1)
}

// tsdbTagsStatement fills the tsdb_tags DDL: the index row expires once its
// latest point has expired (TTL max_expiration, the per-metric expiration baked
// in by the write path). The TTL clause is omitted when no TTL is configured.
func (c *ClickHouseTSDB) tsdbTagsStatement() string {
	var ttlClause string

	if c.ttlDays() > 0 {
		ttlClause = "\nTTL max_expiration"
	}

	return strings.Replace(tsdbTagsDDL, "{{TTL}}", ttlClause, 1)
}

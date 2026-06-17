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
	"strconv"
	"strings"
	"time"
)

// tsdbDataDDL is the table holding the raw points.
//
// row_ttl_days carries a per-metric TTL (in days): the SQL write path sets it
// per metric, while the remote_write path leaves it at its DEFAULT (the global
// TTL), since the Prometheus wire format has no slot for a per-row value. The
// {{TTL_DAYS}} placeholder is the global TTL in days (the column DEFAULT) and
// {{TTL}} is the TTL clause (removed when no TTL is configured).
const tsdbDataDDL = `
CREATE TABLE IF NOT EXISTS tsdb_data
(
    id UUID,
    timestamp DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    value Float64 CODEC(ZSTD(1)),
    row_ttl_days LowCardinality(UInt16) DEFAULT {{TTL_DAYS}} CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toStartOfWeek(timestamp)
ORDER BY (id, timestamp){{TTL}}
SETTINGS allow_suspicious_low_cardinality_types = 1`

// tsdbTagsDDL holds the series (id <-> labels) index. The TTL drops a series
// row once it has received no point for the configured duration, based on
// max_time (assumeNotNull: rows are always created with a non-null max_time by
// the TimeSeries engine on write).
const tsdbTagsDDL = `
CREATE TABLE IF NOT EXISTS tsdb_tags
(
    id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    metric_name LowCardinality(String),
    tenant LowCardinality(String) DEFAULT all_tags['__bleemeo_account__'] CODEC(ZSTD(1)),
    tags Map(LowCardinality(String), String) DEFAULT mapFilter((k, v) -> k != '__bleemeo_account__', all_tags),
    all_tags Map(String, String) EPHEMERAL,
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY (tenant, metric_name)
ORDER BY (tenant, metric_name, id){{TTL}}`

// SQL write path: points are inserted into the Null-engine datapoint_input
// table, and two materialized views fan them out into the data and tags tables,
// computing the series id as reinterpretAsUUID(sipHash128(metric_name, all_tags)).
//
// The tenant label name is hardcoded to '__bleemeo_account__' (it is extracted
// from all_tags into the tenant column by mv_datapoint_tags).
const datapointInputDDL = `
CREATE TABLE IF NOT EXISTS datapoint_input
(
    metric_name String,
    all_tags Map(String, String),
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
    reinterpretAsUUID(sipHash128(metric_name, all_tags)) AS id,
    timestamp,
    value,
    row_ttl_days
FROM datapoint_input`

const mvDatapointTagsDDL = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_datapoint_tags TO tsdb_tags
(
    id UUID,
    metric_name LowCardinality(String),
    tenant LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) AS
SELECT
    reinterpretAsUUID(sipHash128(metric_name, all_tags)) AS id,
    metric_name,
    all_tags['__bleemeo_account__'] AS tenant,
    mapFilter((k, v) -> k != '__bleemeo_account__', all_tags) AS tags,
    timestamp AS min_time,
    timestamp AS max_time
FROM datapoint_input`

// createSchema creates the ClickHouse tables if they don't exist yet.
//
// The data and tags tables are created before the materialized views that write
// into them.
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
		{"tsdb_tags", c.withTTL(tsdbTagsDDL, "toDateTime(assumeNotNull(max_time))")},
		{"datapoint_input", datapointInputDDL},
		{"mv_datapoint_data", mvDatapointDataDDL},
		{"mv_datapoint_tags", mvDatapointTagsDDL},
	}

	for _, stmt := range statements {
		if err := c.conn.Exec(ctx, stmt.ddl); err != nil {
			return fmt.Errorf("create table %s: %w", stmt.name, err)
		}

		c.logger.Debug().Msgf("ClickHouse table %s is ready", stmt.name)
	}

	return nil
}

// ttlDays returns the configured global TTL in days (0 when no TTL is configured).
func (c *ClickHouseTSDB) ttlDays() int {
	return max(int(c.opts.DefaultTimeToLive/(24*time.Hour)), 0)
}

// tsdbDataStatement fills the tsdb_data DDL: {{TTL_DAYS}} is the per-row default
// (the global TTL), and {{TTL}} is a TTL clause based on row_ttl_days so each
// metric can have its own TTL (set by the SQL write path).
func (c *ClickHouseTSDB) tsdbDataStatement() string {
	days := c.ttlDays()

	ddl := strings.Replace(tsdbDataDDL, "{{TTL_DAYS}}", strconv.Itoa(days), 1)

	var ttlClause string
	if days > 0 {
		ttlClause = "\nTTL toDateTime(timestamp) + toIntervalDay(row_ttl_days)"
	}

	return strings.Replace(ddl, "{{TTL}}", ttlClause, 1)
}

// withTTL replaces the {{TTL}} placeholder in a DDL with a global TTL clause
// based on the given DateTime expression. When no TTL is configured (less than
// a day), the placeholder is removed.
func (c *ClickHouseTSDB) withTTL(ddl, timeExpr string) string {
	var ttlClause string

	if days := c.ttlDays(); days > 0 {
		ttlClause = fmt.Sprintf("\nTTL %s + toIntervalDay(%d)", timeExpr, days)
	}

	return strings.Replace(ddl, "{{TTL}}", ttlClause, 1)
}

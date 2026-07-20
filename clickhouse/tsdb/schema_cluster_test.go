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

//nolint:lll
package tsdb

import (
	"testing"
)

const tsdbDataDDLCluster = `
CREATE TABLE IF NOT EXISTS tsdb_data ON CLUSTER test_cluster
(
    id UUID,
    tenant LowCardinality(String) CODEC(ZSTD(1)),
    timestamp DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    value Float64 CODEC(ZSTD(1)),
    row_ttl_days LowCardinality(UInt16) CODEC(ZSTD(1)),
    insert_hour DateTime CODEC(Delta(4), ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/tsdb_data', '{replica}')
PARTITION BY toStartOfInterval(timestamp, INTERVAL 7 DAY, 'UTC')
ORDER BY (tenant, id, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(row_ttl_days)
SETTINGS allow_suspicious_low_cardinality_types = 1`

const tsdbTagsDDLCluster = `
CREATE TABLE IF NOT EXISTS tsdb_tags ON CLUSTER test_cluster
(
    id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, tenant, tags)),
    metric_name LowCardinality(String),
    tenant LowCardinality(String) CODEC(ZSTD(1)),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))),
    max_expiration SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{database}/tsdb_tags', '{replica}')
PRIMARY KEY (tenant, metric_name)
ORDER BY (tenant, metric_name, id)
TTL max_expiration`

const datapointInputDDLCluster = `
CREATE TABLE IF NOT EXISTS datapoint_input ON CLUSTER test_cluster
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

const mvDatapointDataDDLCluster = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_datapoint_data ON CLUSTER test_cluster TO tsdb_data
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

const tsdbDataAgg15mDDLCluster = `
CREATE TABLE IF NOT EXISTS tsdb_data_agg_15m ON CLUSTER test_cluster
(
    id UUID,
    tenant LowCardinality(String) CODEC(ZSTD(1)),
    bucket DateTime CODEC(Delta(4), ZSTD(1)),
    min SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    max SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    sum SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    count SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1)),
    row_ttl_days SimpleAggregateFunction(max, UInt16) CODEC(ZSTD(1))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{database}/tsdb_data_agg_15m', '{replica}')
PARTITION BY toStartOfInterval(bucket, INTERVAL 7 DAY, 'UTC')
ORDER BY (tenant, id, bucket)
TTL bucket + toIntervalDay(row_ttl_days)`

const mvDatapointAgg15mDDLCluster = `
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_datapoint_agg_15m ON CLUSTER test_cluster TO tsdb_data_agg_15m AS
SELECT
    reinterpretAsUUID(sipHash128(metric_name, tenant, tags)) AS id,
    tenant,
    toDateTime(toStartOfInterval(timestamp - toIntervalMillisecond(1), INTERVAL 15 MINUTE) + toIntervalMinute(15)) AS bucket,
    min(value) AS min,
    max(value) AS max,
    sum(value) AS sum,
    count() AS count,
    max(row_ttl_days) AS row_ttl_days
FROM datapoint_input
ARRAY JOIN
    timestamps AS timestamp,
    values AS value
WHERE NOT isNaN(value)
GROUP BY id, tenant, bucket`

// TestSchemaObjectsCluster locks the cluster DDL that ddl.ForCluster derives from
// the single-node templates. This IS the reviewed cluster schema: any change to a
// template (or the transform) must be reflected here on purpose — a schema change
// is never casual, it needs a migration.
func TestSchemaObjectsCluster(t *testing.T) {
	t.Parallel()

	type expect struct {
		clusterDDL string
		singleNode string // the template verbatim (single-node form)
		replicated bool
	}

	want := map[string]expect{
		"tsdb_data":            {tsdbDataDDLCluster, tsdbDataDDL, true},
		"tsdb_tags":            {tsdbTagsDDLCluster, tsdbTagsDDL, true},
		"datapoint_input":      {datapointInputDDLCluster, datapointInputDDL, false},
		"mv_datapoint_data":    {mvDatapointDataDDLCluster, mvDatapointDataDDL, false},
		"tsdb_data_agg_15m":    {tsdbDataAgg15mDDLCluster, tsdbDataAgg15mDDL, true},
		"mv_datapoint_agg_15m": {mvDatapointAgg15mDDLCluster, mvDatapointAgg15mDDL, false},
	}

	seen := map[string]bool{}

	for _, o := range SchemaObjects("test_cluster") {
		seen[o.Name] = true

		exp, ok := want[o.Name]
		if !ok {
			t.Errorf("unexpected object %q from SchemaObjects", o.Name)

			continue
		}

		if o.DDL != exp.clusterDDL {
			t.Errorf("%s cluster DDL =\n%q\nwant\n%q", o.Name, o.DDL, exp.clusterDDL)
		}

		if o.Replicated != exp.replicated {
			t.Errorf("%s Replicated = %v, want %v", o.Name, o.Replicated, exp.replicated)
		}
	}

	for name := range want {
		if !seen[name] {
			t.Errorf("missing object %q from SchemaObjects", name)
		}
	}

	// Single-node mode uses each template verbatim (valid single-node SQL).
	for _, o := range SchemaObjects("") {
		if o.DDL != want[o.Name].singleNode {
			t.Errorf("%s single-node DDL =\n%q\nwant the template verbatim\n%q", o.Name, o.DDL, want[o.Name].singleNode)
		}
	}
}

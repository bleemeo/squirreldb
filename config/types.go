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

package config

import "time"

const (
	BackendCassandra  = "cassandra"
	BackendClickHouse = "clickhouse"
	BackendDummy      = "dummy"
)

// Config is the main structured configuration.
type Config struct {
	Backend string `yaml:"backend"`
	// ExtraWriteBackend is an optional additional, best-effort write target
	// alongside Backend (which is always written synchronously and must succeed,
	// being the source of truth that serves reads, mutable labels and telemetry).
	// The extra backend is written best-effort and non-blocking: a failure to
	// write it is logged and dropped, never blocking or failing the request — so
	// the extra being down never affects Backend. Empty means "Backend only". Used
	// for the Cassandra→ClickHouse migration (dual-write); it must differ from
	// Backend. The migrator reaches it via MigrateToBackend.
	ExtraWriteBackend   string        `yaml:"extra_write_backend"`
	Cassandra           Cassandra     `yaml:"cassandra"`
	ClickHouse          ClickHouse    `yaml:"clickhouse"`
	Redis               Redis         `yaml:"redis"`
	ListenAddress       string        `yaml:"listen_address"`
	AdminListenAddress  string        `yaml:"admin_listen_address"`
	TenantLabelName     string        `yaml:"tenant_label_name"`
	RequireTenantHeader bool          `yaml:"require_tenant_header"`
	RemoteStorage       RemoteStorage `yaml:"remote_storage"`
	DefaultTimeToLive   time.Duration `yaml:"default_time_to_live"`
	PromQL              PromQL        `yaml:"promql"`
	Batch               Batch         `yaml:"batch"`
	Log                 Log           `yaml:"log"`
	Sentry              Sentry        `yaml:"sentry"`
	Internal            Internal      `yaml:"internal"`
	Telemetry           Telemetry     `yaml:"telemetry"`
	// MaxRequestBodySize defines the maximum size of incoming requests body in MiB.
	MaxRequestBodySize int64 `yaml:"max_request_body_size"`
	// MaxAllowedTimeInFuture defines how much duration in the future is allowed for points' timestamps.
	// Point more that this amount in the future might be rejected.
	MaxAllowedTimeInFuture time.Duration `yaml:"max_allowed_time_in_future"`
}

type Internal struct {
	ShowGoroutingOnShutdown bool         `yaml:"show_gorouting_on_shutdown"`
	IndexDummyCheckConflict bool         `yaml:"index_dummy_check_conflict"`
	IndexDummyFixedID       int          `yaml:"index_dummy_fixed_id"`
	Installation            Installation `yaml:"installation"`
	TemporaryStore          string       `yaml:"temporary_store"`
	DisablePreAggregation   bool         `yaml:"disable_pre_aggregation"`
	Dummy                   Dummy        `yaml:"dummy"`
	ReadOnly                bool         `yaml:"read_only"`
}

type Telemetry struct {
	Address string `yaml:"address"`
	Enabled bool   `yaml:"enabled"`
}

type Installation struct {
	Format string `yaml:"format"`
}

type Sentry struct {
	DSN string `yaml:"dsn"`
}

type Log struct {
	Level        int  `yaml:"level"`
	DisableColor bool `yaml:"disable_color"`
}

type Batch struct {
	Size time.Duration `yaml:"size"`
}

type PromQL struct {
	MaxEvaluatedPoints int `yaml:"max_evaluated_points"`
	MaxEvaluatedSeries int `yaml:"max_evaluated_series"`
}

type RemoteStorage struct {
	MaxConcurrentRequests int `yaml:"max_concurrent_requests"`
}

type Redis struct {
	Addresses   []string `yaml:"addresses"`
	Username    string   `yaml:"username"`
	Password    string   `yaml:"password"`
	SSL         bool     `yaml:"ssl"`
	SSLInsecure bool     `yaml:"ssl_insecure"`
	CertPath    string   `yaml:"cert_path"`
	KeyPath     string   `yaml:"key_path"`
	CAPath      string   `yaml:"ca_path"`
	Keyspace    string   `yaml:"keyspace"`
}

type Cassandra struct {
	Addresses              []string      `yaml:"addresses"`
	Keyspace               string        `yaml:"keyspace"`
	ReplicationFactor      int           `yaml:"replication_factor"`
	PreCreateShardDuration time.Duration `yaml:"pre_create_shard_duration"`
	PreCreateShardFraction int           `yaml:"pre_create_shard_fraction"`
	Aggregate              Aggregate     `yaml:"aggregate"`
	Username               string        `yaml:"username"`
	Password               string        `yaml:"password"`
	CertPath               string        `yaml:"cert_path"`
	KeyPath                string        `yaml:"key_path"`
	CAPath                 string        `yaml:"ca_path"`
	EnableHostVerification bool          `yaml:"enable_host_verification"`
}

type Aggregate struct {
	IntendedDuration time.Duration `yaml:"intended_duration"`
}

// ClickHouse holds the configuration of the ClickHouse backend.
// It is used when the "backend" option is set to "clickhouse".
type ClickHouse struct {
	// Addresses are the ClickHouse native protocol endpoints (host:port),
	// used for SQL (table creation and the read/write path).
	Addresses []string `yaml:"addresses"`
	Database  string   `yaml:"database"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
	// ClusterName, when set, makes SquirrelDB create its tables with
	// "ON CLUSTER <name>" and ReplicatedMergeTree engines (ClickHouse cluster
	// mode). Empty means a single ClickHouse node (plain MergeTree). It must
	// match the cluster name in the ClickHouse server configuration.
	//
	// The replicated tables use an explicit, name-based Keeper path, so they
	// must never be RENAMEd; see examples/squirreldb_ha_clickhouse for the
	// node-recovery runbook. Setting this on a ClickHouse that already holds
	// single-node tables needs a migration: docs/clickhouse-cluster.md.
	ClusterName string `yaml:"cluster_name"`
	// MaxConcurrentQueries caps the total in-flight queries (reads + writes)
	// SquirrelDB sends to ClickHouse, so it stays under the server's
	// max_concurrent_queries instead of surfacing a "too many simultaneous
	// queries" error; excess queries wait for a slot. Reads are capped one below
	// this so a write always has a slot. 0 (default) auto-detects the server's
	// max_concurrent_queries minus a margin — good for a 1:1, non-shared setup;
	// set it explicitly for a shared server, a per-user limit, or an
	// N-SquirrelDB/M-ClickHouse fan-out.
	MaxConcurrentQueries int `yaml:"max_concurrent_queries"`
	// WALFlushDelay is how often the elected drainer writes the shared Redis
	// write-ahead log to ClickHouse (0 uses the built-in default, ~5s). Writes are
	// durable in Redis on return; this bounds how long they take to become
	// queryable in ClickHouse.
	WALFlushDelay time.Duration `yaml:"wal_flush_delay"`
	// WALSoftThreshold is the WAL length, in POINTS, above which the drainer flushes
	// early rather than waiting for WALFlushDelay; the producer-backpressure
	// thresholds (slow down at 3×, block at 10×) are derived from it. The WAL stores
	// one point per entry, so its length is an exact, cheap point count; each point
	// carries its labels (~150-200 B), so this also bounds Redis memory. 0 uses the
	// built-in default (200k points ≈ 35 MiB → block at ~2M points ≈ 350 MiB).
	WALSoftThreshold int `yaml:"wal_soft_threshold"`
}

// Dummy holds the configuration of the dummy backend.
// It is used when the "backend" option is set to "dummy". The dummy backend
// discards every write and serves empty reads; it has no external dependency.
// Its purpose is to isolate load-test overhead: if a load test is slow against
// the dummy backend, the bottleneck is the test (or the batcher), not Cassandra
// or ClickHouse.
type Dummy struct {
	// Batcher, when true (the default), routes writes through the shared batcher
	// (batch.Batch) and its temporary store before discarding them, so the dummy
	// backend exercises the same write path as Cassandra/ClickHouse minus the
	// database write itself. When false, writes are discarded immediately, with
	// no batching and no temporary store (the absolute load-test baseline).
	Batcher bool `yaml:"batcher"`
}

// Copyright 2015-2025 Bleemeo
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

// Config is the main structured configuration.
type Config struct {
	Cassandra           Cassandra     `yaml:"cassandra"`
	Redis               Redis         `yaml:"redis"`
	ListenAddress       string        `yaml:"listen_address"`
	TenantLabelName     string        `yaml:"tenant_label_name"`
	RequireTenantHeader bool          `yaml:"require_tenant_header"`
	RemoteStorage       RemoteStorage `yaml:"remote_storage"`
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
	Index                   string       `yaml:"index"`
	IndexDummyCheckConflict bool         `yaml:"index_dummy_check_conflict"`
	IndexDummyFixedID       int          `yaml:"index_dummy_fixed_id"`
	Installation            Installation `yaml:"installation"`
	TSDB                    string       `yaml:"tsdb"`
	TemporaryStore          string       `yaml:"temporary_store"`
	Locks                   string       `yaml:"locks"`
	States                  string       `yaml:"states"`
	Store                   string       `yaml:"store"`
	MutableLabelsProvider   string       `yaml:"mutable_labels_provider"`
	DisablePreAggregation   bool         `yaml:"disable_pre_aggregation"`
	ReadOnly                bool         `yaml:"read_only"`
	UseThanosPromQLEngine   bool         `yaml:"use_thanos_promql_engine"`
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
	DefaultTimeToLive      time.Duration `yaml:"default_time_to_live"`
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

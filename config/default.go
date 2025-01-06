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

// defaultPaths returns the default paths used to search for config files.
func defaultPaths() []string {
	return []string{
		"squirreldb.conf",
	}
}

func DefaultConfig() Config {
	return Config{
		Cassandra: Cassandra{
			Addresses:              []string{"127.0.0.1:9042"},
			Keyspace:               "squirreldb",
			ReplicationFactor:      1,
			DefaultTimeToLive:      365 * 24 * time.Hour, // 1 year
			PreCreateShardDuration: 30 * time.Minute,
			PreCreateShardFraction: 27,
			Aggregate: Aggregate{
				IntendedDuration: time.Minute,
			},
			Username:               "",
			Password:               "",
			CertPath:               "",
			KeyPath:                "",
			CAPath:                 "",
			EnableHostVerification: true,
		},
		Redis: Redis{
			Addresses:   []string{},
			Username:    "",
			Password:    "",
			SSL:         false,
			SSLInsecure: false,
			CertPath:    "",
			KeyPath:     "",
			CAPath:      "",
			Keyspace:    "",
		},
		ListenAddress:       "localhost:9201",
		TenantLabelName:     "__account_id",
		RequireTenantHeader: false,
		RemoteStorage: RemoteStorage{
			MaxConcurrentRequests: 0,
		},
		PromQL: PromQL{
			MaxEvaluatedPoints: 0,
			MaxEvaluatedSeries: 0,
		},
		Batch: Batch{
			Size: 15 * time.Minute,
		},
		Log: Log{
			Level:        1,
			DisableColor: false,
		},
		Sentry: Sentry{
			DSN: "",
		},
		Telemetry: Telemetry{
			Enabled: true,
			Address: "https://telemetry.bleemeo.com/v1/telemetry/",
		},
		Internal: Internal{
			Index:                   "cassandra",
			IndexDummyCheckConflict: true,
			IndexDummyFixedID:       0,
			Installation: Installation{
				Format: "Manual",
			},
			TSDB:                  "cassandra",
			TemporaryStore:        "redis",
			Locks:                 "cassandra",
			States:                "cassandra",
			Store:                 "batcher",
			MutableLabelsProvider: "cassandra",
			UseThanosPromQLEngine: false,
			DisablePreAggregation: false,
		},
		MaxRequestBodySize:     64, // MiB
		MaxAllowedTimeInFuture: 48 * time.Hour,
	}
}

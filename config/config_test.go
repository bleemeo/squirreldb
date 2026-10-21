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

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// TestStructuredConfig tests loading the full configuration file.
func TestStructuredConfig(t *testing.T) {
	expectedConfig := Config{
		Backend: "clickhouse",
		Cassandra: Cassandra{
			Addresses: []string{
				"127.0.0.1:9000",
				"127.0.0.2:8000",
			},
			Keyspace:          "squirreldb",
			ReplicationFactor: 3,
			Aggregate: Aggregate{
				IntendedDuration: 60 * time.Second,
			},
			Username:               backendCassandra,
			Password:               "pass",
			CertPath:               "/cert.pem",
			KeyPath:                "/key.pem",
			CAPath:                 "/ca.pem",
			EnableHostVerification: true,
		},
		Redis: Redis{
			Addresses:   []string{"127.0.0.1:5000"},
			Username:    "redis",
			Password:    "pass",
			SSL:         true,
			SSLInsecure: true,
			CertPath:    "/cert.pem",
			KeyPath:     "/key.pem",
			CAPath:      "/ca.pem",
			Keyspace:    "squirreldb",
		},
		DefaultTimeToLive:  4 * time.Hour,
		ListenAddress:      "127.0.0.1:9090",
		AdminListenAddress: "127.0.0.1:9091",
		TLS: TLS{
			CertFile:       "/cert.pem",
			KeyFile:        "/key.pem",
			ClientCAFile:   "/ca.pem",
			ClientAuthType: "RequireAndVerifyClientCert",
			MinVersion:     "TLS13",
		},
		Auth: Auth{
			Enabled:            true,
			MetricsRequireAuth: true,
			Users: []AuthUser{
				{
					Name:        "glouton-acme",
					TokenSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					Tenant:      "acme",
					Scopes:      []string{"write"},
				},
				{
					Name:          "dashglass",
					PasswordHash:  "$2y$10$abcdefghijklmnopqrstuuABCDEFGHIJKLMNOPQRSTUVWXYZ01234",
					Scopes:        []string{"read"},
					ForcedMatcher: `env="prod"`,
					Backends:      []string{"clickhouse"},
				},
			},
		},
		TenantLabelName: "account",
		RemoteStorage: RemoteStorage{
			MaxConcurrentRequests: 1,
		},
		PromQL: PromQL{
			MaxEvaluatedPoints: 2,
			MaxEvaluatedSeries: 3,
		},
		Batch: Batch{
			Size: 30 * time.Minute,
		},
		Log: Log{
			Level:        1,
			DisableColor: true,
		},
		Sentry: Sentry{
			DSN: "my_dsn",
		},
		Internal: Internal{
			IndexDummyCheckConflict: true,
			IndexDummyFixedID:       1,
			Installation: Installation{
				Format: "manual",
			},
			TemporaryStore:        "redis",
			DisablePreAggregation: true,
		},
		Telemetry: Telemetry{
			Address: "http://example.com",
			Enabled: true,
		},
	}

	config, warnings, err := loadToStruct(false, nil, "testdata/full.conf")
	if warnings != nil {
		t.Fatalf("Warning while loading config: %s", warnings)
	}

	if err != nil {
		t.Fatalf("Failed to load config: %s", err)
	}

	if diff := cmp.Diff(expectedConfig, config); diff != "" {
		t.Fatalf("Unexpected config loaded:\n%s", diff)
	}
}

// TestMergeWithDefault tests that the config files and the environment variables
// are correctly merged.
// For files, basic types (string, int, ...) are overwritten and arrays are concatenated.
// Files overwrite default values but merges maps with the defaults.
// Environment variables always overwrite the existing config.
func TestMergeWithDefault(t *testing.T) {
	expectedConfig := DefaultConfig()
	expectedConfig.Telemetry.Enabled = false
	expectedConfig.Redis.SSLInsecure = true
	expectedConfig.Redis.CertPath = "a"
	expectedConfig.Redis.CAPath = "c"
	expectedConfig.Cassandra.Addresses = []string{}
	expectedConfig.Cassandra.Keyspace = "squirreldb"
	expectedConfig.Redis.Addresses = []string{"127.0.0.1:6379", "127.0.0.1:6380"}

	t.Setenv("SQUIRRELDB_CASSANDRA_ADDRESSES", "")
	t.Setenv("SQUIRRELDB_CASSANDRA_KEYSPACE", "squirreldb")

	config, warnings, err := loadToStruct(true, nil, "testdata/merge")
	if warnings != nil {
		t.Fatalf("Warning while loading config: %s", warnings)
	}

	if err != nil {
		t.Fatalf("Failed to load config: %s", err)
	}

	if diff := cmp.Diff(expectedConfig, config); diff != "" {
		t.Fatalf("Unexpected config:\n%s", diff)
	}
}

// TestFlags tests that flags values are passed correctly in the config
// and that default flag values are not kept if a setting is set.
func TestFlags(t *testing.T) {
	expectedConfig := DefaultConfig()

	// Values set from config file.
	expectedConfig.Internal.DisablePreAggregation = true
	expectedConfig.Cassandra.Addresses = []string{"192.168.0.2:9042"}

	// Value set from config, env and flag, only the flag should be kept.
	expectedConfig.Redis.Addresses = []string{"192.168.0.4:6379"}
	flagSet := NewFlagSet("test", ConfigFlags())

	if err := flagSet.Parse([]string{"--redis.addresses", "192.168.0.4:6379"}); err != nil {
		t.Fatalf("Failed to parse flags: %s", err)
	}

	t.Setenv("SQUIRRELDB_REDIS_ADDRESSES", "192.168.0.3:9042")

	config, warnings, err := loadToStruct(true, flagSet, "testdata/flags.conf")
	if warnings != nil {
		t.Fatalf("Warning while loading config: %s", warnings)
	}

	if err != nil {
		t.Fatalf("Failed to load config: %s", err)
	}

	if diff := cmp.Diff(expectedConfig, config); diff != "" {
		t.Fatalf("Unexpected config:\n%s", diff)
	}
}

// TestloadToStruct tests loading the config and the warnings and errors returned.
func TestLoadToStruct(t *testing.T) {
	tests := []struct {
		Name         string
		Files        []string
		Environment  map[string]string
		WantConfig   Config
		WantWarnings []string
		WantError    error
	}{
		{
			Name:  "wrong type",
			Files: []string{"testdata/bad_wrong_type.conf"},
			WantWarnings: []string{
				"'cassandra.addresses[0]' expected type 'string', got unconvertible " +
					"type 'map[string]interface {}'",
				`'cassandra.replication_factor' cannot parse value as 'int': strconv.ParseInt: invalid syntax`,
			},
			WantConfig: Config{
				Cassandra: Cassandra{
					Addresses: []string{""},
					Keyspace:  "squirreldb",
				},
			},
		},
		{
			Name:  "invalid yaml",
			Files: []string{"testdata/bad_yaml.conf"},
			WantWarnings: []string{
				"failed to load 'testdata/bad_yaml.conf': yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `bad:bad` into map[string]interface {}", //nolint: lll
			},
		},
		{
			Name:  "invalid yaml multiple files",
			Files: []string{"testdata/invalid"},
			WantWarnings: []string{
				"failed to load 'testdata/invalid/10-invalid.conf': yaml: line 2: found character that cannot start any token",
			},
			WantConfig: Config{
				Redis: Redis{
					SSL: true,
				},
				Cassandra: Cassandra{
					Addresses: []string{"127.0.0.1:9000"},
				},
			},
		},
		{
			Name: "slice from env",
			Environment: map[string]string{
				"SQUIRRELDB_REDIS_ADDRESSES":     "127.0.0.1:6379",
				"SQUIRRELDB_CASSANDRA_ADDRESSES": "127.0.0.1:9090,127.0.0.1:9091,127.0.0.1:9092",
			},
			WantConfig: Config{
				Redis: Redis{
					Addresses: []string{"127.0.0.1:6379"},
				},
				Cassandra: Cassandra{
					Addresses: []string{
						"127.0.0.1:9090",
						"127.0.0.1:9091",
						"127.0.0.1:9092",
					},
				},
			},
		},
		{
			Name: "empty file",
			Files: []string{
				"testdata/empty.conf",
				"testdata/simple.conf",
			},
			WantConfig: Config{
				Redis: Redis{
					SSL: true,
				},
			},
		},
		{
			Name:  "deprecated default_time_to_live from file",
			Files: []string{"testdata/deprecated_ttl.conf"},
			WantWarnings: []string{
				`config key "cassandra.default_time_to_live" is deprecated, ` +
					`please use "default_time_to_live" instead`,
			},
			WantConfig: Config{
				DefaultTimeToLive: 4 * time.Hour,
			},
		},
		{
			Name: "deprecated default_time_to_live from env",
			Environment: map[string]string{
				"SQUIRRELDB_CASSANDRA_DEFAULT_TIME_TO_LIVE": "4h",
			},
			WantWarnings: []string{
				`config key "cassandra.default_time_to_live" is deprecated, ` +
					`please use "default_time_to_live" instead`,
			},
			WantConfig: Config{
				DefaultTimeToLive: 4 * time.Hour,
			},
		},
		{
			Name:  "deprecated and new default_time_to_live both set",
			Files: []string{"testdata/deprecated_ttl_both.conf"},
			WantWarnings: []string{
				`config key "cassandra.default_time_to_live" is deprecated and ignored ` +
					`because "default_time_to_live" is also set, please remove "cassandra.default_time_to_live"`,
			},
			WantConfig: Config{
				DefaultTimeToLive: 10 * time.Hour,
			},
		},
	}

	safeErrorStr := func(e error) string {
		if e == nil {
			return "<no error>"
		}

		return e.Error()
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			for k, v := range test.Environment {
				t.Setenv(k, v)
			}

			config, warnings, err := loadToStruct(false, nil, test.Files...)
			if (test.WantError == nil) != (err == nil) {
				t.Fatalf(
					"Unexpected error for files %s: got %q, want %q",
					test.Files,
					safeErrorStr(err),
					safeErrorStr(test.WantError),
				)
			} else if test.WantError != nil {
				if diff := cmp.Diff(test.WantError.Error(), err.Error()); diff != "" {
					t.Fatalf("Unexpected error for files %s\n%s", test.Files, diff)
				}
			}

			var strWarnings []string

			for _, warning := range warnings {
				strWarnings = append(strWarnings, warning.Error())
			}

			lessFunc := func(a, b string) bool {
				return a < b
			}

			if diff := cmp.Diff(test.WantWarnings, strWarnings, cmpopts.SortSlices(lessFunc)); diff != "" {
				t.Fatalf("Unexpected warnings:\n%s", diff)
			}

			if diff := cmp.Diff(test.WantConfig, config); diff != "" {
				t.Fatalf("Unexpected config:\n%s", diff)
			}
		})
	}
}

// TestLoad tests loading the full config and the warnings and errors returned.
func TestLoad(t *testing.T) {
	defaultConfig := DefaultConfig()

	cfg_simple := DefaultConfig()
	cfg_simple.Redis.SSL = true

	tests := []struct {
		Name         string
		Files        []string
		Environment  map[string]string
		WantConfig   Config
		WantWarnings []string
		WantError    error
	}{
		{
			Name: "empty file",
			Files: []string{
				"testdata/empty.conf",
			},
			WantConfig: defaultConfig,
		},
		{
			Name: "simple",
			Files: []string{
				"testdata/simple.conf",
			},
			WantConfig: cfg_simple,
		},
		{
			Name:       "zero_ttl",
			Files:      []string{"testdata/zero_ttl.conf"},
			WantConfig: defaultConfig,
			WantError:  ErrZeroTTL,
		},
	}

	safeErrorStr := func(e error) string {
		if e == nil {
			return "<no error>"
		}

		return e.Error()
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			for k, v := range test.Environment {
				t.Setenv(k, v)
			}

			config, warnings, err := Load(nil, test.Files...)
			if (test.WantError == nil) != (err == nil) {
				t.Fatalf(
					"Unexpected error for files %s: got %q, want %q",
					test.Files,
					safeErrorStr(err),
					safeErrorStr(test.WantError),
				)
			} else if test.WantError != nil {
				if diff := cmp.Diff(test.WantError.Error(), err.Error()); diff != "" {
					t.Errorf("Unexpected error for files %s\n%s", test.Files, diff)
				}

				return
			}

			var strWarnings []string

			for _, warning := range warnings {
				strWarnings = append(strWarnings, warning.Error())
			}

			lessFunc := func(a, b string) bool {
				return a < b
			}

			if diff := cmp.Diff(test.WantWarnings, strWarnings, cmpopts.SortSlices(lessFunc)); diff != "" {
				t.Fatalf("Unexpected warnings: (-want +got)\n%s", diff)
			}

			if diff := cmp.Diff(test.WantConfig, config); diff != "" {
				t.Fatalf("Unexpected config: (-want +got)\n%s", diff)
			}
		})
	}
}

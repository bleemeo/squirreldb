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
		Cassandra: Cassandra{
			Addresses: []string{
				"127.0.0.1:9000",
				"127.0.0.2:8000",
			},
			Keyspace:          "squirreldb",
			ReplicationFactor: 3,
			DefaultTimeToLive: 4 * time.Hour,
			Aggregate: Aggregate{
				IntendedDuration: 60 * time.Second,
			},
			Username:               "cassandra",
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
		ListenAddress: "127.0.0.1:9090",
		RemoteStorage: RemoteStorage{
			MaxConcurrentRequests: 1,
		},
		PromQL: PromQL{
			MaxEvaluatedPoints: 2,
			MaxEvaluatedSeries: 3,
			TenantLabelName:    "account",
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
			Index:                   "cassandra",
			IndexDummyCheckConflict: true,
			IndexDummyFixedID:       1,
			Installation: Installation{
				Format: "manual",
			},
			TSDB:                  "cassandra",
			TemporaryStore:        "redis",
			Locks:                 "cassandra",
			States:                "cassandra",
			Store:                 "batcher",
			MutableLabelsProvider: "cassandra",
			DisableBackgroundTask: true,
			Telemetry: Telemetry{
				Address: "http://example.com",
				Enabled: true,
				ID: ID{
					Path: "my_id",
				},
			},
		},
	}

	config, warnings, err := loadToStruct(false, "testdata/full.conf")
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
	expectedConfig.Internal.Telemetry.Enabled = false
	expectedConfig.Redis.SSLInsecure = true
	expectedConfig.Redis.CertPath = "a"
	expectedConfig.Redis.CAPath = "c"
	expectedConfig.Cassandra.Addresses = []string{}
	expectedConfig.Cassandra.Keyspace = "squirreldb"
	expectedConfig.Redis.Addresses = []string{"127.0.0.1:6379", "127.0.0.1:6380"}

	t.Setenv("SQUIRRELDB_CASSANDRA_ADDRESSES", "")
	t.Setenv("SQUIRRELDB_CASSANDRA_KEYSPACE", "squirreldb")

	config, warnings, err := loadToStruct(true, "testdata/merge")
	if warnings != nil {
		t.Fatalf("Warning while loading config: %s", warnings)
	}

	if err != nil {
		t.Fatalf("Failed to load config: %s", err)
	}

	if diff := cmp.Diff(expectedConfig, config); diff != "" {
		t.Fatalf("Default value modified:\n%s", diff)
	}
}

// TestloadToStruct tests loading the config and the warnings and errors returned.
func TestLoad(t *testing.T) {
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
					"type 'map[string]interface {}', value: 'map[a:b]'",
				`cannot parse 'cassandra.replication_factor' as int: strconv.ParseInt: parsing "bad": invalid syntax`,
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
				"line 1: cannot unmarshal !!str `bad:bad` into map[string]interface {}",
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
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			for k, v := range test.Environment {
				t.Setenv(k, v)
			}

			config, warnings, err := loadToStruct(false, test.Files...)
			if diff := cmp.Diff(test.WantError, err); diff != "" {
				t.Fatalf("Unexpected error for files %s\n%s", test.Files, diff)
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

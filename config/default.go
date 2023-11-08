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
			Addresses:         []string{"127.0.0.1:9042"},
			Keyspace:          "squirreldb",
			ReplicationFactor: 1,
			DefaultTimeToLive: 365 * 24 * time.Hour, // 1 year
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
	}
}

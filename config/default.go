package config

//nolint: gochecknoglobals
var defaults = map[string]interface{}{
	"cassandra.addresses":                   []string{"localhost:9042"},
	"cassandra.keyspace":                    "squirreldb",
	"cassandra.replication_factor":          1,
	"cassandra.default_time_to_live":        "8760h", // 1 year
	"cassandra.partition_size.raw":          "120h",  // 5 days
	"cassandra.partition_size.aggregate":    "1920h", // 80 days
	"cassandra.aggregate.resolution":        "5m",
	"cassandra.aggregate.size":              "24h",
	"cassandra.aggregate.intended_duration": "1m",
	"redis.addresses":                       []string{},
	"remote_storage.listen_address":         "localhost:9201",
	"batch.size":                            "15m",
	"index.include_id":                      false,
	"log.level":                             0,
	"internal.index":                        "cassandra",
	"internal.index_dummy_check_conflict":   true,
	"internal.index_dummy_fixed_id":         0,
	"internal.tsdb":                         "cassandra",
	"internal.temporary_store":              "redis",
	"internal.locks":                        "cassandra",
	"internal.states":                       "cassandra",
	"internal.store":                        "batcher",
}

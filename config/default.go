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
	"redis.address":                         "",
	"remote_storage.listen_address":         "localhost:9201",
	"batch.size":                            "15m",
	"index.include_uuid":                    false,
	"log.level":                             0,
}

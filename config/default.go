package config

//nolint: gochecknoglobals
var defaults = map[string]interface{}{
	"cassandra.addresses":                []string{"localhost:9042"},
	"cassandra.keyspace":                 "squirreldb",
	"cassandra.replication_factor":       1,
	"cassandra.default_time_to_live":     31536000,
	"cassandra.partition_size.raw":       432000,
	"cassandra.partition_size.aggregate": 6912000,
	"cassandra.aggregate.resolution":     300,
	"cassandra.aggregate.size":           86400,
	"remote_storage.listen_address":      "localhost:1234",
	"redis.address":                      "localhost:6379",
	"batch.size":                         360,
	"debug.level":                        0,
}

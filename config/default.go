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
	"redis.address":                      "localhost:6379",
	"remote_storage.listen_address":      "localhost:9201",
	"batch.size":                         360,
	"debug.level":                        0,
}

//nolint: gochecknoglobals
var envToKey = map[string]string{
	"CASSANDRA_ADDRESSES":                "cassandra.addresses",
	"CASSANDRA_KEYSPACE":                 "cassandra.keyspace",
	"CASSANDRA_REPLICATION_FACTOR":       "cassandra.replication_factor",
	"CASSANDRA_DEFAULT_TIME_TO_LIVE":     "cassandra.default_time_to_live",
	"CASSANDRA_PARTITION_SIZE_RAW":       "cassandra.partition_size.raw",
	"CASSANDRA_PARTITION_SIZE_AGGREGATE": "cassandra.partition_size.aggregate",
	"CASSANDRA_AGGREGATE_RESOLUTION":     "cassandra.aggregate.resolution",
	"CASSANDRA_AGGREGATE_SIZE":           "cassandra.aggregate.size",
	"REDIS_ADDRESS":                      "redis.address",
	"REMOTE_STORAGE_LISTEN_ADDRESS":      "remote_storage.listen_address",
	"BATCH_SIZE":                         "batch.size",
	"DEBUG_LEVEL":                        "debug.level",
}

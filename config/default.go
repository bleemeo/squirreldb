package config

var defaults = map[string]interface{}{
	"cassandra.addresses":                []string{"localhost:9042"},
	"cassandra.replication_factor":       1,
	"cassandra.keyspace":                 "squirreldb",
	"cassandra.default_time_to_live":     31536000,
	"cassandra.partition_size.raw":       432000,
	"cassandra.partition_size.aggregate": 6912000,
	"cassandra.aggregate.resolution":     300,
	"cassandra.aggregate.size":           86400,
	"cassandra.aggregate.start_offset":   32400,
	"prometheus.listen_address":          "localhost:1234",
	"batch.size":                         360,
}

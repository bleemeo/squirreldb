package config

//nolint:gochecknoglobals
var defaults = map[string]interface{}{
	"cassandra.addresses":                    []string{"localhost:9042"},
	"cassandra.keyspace":                     "squirreldb",
	"cassandra.replication_factor":           1,
	"cassandra.default_time_to_live":         "8760h", // 1 year
	"cassandra.aggregate.intended_duration":  "1m",
	"redis.addresses":                        []string{},
	"listen_address":                         "localhost:9201",
	"remote_storage.max_concurrent_requests": 0,
	"promql.max_evaluated_points":            0,
	"promql.max_evaluated_series":            0,
	"promql.tenant_label_name":               "__account_id",
	"batch.size":                             "15m",
	"log.level":                              0,
	"internal.index":                         "cassandra",
	"internal.index_dummy_check_conflict":    true,
	"internal.index_dummy_fixed_id":          0,
	"internal.installation.format":           "Manual",
	"internal.tsdb":                          "cassandra",
	"internal.temporary_store":               "redis",
	"internal.locks":                         "cassandra",
	"internal.states":                        "cassandra",
	"internal.store":                         "batcher",
	"internal.redis_keyspace":                "",
	"internal.mutable_labels_provider":       "cassandra",
	"telemetry.address":                      "https://telemetry.bleemeo.com/v1/telemetry/",
	"telemetry.enabled":                      true,
	"telemetry.id.path":                      "./telemetry.json",
}

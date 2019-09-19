package config

const (
	BatchLength          = 30
	BatchCheckerInterval = 60
	BatchRetryDelay      = 10
)

const (
	CassandraKeyspace          = "remote_storage"
	CassandraReplicationFactor = 1
	CassandraMetricsTable      = "metrics"
	CassandraMetricRetention   = 397
	CassandraRetryDelay        = 10
)

const (
	PartitionLength = 432000
)

const (
	PrometheusAddress    = ":1234"
	PrometheusRetryDelay = 10
)

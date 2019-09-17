package config

const (
	BatchDuration        = 300
	BatchCheckerDuration = 60
)

const (
	CassandraInitSessionAttempts = 10
	CassandraInitSessionTimeout  = 10
	CassandraKeyspace            = "remote_storage"
	CassandraMetricsTable        = "metrics"
)

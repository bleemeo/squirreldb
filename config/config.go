package config

// Seconds
const (
	BatchTimeLength = 60

	BatchCheckerDelay = 5

	BatchAppendAttempts = -1
	BatchAppendTimeout  = 10

	BatchGetAttempts = -1
	BatchGetTimeout  = 10

	BatchSetAttempts = -1
	BatchSetTimeout  = 10

	BatchWriteAttempts = -1
	BatchWriteTimeout  = 10
)

const (
	CassandraKeyspace     = "remote_storage"
	CassandraMetricsTable = "metrics"

	CassandraInitSessionAttempts = 10
	CassandraInitSessionTimeout  = 10
)

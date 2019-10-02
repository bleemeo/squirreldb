package config

import "time"

const (
	BatchDuration        = 300 * time.Second
	BatchCheckerInterval = 60 * time.Second
)

const (
	CassandraKeyspace          = "squirreldb"
	CassandraReplicationFactor = 1
	CassandraMetricsTable      = "data"
	CassandraMetricRetention   = 397 * 24 * time.Hour
)

const (
	PartitionLength = 5 * 24 * time.Hour
)

const (
	PrometheusAddress = ":1234"
)

const (
	StorageTimeToLive = (BatchDuration * 2) + (150 * time.Second)
)

const (
	StoreExpiratorInterval = 60 * time.Second
)

package config

import "time"

const (
	BatchLength          = 300 * time.Second
	BatchCheckerInterval = 60 * time.Second
	BatchRetryDelay      = 10 * time.Second
)

const (
	CassandraKeyspace          = "squirreldb"
	CassandraReplicationFactor = 1
	CassandraMetricsTable      = "data"
	CassandraMetricRetention   = 397 * 24 * time.Hour
	CassandraRetryDelay        = 10 * time.Second
)

const (
	PartitionLength = 5 * 24 * time.Hour
)

const (
	PrometheusAddress    = ":1234"
	PrometheusRetryDelay = 10 * time.Second
)

const (
	StorageTimeToLive = (BatchLength * 2) + (150 * time.Second)
)

const (
	StoreExpiratorInterval = 60 * time.Second
)

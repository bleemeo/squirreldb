package tsdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	aggregationSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "tsdb",
		Name:      "aggregation_seconds",
		Help:      "Total processing time spent for aggregating each shard",
	})
	aggregatdUntilSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "squirreldb",
		Subsystem: "tsdb",
		Name:      "aggregated_untile_seconds",
		Help:      "Most recent timestamp for which all shard are aggregated",
	})
	cassandraQueriesSecondsReadAggregated = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "cassandra_queries_seconds",
		Help:        "Total processing time spent in Cassandra itself in seconds",
		ConstLabels: prometheus.Labels{"operation": "read", "type": "aggregated"},
	})
	cassandraQueriesSecondsReadRaw = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "cassandra_queries_seconds",
		Help:        "Total processing time spent in Cassandra itself in seconds",
		ConstLabels: prometheus.Labels{"operation": "read", "type": "raw"},
	})
	cassandraQueriesSecondsWriteAggregated = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "cassandra_queries_seconds",
		Help:        "Total processing time spent in Cassandra itself in seconds",
		ConstLabels: prometheus.Labels{"operation": "write", "type": "aggregated"},
	})
	cassandraQueriesSecondsWriteRaw = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "cassandra_queries_seconds",
		Help:        "Total processing time spent in Cassandra itself in seconds",
		ConstLabels: prometheus.Labels{"operation": "write", "type": "raw"},
	})
	requestsPointsTotalReadAggregated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "requests_points_total",
		Help:        "Total points processed by persistent TSDB",
		ConstLabels: prometheus.Labels{"operation": "read", "type": "aggregated"},
	})
	requestsPointsTotalReadRaw = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "requests_points_total",
		Help:        "Total points processed by persistent TSDB",
		ConstLabels: prometheus.Labels{"operation": "read", "type": "raw"},
	})
	requestsPointsTotalWriteAggregated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "requests_points_total",
		Help:        "Total points processed by persistent TSDB",
		ConstLabels: prometheus.Labels{"operation": "write", "type": "aggregated"},
	})
	requestsPointsTotalWriteRaw = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "requests_points_total",
		Help:        "Total points processed by persistent TSDB",
		ConstLabels: prometheus.Labels{"operation": "write", "type": "raw"},
	})
	requestsSecondsReadAggregated = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "read", "type": "aggregated"},
	})
	requestsSecondsReadRaw = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "read", "type": "raw"},
	})
	requestsSecondsWriteAggregated = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "write", "type": "aggregated"},
	})
	requestsSecondsWriteRaw = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "tsdb",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "write", "type": "raw"},
	})
)

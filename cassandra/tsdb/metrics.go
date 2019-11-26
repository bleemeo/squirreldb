package tsdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	querySecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "query_seconds",
		Help:        "Total seconds of querying Cassandra",
		ConstLabels: prometheus.Labels{"type": "read"},
	})
	querySecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "query_seconds",
		Help:        "Total seconds of querying Cassandra",
		ConstLabels: prometheus.Labels{"type": "write"},
	})
	readPointsTotalAggregated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "read_points_total",
		Help:        "Total number of read points from Cassandra after temporal filtering and deduplication",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	readPointsTotalRaw = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "read_points_total",
		Help:        "Total number of read points from Cassandra after temporal filtering and deduplication",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	readSecondsAggregated = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "read_seconds",
		Help:        "Total seconds of reading, including Cassandra querying, temporal filtering and deduplication",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	readSecondsRaw = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "read_seconds",
		Help:        "Total seconds of reading, including Cassandra querying, temporal filtering and deduplication",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	writtenPointsTotalAggregated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "written_points_total",
		Help:        "Total number of written points to Cassandra after deduplication",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	writtenPointsTotalRaw = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "written_points_total",
		Help:        "Total number of written points to Cassandra after deduplication",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	writeSecondsAggregated = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "write_seconds",
		Help:        "Total seconds of writing, including Cassandra querying and deduplication",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	writeSecondsRaw = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "write_seconds",
		Help:        "Total seconds of writing, including Cassandra querying and deduplication",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
)

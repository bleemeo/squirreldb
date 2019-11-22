package tsdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	aggregateLastTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "last_timestamp",
		Help:      "Last aggregation timestamp",
	})
	querySecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "query_seconds",
		Help:        "Total seconds of querying",
		ConstLabels: prometheus.Labels{"type": "read"},
	})
	querySecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "query_seconds",
		Help:        "Total seconds of querying",
		ConstLabels: prometheus.Labels{"type": "write"},
	})
	readPointsTotalAggregated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "read_points_total",
		Help:        "Total number of points read",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	readPointsTotalRaw = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "read_points_total",
		Help:        "Total number of points read",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	readSecondsAggregated = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "read_seconds",
		Help:        "Total seconds of reading",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	readSecondsRaw = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "read_seconds",
		Help:        "Total seconds of reading",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	wrotePointsTotalAggregated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "wrote_points_total",
		Help:        "Total number of points wrote",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	wrotePointsTotalRaw = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "wrote_points_total",
		Help:        "Total number of points wrote",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	wroteSecondsAggregated = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "wrote_seconds",
		Help:        "Total seconds of writing",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	wroteSecondsRaw = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "tsdb",
		Name:        "wrote_seconds",
		Help:        "Total seconds of writing",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
)

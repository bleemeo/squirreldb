package tsdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	aggregateLastTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "last_timestamp",
		Help:      "Last aggregation timestamp",
	})
	aggregateNextTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "next_timestamp",
		Help:      "Next aggregation timestamp",
	})
	aggregateSeconds = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "seconds_total",
		Help:      "Total seconds of aggregation (reading, aggregating, writing)",
	})
	aggregateProcessedPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "processed_points_total",
		Help:      "Total number of points processed",
	})
	readAggregatedSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "read_seconds",
		Help:        "Total seconds of reading",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	readRawSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "read_seconds",
		Help:        "Total seconds of reading",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	readAggregatedPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "read_points_total",
		Help:        "Total number of points read",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	readRawPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "read_points_total",
		Help:        "Total number of points read",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	wroteAggregatedSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "wrote_seconds",
		Help:        "Total seconds of writing",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	wroteRawSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "wrote_seconds",
		Help:        "Total seconds of writing",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	wroteAggregatedPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "wrote_points_total",
		Help:        "Total number of points wrote",
		ConstLabels: prometheus.Labels{"type": "aggregated"},
	})
	wroteRawPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "wrote_points_total",
		Help:        "Total number of points wrote",
		ConstLabels: prometheus.Labels{"type": "raw"},
	})
	readQueriesSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "queries_seconds",
		Help:        "Total number of queries",
		ConstLabels: prometheus.Labels{"type": "read"},
	})
	writeQueriesSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "",
		Name:        "queries_seconds",
		Help:        "Total number of queries",
		ConstLabels: prometheus.Labels{"type": "write"},
	})
)

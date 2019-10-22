package cassandra

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	aggregateSecondsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "seconds_total",
		Help:      "Total seconds of aggregation (reading, aggregating, writing)",
	})
	aggregateReadPointsSecondsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "read_points_seconds_total",
		Help:      "Total seconds of points reading",
	})
	aggregateAggregatePointsSecondsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "aggregate_points_seconds_total",
		Help:      "Total seconds of points aggregating",
	})
	aggregateWritePointsSecondsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "write_points_seconds_total",
		Help:      "Total seconds of points writing",
	})
	aggregateWriteRowsSecondsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "write_rows_seconds_total",
		Help:      "Total seconds of rows writing",
	})
	aggregateReadPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "read_points_total",
		Help:      "Total number of points read",
	})
	aggregateAggregatedPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "aggregated_points_total",
		Help:      "Total number of points aggregated",
	})
	aggregateWrotePointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "wrote_points_total",
		Help:      "Total number of points wrote",
	})
	aggregateWroteRowsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "wrote_rows_total",
		Help:      "Total number of rows wrote",
	})
)

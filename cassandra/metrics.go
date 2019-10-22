package cassandra

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	aggregateSecondsTotal = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "seconds_total",
		Help:      "Total seconds of aggregation (reading, aggregating, writing)",
	})
	aggregateReadPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandra",
		Subsystem: "aggregate",
		Name:      "read_points_total",
		Help:      "Total number of points read",
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

package batch

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	requestsPointsTotalDelete = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_points_total",
		Help:        "Total points processed (only points in the in-memory store are counter, not points from/to TSDB)",
		ConstLabels: prometheus.Labels{"operation": "delete"},
	})
	requestsPointsTotalRead = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_points_total",
		Help:        "Total points processed (only points in the in-memory store are counter, not points from/to TSDB)",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	requestsPointsTotalWrite = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_points_total",
		Help:        "Total points processed (only points in the in-memory store are counter, not points from/to TSDB)",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
	requestsSecondsDelete = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds (only time for operation on in-memory store is counter, not time spent in TSDB)",
		ConstLabels: prometheus.Labels{"operation": "delete"},
	})
	requestsSecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds (only time for operation on in-memory store is counter, not time spent in TSDB)",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	requestsSecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds (only time for operation on in-memory store is counter, not time spent in TSDB)",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
)

package batch

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	requestsPointsTotalRead = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_points_total",
		Help:        "Total points processed by batcher",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	requestsPointsTotalWrite = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_points_total",
		Help:        "Total points processed by batcher",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
	requestsSecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	requestsSecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})

	BackgroundSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "batch",
		Name:      "background_seconds",
		Help:      "Total background processing time in seconds",
	})
	FlushPointsTotalDelete = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "points_total",
		Help:        "Total points processed by the flush() method from/to memory-store",
		ConstLabels: prometheus.Labels{"operation": "delete"},
	})
	FlushPointsTotalRead = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "batch",
		Name:        "points_total",
		Help:        "Total points processed by the flush() method from/to memory-store",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
)

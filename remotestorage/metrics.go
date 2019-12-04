package remotestorage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	requestsPointsTotalRead = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "remote_storage",
		Name:        "requests_points_total",
		Help:        "Total points processed by SquirrelDB remote storage",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	requestsPointsTotalWrite = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "remote_storage",
		Name:        "requests_points_total",
		Help:        "Total points processed by SquirrelDB remote storage",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
	requestsSecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "remote_storage",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds (including sending response to client)",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	requestsSecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "remote_storage",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds (including sending response to client)",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
)

package redis

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	operationPointssAdd = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "redis",
		Name:        "operations_points_total",
		Help:        "Total points processed by Redis operation",
		ConstLabels: prometheus.Labels{"operation": "add"},
	})
	operationPointssGet = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "redis",
		Name:        "operations_points_total",
		Help:        "Total points processed by Redis operation",
		ConstLabels: prometheus.Labels{"operation": "get"},
	})
	operationPointssSet = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "redis",
		Name:        "operations_points_total",
		Help:        "Total points processed by Redis operation",
		ConstLabels: prometheus.Labels{"operation": "set"},
	})
	operationSecondsAdd = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "redis",
		Name:        "operations_seconds",
		Help:        "Total processing time of Redis operations in seconds",
		ConstLabels: prometheus.Labels{"operation": "add"},
	})
	operationSecondsGet = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "redis",
		Name:        "operations_seconds",
		Help:        "Total processing time of Redis operations in seconds",
		ConstLabels: prometheus.Labels{"operation": "get"},
	})
	operationSecondsSet = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "redis",
		Name:        "operations_seconds",
		Help:        "Total processing time of Redis operations in seconds",
		ConstLabels: prometheus.Labels{"operation": "set"},
	})
)

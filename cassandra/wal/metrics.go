package wal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	requestsSecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "wal",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
	requestsSecondsFlush = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "wal",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "flush"},
	})
	requestsSecondsCheckpoint = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "wal",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "checkpoint"},
	})
	requestsSecondsPurge = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "wal",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "purge"},
	})
	requestsSecondsReadOther = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "wal",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	writeByOther = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "wal",
		Name:      "write_other_total",
		Help:      "Total writes done by another SquirrelDB instance",
	})
)

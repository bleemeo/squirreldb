package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	lookupLabelsSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_labels_seconds",
		Help:      "Total lookup for labels (from UUID) time in seconds",
	})
	lookupUUIDSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_uuid_seconds",
		Help:      "Total lookup for metric UUID (from labels) time in seconds",
	})
	lookupUUIDMisses = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_uuid_misses_total",
		Help:      "Total lookup for metric UUID that missed the cache (including new metrics)",
	})
	lookupUUIDNew = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_uuid_new_total",
		Help:      "Total lookup for metric UUID for new metrics",
	})
	searchMetricsSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "search_metrics_seconds",
		Help:      "Total search of metrics (from labels selector) time in seconds",
	})
	searchMetricsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "search_metrics_totals",
		Help:      "Total number of metrics matching a serch from labels selector",
	})
)

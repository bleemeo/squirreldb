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
		Help:      "Total lookup for labels (from ID) time in seconds",
	})
	LookupIDSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_id_seconds",
		Help:      "Total lookup for metric ID (from labels) time in seconds",
	})
	LookupIDMisses = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_id_misses_total",
		Help:      "Total lookup for metric ID that missed the cache (including new metrics)",
	})
	LookupIDNew = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_id_new_total",
		Help:      "Total lookup for metric ID for new metrics",
	})
	LookupIDRefresh = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_id_refresh_total",
		Help:      "Total lookup for metric ID that caused a refresh of TTL",
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
		Name:      "search_metrics_total",
		Help:      "Total number of metrics matching a serch from labels selector",
	})
	expirationMoveSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "expire_move_seconds",
		Help:      "Total processing time to move metrics ID from one expiration list to another in seconds",
	})
	expireMetric = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "expire_metrics_total",
		Help:      "Total number of metrics processed by the expiration task (deleted or not)",
	})
	expireMetricDelete = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "expire_metrics_delete_total",
		Help:      "Total number of metrics deleted by the expiration task",
	})
	expireGhostMetric = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "expire_ghost_metrics_total",
		Help:      "Total number of \"ghost\" metrics deleted by the expiration task, that is metric partially created",
	})
	expireTotalSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "expire_seconds",
		Help:      "Total processing time of the expiration task in seconds",
	})
	expireLockSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "expire_lock_seconds",
		Help:      "Total processing time with the new-metric lock hold by the task in seconds",
	})
)

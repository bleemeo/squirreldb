package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	CreateMetricSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "create_seconds",
		Help:      "Total metric creation time in seconds (excluding time to took lock)",
	})
	updatePostingSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "update_postings_seconds",
		Help:      "Total postings updates time in seconds (including time to took lock)",
	})
	lookupLabelsSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_labels_seconds",
		Help:      "Total lookup for labels (from ID) time in seconds",
	})
	LookupIDRequestSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_id_request_seconds",
		Help:      "Total lookup request for metric IDs (from labels) time in seconds",
	})
	LookupIDs = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_id_total",
		Help:      "Total metric looked-up for ID by labels",
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
	LookupIDConcurrentNew = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_id_concurrent_new_total",
		Help:      "Total lookup for metric ID for new metrics happened concurrently for the same metric",
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
	expireConflictMetric = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "expire_conflict_total",
		Help:      "Total number of conflict in expiration updates",
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
	cassandraQueriesSecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "index",
		Name:        "cassandra_queries_seconds",
		Help:        "Total processing time spent in Cassandra in seconds",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	cassandraQueriesSecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "index",
		Name:        "cassandra_queries_seconds",
		Help:        "Total processing time spent in Cassandra in seconds",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
)

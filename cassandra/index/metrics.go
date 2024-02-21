package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	CreateMetricSeconds     prometheus.Summary
	UpdatePostingSeconds    prometheus.Summary
	UpdatedPosting          prometheus.Counter
	LookupLabelsSeconds     prometheus.Summary
	LookupIDRequestSeconds  prometheus.Summary
	LookupIDNew             prometheus.Counter
	LookupIDConcurrentNew   prometheus.Counter
	LookupIDRefresh         prometheus.Counter
	SearchMetricsSeconds    prometheus.Summary
	SearchMetrics           prometheus.Counter
	ExpirationMoveSeconds   prometheus.Summary
	ExpireMetric            prometheus.Counter
	ExpireMetricDelete      prometheus.Counter
	ExpireConflictMetric    prometheus.Counter
	ExpireGhostMetric       prometheus.Counter
	ExpireTotalSeconds      prometheus.Summary
	ExpireLockSeconds       prometheus.Summary
	CassandraQueriesSeconds *prometheus.SummaryVec
	CacheAccess             *prometheus.CounterVec
	CacheSize               *prometheus.GaugeVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		CreateMetricSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "create_seconds",
			Help:      "Total metric creation time in seconds (excluding time to took lock)",
		}),
		UpdatePostingSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "update_postings_seconds",
			Help:      "Total postings updates time in seconds (including time to took lock)",
		}),
		UpdatedPosting: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "updated_postings_total",
			Help:      "Total number of posting updated",
		}),
		LookupLabelsSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "lookup_labels_seconds",
			Help:      "Total lookup for labels (from ID) time in seconds",
		}),
		LookupIDRequestSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "lookup_id_request_seconds",
			Help:      "Total lookup request for metric IDs (from labels) time in seconds",
		}),
		LookupIDNew: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "lookup_id_new_total",
			Help:      "Total lookup for metric ID for new metrics",
		}),
		LookupIDConcurrentNew: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "lookup_id_concurrent_new_total",
			Help:      "Total lookup for metric ID for new metrics happened concurrently for the same metric",
		}),
		LookupIDRefresh: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "lookup_id_refresh_total",
			Help:      "Total lookup for metric ID that caused a refresh of TTL",
		}),
		SearchMetricsSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "search_metrics_seconds",
			Help:      "Total search of metrics (from labels selector) time in seconds",
		}),
		SearchMetrics: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "search_metrics_total",
			Help:      "Total number of metrics matching a serch from labels selector",
		}),
		ExpirationMoveSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "expire_move_seconds",
			Help:      "Total processing time to move metrics ID from one expiration list to another in seconds",
		}),
		ExpireMetric: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "expire_metrics_total",
			Help:      "Total number of metrics processed by the expiration task (deleted or not)",
		}),
		ExpireMetricDelete: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "expire_metrics_delete_total",
			Help:      "Total number of metrics deleted by the expiration task",
		}),
		ExpireConflictMetric: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "expire_conflict_total",
			Help:      "Total number of conflict in expiration updates",
		}),
		ExpireGhostMetric: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "expire_ghost_metrics_total",
			Help:      "Total number of \"ghost\" metrics deleted by the expiration task, that is metric partially created",
		}),
		ExpireTotalSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "expire_seconds",
			Help:      "Total processing time of the expiration task in seconds",
		}),
		ExpireLockSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "expire_lock_seconds",
			Help:      "Total processing time with the new-metric lock hold by the task in seconds",
		}),
		CassandraQueriesSeconds: promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "index",
			Name:      "cassandra_queries_seconds",
			Help:      "Total processing time spent in Cassandra in seconds",
		}, []string{"operation"}),
		CacheSize: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "squirreldb",
				Subsystem: "index",
				Name:      "cache_size",
				Help:      "Entry in in-memory cache",
			},
			[]string{"cache"},
		),
		CacheAccess: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "squirreldb",
				Subsystem: "index",
				Name:      "cache_access_total",
				Help:      "Total number of access of each cache",
			},
			[]string{"cache", "status"},
		),
	}
}

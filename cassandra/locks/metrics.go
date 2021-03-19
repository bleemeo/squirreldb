package locks

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	CassandraQueriesSeconds *prometheus.SummaryVec
	LocksLockSuccess        prometheus.Counter
	LocksLockSeconds        prometheus.Summary
	LocksUnlockSeconds      prometheus.Summary
	PendingLock             prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		CassandraQueriesSeconds: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "squirreldb",
				Subsystem: "locks",
				Name:      "cassandra_queries_seconds",
				Help:      "Total processing time spent in Cassandra in seconds",
			},
			[]string{"operation"},
		),
		LocksLockSuccess: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "lock_success_total",
			Help:      "Total number of successful (acquired) locks",
		}),
		LocksLockSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "lock_seconds",
			Help:      "Total time to acquire a lock (including waiting time) in seconds",
		}),
		LocksUnlockSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "unlock_seconds",
			Help:      "Total time to release a lock in seconds",
		}),
		PendingLock: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "lock_pending",
			Help:      "Number of gorouting trying to acquire a lock",
		}),
	}
}

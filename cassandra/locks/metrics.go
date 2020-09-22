package locks

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	cassandraQueriesSeconds = promauto.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "cassandra_queries_seconds",
			Help:      "Total processing time spent in Cassandra in seconds",
		},
		[]string{"operation"},
	)
	locksLockSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "locks",
		Name:      "lock_success_total",
		Help:      "Total number of successful (acquired) locks",
	})
	locksLockSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "locks",
		Name:      "lock_seconds",
		Help:      "Total time to acquire a lock (including waiting time) in seconds",
	})
	locksUnlockSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "locks",
		Name:      "unlock_seconds",
		Help:      "Total time to release a lock in seconds",
	})
	pendingLock = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "squirreldb",
		Subsystem: "locks",
		Name:      "lock_pending",
		Help:      "Number of gorouting trying to acquire a lock",
	})
)

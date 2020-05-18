package locks

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	locksAlreadyAcquireSeconds = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "locks",
		Name:      "lock_already_acquire_total",
		Help:      "Total TryLock() on a already acquired lock",
	})
	locksTryLockSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "locks",
		Name:      "try_lock_seconds",
		Help:      "Total time to try acquire a lock (successful or failed) in seconds",
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
	locksRefreshSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "locks",
		Name:      "refresh_seconds",
		Help:      "Total time to refresh a lock in seconds",
	})
)

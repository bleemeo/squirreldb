package locks

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	locksTryLockSeconds = promauto.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "try_lock_seconds",
			Help:      "Total time to try acquire a lock (successful or failed) in seconds",
		},
		[]string{"acquired"},
	)
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
	pendingLock = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "squirreldb",
		Subsystem: "locks",
		Name:      "lock_pending",
		Help:      "Number of gorouting trying to acquire a lock",
	})
)

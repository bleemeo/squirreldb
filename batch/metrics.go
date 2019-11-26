package batch

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	addedPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "batch",
		Subsystem: "",
		Name:      "add_points_total",
		Help:      "Total number of added points to in-memory store",
	})
	addSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "batch",
		Subsystem: "",
		Name:      "append_seconds",
		Help:      "Total seconds of adding to in-memory store",
	})
	readPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "read_points_total",
		Help:      "Total number of read points from in-memory store",
	})
	readSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "read_seconds",
		Help:      "Total seconds of reading from in-memory store",
	})
	purgedPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "purged_points_total",
		Help:      "Total number of purged points from in-memory store",
	})
	purgeSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "purge_seconds",
		Help:      "Total seconds of purging from in-memory store",
	})
)

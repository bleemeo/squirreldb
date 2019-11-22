package redis

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	appendPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "append_points_total",
		Help:      "Total number of points appended",
	})
	appendSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "append_seconds",
		Help:      "Total seconds of appending",
	})
	getPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "get_points_total",
		Help:      "Total number of points get",
	})
	getSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "get_seconds",
		Help:      "Total seconds of appending",
	})
	setPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "set_points_total",
		Help:      "Total number of points set",
	})
	setSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "store",
		Subsystem: "",
		Name:      "set_seconds",
		Help:      "Total seconds of setting",
	})
)

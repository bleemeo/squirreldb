package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	readRequestPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "prometheus",
		Subsystem: "",
		Name:      "read_request_points_total",
		Help:      "Total number of read points, including response to client",
	})
	readRequestSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "prometheus",
		Subsystem:   "",
		Name:        "read_request_seconds",
		Help:        "Total seconds of processing read request, including response to client",
		ConstLabels: prometheus.Labels{"type": "read"},
	})
	writeRequestPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "prometheus",
		Subsystem: "",
		Name:      "write_request_points_total",
		Help:      "Total number of written points",
	})
	writeRequestSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "prometheus",
		Subsystem:   "",
		Name:        "write_request_seconds",
		Help:        "Total seconds of processing write request",
		ConstLabels: prometheus.Labels{"type": "write"},
	})
)

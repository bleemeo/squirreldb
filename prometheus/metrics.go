package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestSecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "prometheus",
		Subsystem:   "",
		Name:        "request_seconds",
		Help:        "Total seconds of processing request",
		ConstLabels: prometheus.Labels{"type": "read"},
	})
	requestSecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "prometheus",
		Subsystem:   "",
		Name:        "request_seconds",
		Help:        "Total seconds of processing request",
		ConstLabels: prometheus.Labels{"type": "write"},
	})
)

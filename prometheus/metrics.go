package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	readRequestSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "prometheus",
		Subsystem:   "",
		Name:        "request_seconds",
		Help:        "Total number of queries",
		ConstLabels: prometheus.Labels{"type": "read"},
	})
	writeRequestSeconds = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "prometheus",
		Subsystem:   "",
		Name:        "request_seconds",
		Help:        "Total number of queries",
		ConstLabels: prometheus.Labels{"type": "write"},
	})
)

package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	querySecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "index",
		Name:        "query_seconds",
		Help:        "Total seconds of querying",
		ConstLabels: prometheus.Labels{"type": "read"},
	})
	querySecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "index",
		Name:        "query_seconds",
		Help:        "Total seconds of querying",
		ConstLabels: prometheus.Labels{"type": "write"},
	})
)

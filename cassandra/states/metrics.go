package states

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	querySecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "states",
		Name:        "query_seconds",
		Help:        "Total seconds of querying",
		ConstLabels: prometheus.Labels{"type": "read"},
	})
	querySecondsUpdate = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "states",
		Name:        "query_seconds",
		Help:        "Total seconds of querying",
		ConstLabels: prometheus.Labels{"type": "update"},
	})
	querySecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "cassandra",
		Subsystem:   "states",
		Name:        "query_seconds",
		Help:        "Total seconds of querying",
		ConstLabels: prometheus.Labels{"type": "write"},
	})
)

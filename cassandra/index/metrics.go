package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestsSecondsLabels = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "labels_seconds",
		Help:      "Total processing time in seconds (including cache processing and Cassandra querying)",
	})
	requestsSecondsUUID = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "index",
		Name:        "uuid_seconds",
		Help:        "Total processing time in seconds (including cache processing and Cassandra querying)",
		ConstLabels: prometheus.Labels{"operation": "all"},
	})
	requestsSecondsUUIDs = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "index",
		Name:        "uuids_seconds",
		Help:        "Total processing time in seconds (including Cassandra querying and match processing)",
		ConstLabels: prometheus.Labels{"type": "uuids"},
	})
)

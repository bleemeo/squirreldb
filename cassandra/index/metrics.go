package index

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	requestsSecondsLabels = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_labels_seconds",
		Help:      "Total processing time in seconds (including cache processing and Cassandra querying)",
	})
	requestsSecondsUUID = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "lookup_uuid_seconds",
		Help:      "Total processing time in seconds (including cache processing and Cassandra querying)",
	})
	requestsSecondsUUIDs = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: "squirreldb",
		Subsystem: "index",
		Name:      "search_uuids_seconds",
		Help:      "Total processing time in seconds (including Cassandra querying and match processing)",
	})
)

package distributor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	activeShard = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "squirreldb",
		Subsystem: "distributor",
		Name:      "active_shard",
		Help:      "Total shard active on this node",
	})
	pointsSendWrite = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "distributor",
		Name:        "points_send_total",
		Help:        "Total points send to another SquirrelDB",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
	pointsByShard = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "distributor",
			Name:      "points_total",
			Help:      "Total points processed by this instance and by shard",
		},
		[]string{"operation", "shard"},
	)
	requestsSecondsRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "distributor",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "read"},
	})
	requestsSecondsWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "distributor",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "write"},
	})
	requestsSecondsClusterWrite = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "distributor",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "cluster_write"},
	})
	requestsSecondsClusterRead = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "distributor",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "cluster_read"},
	})
)

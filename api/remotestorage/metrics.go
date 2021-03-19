package remotestorage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	RequestsPoints  *prometheus.CounterVec
	RequestsSeconds *prometheus.SummaryVec
	RequestsError   *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		RequestsPoints: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "remote_storage",
			Name:      "requests_points_total",
			Help:      "Total points processed by SquirrelDB remote storage",
		}, []string{"operation"}),
		RequestsSeconds: promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "remote_storage",
			Name:      "requests_seconds",
			Help:      "Total processing time in seconds (including sending response to client)",
		}, []string{"operation"}),
		RequestsError: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "remote_storage",
			Name:      "requests_error_total",
			Help:      "Total number of errors while processing requests",
		}, []string{"operation"}),
	}
}

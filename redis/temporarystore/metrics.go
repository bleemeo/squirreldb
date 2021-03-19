package temporarystore

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	OperationPoints  *prometheus.CounterVec
	OperationSeconds *prometheus.SummaryVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		OperationPoints: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "squirreldb",
				Subsystem: "redis",
				Name:      "operations_points_total",
				Help:      "Total points processed by Redis operation",
			},
			[]string{"operation"},
		),
		OperationSeconds: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "squirreldb",
				Subsystem: "redis",
				Name:      "operations_seconds",
				Help:      "Total processing time of Redis operations in seconds",
			},
			[]string{"operation"},
		),
	}
}

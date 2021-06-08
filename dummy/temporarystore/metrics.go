package temporarystore

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	MetricsTotal prometheus.Gauge
	PointsTotal  prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		MetricsTotal: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "squirreldb",
			Subsystem: "memorystore",
			Name:      "metrics",
			Help:      "Count of metrics known by the memory store",
		}),
		PointsTotal: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "squirreldb",
			Subsystem: "memorystore",
			Name:      "points",
			Help:      "Count of points stored by the memory store",
		}),
	}
}

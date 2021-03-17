package temporarystore

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	metricsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "squirreldb",
		Subsystem: "memorystore",
		Name:      "metrics_total",
		Help:      "Total metrics known by the memory store",
	})
	pointsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "squirreldb",
		Subsystem: "memorystore",
		Name:      "points_total",
		Help:      "Total points stored by the memory store",
	})
)

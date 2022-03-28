package promql

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	RequestsPoints prometheus.Histogram
	RequestsSeries prometheus.Histogram
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		RequestsPoints: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "squirreldb",
			Subsystem: "queryable",
			Name:      "requests_points",
			Help:      "Total points processed by SquirrelDB PromQL",
			Buckets:   []float64{0, 1, 5, 10, 100, 1000, 10000, 100000, 1000000},
		}),
		RequestsSeries: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "squirreldb",
			Subsystem: "queryable",
			Name:      "series_evaluated",
			Help:      "Total series evaluated by SquirrelDB PromQL",
			Buckets:   []float64{0, 1, 5, 10, 100, 1000, 10000},
		}),
	}
}

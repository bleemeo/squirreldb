package remotestorage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	RequestsPoints prometheus.Histogram
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		RequestsPoints: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "squirreldb",
			Subsystem: "remote_storage",
			Name:      "write_requests_points",
			Help:      "Total points written by SquirrelDB remote storage",
			Buckets:   []float64{0, 1, 5, 10, 100, 1000, 10000, 100000, 1000000},
		}),
	}
}

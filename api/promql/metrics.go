package promql

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	RequestsPoints  *prometheus.HistogramVec
	RequestsSeries  *prometheus.HistogramVec
	RequestsSeconds *prometheus.SummaryVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		RequestsPoints: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "squirreldb",
			Subsystem: "promql",
			Name:      "requests_points",
			Help:      "Total points processed by SquirrelDB PromQL",
			Buckets:   []float64{0, 1, 5, 10, 100, 1000, 10000, 100000, 1000000},
		}, []string{"operation"}),
		RequestsSeries: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "squirreldb",
			Subsystem: "promql",
			Name:      "series_evaluated",
			Help:      "Total series evaludated by SquirrelDB PromQL",
			Buckets:   []float64{0, 1, 5, 10, 100, 1000, 10000},
		}, []string{"operation"}),
		RequestsSeconds: promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "promql",
			Name:      "requests_seconds",
			Help:      "Total processing time in seconds (including sending response to client)",
		}, []string{"operation"}),
	}
}

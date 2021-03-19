package tsdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	AggregationSeconds      prometheus.Summary
	AggregatdUntilSeconds   prometheus.Gauge
	CassandraQueriesSeconds *prometheus.SummaryVec
	RequestsPoints          *prometheus.CounterVec
	RequestsSeconds         *prometheus.SummaryVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		AggregationSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "tsdb",
			Name:      "aggregation_seconds",
			Help:      "Total processing time spent for aggregating each shard",
		}),
		AggregatdUntilSeconds: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "squirreldb",
			Subsystem: "tsdb",
			Name:      "aggregated_until_seconds",
			Help:      "Most recent timestamp for which all shard are aggregated",
		}),
		CassandraQueriesSeconds: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "squirreldb",
				Subsystem: "tsdb",
				Name:      "cassandra_queries_seconds",
				Help:      "Total processing time spent in Cassandra itself in seconds",
			},
			[]string{"operation", "type"},
		),
		RequestsPoints: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "squirreldb",
				Subsystem: "tsdb",
				Name:      "requests_points_total",
				Help:      "Total points processed by persistent TSDB",
			},
			[]string{"operation", "type"},
		),
		RequestsSeconds: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "squirreldb",
				Subsystem: "tsdb",
				Name:      "requests_seconds",
				Help:      "Total processing time in seconds",
			},
			[]string{"operation", "type"},
		),
	}
}

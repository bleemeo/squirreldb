package mutable

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	CacheAccess *prometheus.CounterVec
	CacheSize   *prometheus.GaugeVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		CacheSize: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "squirreldb",
				Subsystem: "mutable",
				Name:      "cache_size",
				Help:      "Number of entries stored in the cache",
			},
			[]string{"cache"},
		),
		CacheAccess: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "squirreldb",
				Subsystem: "mutable",
				Name:      "cache_access_total",
				Help:      "Total number of access to the cache",
			},
			[]string{"cache", "status"},
		),
	}
}

// metricStatus returns the cache status given whether the entry was found or not.
func metricStatus(found bool) string {
	if found {
		return "hit"
	}

	return "miss"
}

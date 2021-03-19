package cluster

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	MessageSeconds *prometheus.SummaryVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		MessageSeconds: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "squirreldb",
				Subsystem: "cluster",
				Name:      "message_processing_seconds",
				Help:      "Total processing time spent to send/receive message",
			},
			[]string{"direction"},
		),
	}
}

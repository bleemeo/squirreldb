package batch

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	RequestsPoints       *prometheus.CounterVec
	RequestsSeconds      *prometheus.SummaryVec
	DuplicatedPoints     prometheus.Counter
	BackgroundSeconds    prometheus.Summary
	FlushPoints          *prometheus.CounterVec
	Takeover             *prometheus.CounterVec
	TransferOwner        prometheus.Counter
	NonOwnerWrite        prometheus.Counter
	NewPointsDuringFlush prometheus.Counter
	ConflictFlushTotal   prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		RequestsPoints: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "requests_points_total",
			Help:      "Total points processed by batcher",
		}, []string{"operation"}),
		RequestsSeconds: promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "requests_seconds",
			Help:      "Total processing time in seconds",
		}, []string{"operation"}),
		DuplicatedPoints: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "duplicated_points_total",
			Help:      "Total duplicated points eliminated by batcher during write request",
		}),
		BackgroundSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "background_seconds",
			Help:      "Total background processing time in seconds",
		}),
		FlushPoints: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "flush_points_total",
			Help:      "Total points processed by the flush() method from/to memory-store",
		}, []string{"operation"}),
		Takeover: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "takeover_total",
			Help:      "Total number of metrics take-over on this SquirrelDB. Take-over means that a SquirrelDB owner of a metrics crashed",
		}, []string{"direction"}),
		TransferOwner: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "transfer_owner_total",
			Help:      "Total number of metrics whose ownership was transferred to his SquirrelDB. Transfert means a SquirrelDB owner of a metrics cleaned stopped",
		}),
		NonOwnerWrite: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "non_owner_write_total",
			Help:      "Total number of metrics wrote to TSBD by a SquirrelDB not owner of it. This happen when lots of points got wrote on the metrics (e.g. backlog catch-up of Prometheus)",
		}),
		NewPointsDuringFlush: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "new_points_during_tsdb_write_total",
			Help:      "Total number of points arrived during the write to TSDB (which require to be re-appened to avoid loss)",
		}),
		ConflictFlushTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "batch",
			Name:      "conflict_write_memorystore_total",
			Help:      "Total number conflict during set (purge of old points) in the memory store",
		}),
	}
}

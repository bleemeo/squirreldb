// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metrics are the Prometheus metrics of the ClickHouse backend.
type metrics struct {
	FlushSeconds     prometheus.Summary
	FlushPointsTotal prometheus.Counter
	FlushErrorsTotal prometheus.Counter
	// FlushRowsPerBatch / FlushMetricsPerBatch capture the size of a single
	// clickhouse-go INSERT block the drainer sends: points (timestamps/values array
	// elements across all series) and distinct series (one block row each, since the
	// write path sends one row per series — see datapointInputDDL). clickhouse-go
	// buffers the whole block uncompressed before Send, so these reveal the peak and
	// the points-per-series ratio that drives it.
	FlushRowsPerBatch    prometheus.Histogram
	FlushMetricsPerBatch prometheus.Histogram

	RegisterSeconds       prometheus.Summary
	RegisteredSeriesTotal prometheus.Counter
	// SeenSeriesSize is the current size of the eager-registration cache, bounded
	// by the active series cardinality (not by the point throughput) — lets us rule
	// the cache out as a memory driver during a backlog.
	SeenSeriesSize prometheus.Gauge

	ReadSeconds     *prometheus.SummaryVec
	ReadSeriesTotal prometheus.Counter
	// ReadSourceTotal counts Select calls by the point source they read, "raw" or
	// "aggregated" (the pre-aggregation routing decision, see aggSelection). Instant-
	// vector snap reads (aggModeInstantSnap) count as "aggregated" too.
	ReadSourceTotal *prometheus.CounterVec

	// WAL / drainer.
	WALLength            prometheus.Gauge       // current number of buffered points in the Redis WAL
	WALDrainerIsLeader   prometheus.Gauge       // 1 if this node holds the drainer lease
	WALBackpressureTotal *prometheus.CounterVec // writes slowed/blocked, by kind

	// Best-effort (dual-write extra) drop accounting. Only ever non-zero when the
	// ClickHouse backend runs as a best-effort extra write target.
	WALDroppedPointsTotal prometheus.Counter // points dropped because ClickHouse was unavailable
	// WALDroppedMin/MaxTimestamp are the min/max point timestamps (unix seconds)
	// ever dropped by this instance — the range to re-run `squirreldb migrate`
	// over to repair from the primary. Cumulative (never reset), 0 until a drop.
	WALDroppedMinTimestamp prometheus.Gauge
	WALDroppedMaxTimestamp prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	const (
		namespace = "squirreldb"
		subSystem = "clickhouse"
	)

	return &metrics{
		FlushSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "flush_seconds",
			Help:      "Duration of a batch flush write to ClickHouse",
		}),
		FlushPointsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "flush_points_total",
			Help:      "Total points written to ClickHouse",
		}),
		FlushErrorsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "flush_errors_total",
			Help:      "Total batch flushes that failed",
		}),
		FlushRowsPerBatch: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "flush_rows_per_batch",
			Help:      "Points (timestamps/values array elements) sent to ClickHouse in a single INSERT block",
			Buckets:   prometheus.ExponentialBuckets(100, 4, 10), // 100 .. ~26M
		}),
		FlushMetricsPerBatch: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "flush_metrics_per_batch",
			Help:      "Distinct series flushed to ClickHouse in a single INSERT block",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 8), // 1 .. ~16k
		}),
		RegisterSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "register_seconds",
			Help:      "Duration of an eager tsdb_tags registration of new series (SQL mode)",
		}),
		RegisteredSeriesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "registered_series_total",
			Help:      "Total series registered or refreshed in tsdb_tags",
		}),
		SeenSeriesSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "seen_series_size",
			Help:      "Current number of series in the eager-registration cache",
		}),
		ReadSeconds: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: namespace,
				Subsystem: subSystem,
				Name:      "read_seconds",
				Help:      "Synchronous processing time of a Select, by step (SQL mode)",
			},
			[]string{"step"},
		),
		ReadSeriesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "read_series_total",
			Help:      "Total series resolved by Select (SQL mode)",
		}),
		ReadSourceTotal: newReadSourceTotal(reg, namespace, subSystem),
		WALLength: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "wal_length",
			Help:      "Current number of buffered points in the ClickHouse write-ahead log (one entry per point)",
		}),
		WALDrainerIsLeader: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "wal_drainer_is_leader",
			Help:      "1 if this node currently holds the WAL drainer lease (writes the WAL to ClickHouse), else 0",
		}),
		WALBackpressureTotal: newWALBackpressureTotal(reg, namespace, subSystem),
		WALDroppedPointsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "wal_dropped_points_total",
			Help:      "Points dropped by a best-effort (dual-write extra) ClickHouse because it was unavailable",
		}),
		WALDroppedMinTimestamp: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "wal_dropped_min_timestamp_seconds",
			Help:      "Oldest point timestamp ever dropped by a best-effort ClickHouse (repair range lower bound); 0 if none",
		}),
		WALDroppedMaxTimestamp: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "wal_dropped_max_timestamp_seconds",
			Help:      "Newest point timestamp ever dropped by a best-effort ClickHouse (repair range upper bound); 0 if none",
		}),
	}
}

// newWALBackpressureTotal builds the WAL backpressure counter with both label
// values pre-initialized to 0 (a CounterVec emits no series for an untouched
// label set), so they show even before the WAL first backs up.
func newWALBackpressureTotal(reg prometheus.Registerer, namespace, subSystem string) *prometheus.CounterVec {
	c := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subSystem,
		Name:      "wal_backpressure_total",
		Help:      "Writes slowed or blocked by WAL backpressure, by kind (slow = ramp sleep, block = held until drained)",
	}, []string{"kind"})

	c.WithLabelValues("slow")
	c.WithLabelValues("block")

	return c
}

// newReadSourceTotal builds the read-source counter with every label value
// pre-initialized to 0, so all sources show even before the first read.
func newReadSourceTotal(reg prometheus.Registerer, namespace, subSystem string) *prometheus.CounterVec {
	c := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subSystem,
		Name:      "read_source_total",
		Help:      "Select calls by point source read (raw, aggregated)",
	}, []string{"source"})

	c.WithLabelValues("raw")
	c.WithLabelValues("aggregated")

	return c
}

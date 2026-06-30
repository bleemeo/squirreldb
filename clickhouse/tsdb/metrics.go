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
	FlushSeconds            prometheus.Summary
	FlushPointsTotal        prometheus.Counter
	FlushErrorsTotal        prometheus.Counter
	FlushDroppedPointsTotal prometheus.Counter
	// FlushRowsPerBatch / FlushMetricsPerBatch capture the size of a single
	// clickhouse-go INSERT block (rows = one per point, metrics = distinct series).
	// During a backlog catch-up the batcher hands a large flush to the writer in one
	// block, and clickhouse-go buffers the whole block (with metric_name + tags
	// duplicated per row) in memory before Send — these reveal that peak.
	FlushRowsPerBatch    prometheus.Histogram
	FlushMetricsPerBatch prometheus.Histogram

	RegisterSeconds       prometheus.Summary
	RegisteredSeriesTotal prometheus.Counter
	// SeenSeriesSize is the current size of the eager-registration cache, bounded
	// by the active series cardinality (not by the point throughput) — lets us rule
	// the cache out as a memory driver during a backlog.
	SeenSeriesSize prometheus.Gauge

	ReadSeconds            *prometheus.SummaryVec
	ReadSeriesTotal        prometheus.Counter
	ReadPendingPointsTotal prometheus.Counter
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
		FlushDroppedPointsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "flush_dropped_points_total",
			Help:      "Total points dropped at flush because their labels were missing (expired) in the temporary store",
		}),
		FlushRowsPerBatch: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "flush_rows_per_batch",
			Help:      "Rows (one per point) sent to ClickHouse in a single INSERT block",
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
		ReadPendingPointsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subSystem,
			Name:      "read_pending_points_total",
			Help:      "Total batcher pending points merged into reads (SQL mode)",
		}),
	}
}

// Copyright 2015-2025 Bleemeo
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

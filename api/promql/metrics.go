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

package promql

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	RequestsPoints prometheus.Histogram
	RequestsSeries prometheus.Histogram
	CachedPoints   prometheus.Counter
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
		CachedPoints: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "queryable",
			Name:      "cached_points_total",
			Help:      "Total points processed by SquirrelDB PromQL which reads were cached",
		}),
	}
}

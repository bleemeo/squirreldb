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

package temporarystore

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	MetricsTotal prometheus.Gauge
	PointsTotal  prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		MetricsTotal: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "squirreldb",
			Subsystem: "memorystore",
			Name:      "metrics",
			Help:      "Count of metrics known by the memory store",
		}),
		PointsTotal: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "squirreldb",
			Subsystem: "memorystore",
			Name:      "points",
			Help:      "Count of points stored by the memory store",
		}),
	}
}

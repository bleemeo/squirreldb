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
	OperationPoints  *prometheus.CounterVec
	OperationSeconds *prometheus.SummaryVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		OperationPoints: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "squirreldb",
				Subsystem: "redis",
				Name:      "operations_points_total",
				Help:      "Total points processed by Redis operation",
			},
			[]string{"operation"},
		),
		OperationSeconds: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "squirreldb",
				Subsystem: "redis",
				Name:      "operations_seconds",
				Help:      "Total processing time of Redis operations in seconds",
			},
			[]string{"operation"},
		),
	}
}

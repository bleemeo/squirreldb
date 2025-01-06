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

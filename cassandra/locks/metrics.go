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

package locks

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	CassandraQueriesSeconds *prometheus.SummaryVec
	LocksLockSuccess        prometheus.Counter
	LocksLockSeconds        prometheus.Summary
	LocksUnlockSeconds      prometheus.Summary
	PendingLock             prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		CassandraQueriesSeconds: promauto.With(reg).NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "squirreldb",
				Subsystem: "locks",
				Name:      "cassandra_queries_seconds",
				Help:      "Total processing time spent in Cassandra in seconds",
			},
			[]string{"operation"},
		),
		LocksLockSuccess: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "lock_success_total",
			Help:      "Total number of successful (acquired) locks",
		}),
		LocksLockSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "lock_seconds",
			Help:      "Total time to acquire a lock (including waiting time) in seconds",
		}),
		LocksUnlockSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "unlock_seconds",
			Help:      "Total time to release a lock in seconds",
		}),
		PendingLock: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "squirreldb",
			Subsystem: "locks",
			Name:      "lock_pending",
			Help:      "Number of gorouting trying to acquire a lock",
		}),
	}
}

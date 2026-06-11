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

package requestscoped

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// storeMetrics are backend-agnostic metrics of the SquirrelDB <-> TSDB boundary,
// so the Cassandra and ClickHouse backends can be compared on the same basis.
type storeMetrics struct {
	IngestedPoints     prometheus.Counter
	WriteCommitSeconds prometheus.Summary
}

func newStoreMetrics(reg prometheus.Registerer) *storeMetrics {
	return &storeMetrics{
		IngestedPoints: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "tsdb",
			Name:      "ingested_points_total",
			Help:      "Total points written into the TSDB backend (ingress, any backend)",
		}),
		WriteCommitSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "tsdb",
			Name:      "write_commit_seconds",
			Help:      "Duration of a write commit to the TSDB backend (any backend)",
		}),
	}
}

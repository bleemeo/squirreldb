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

package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	RequestsSeconds *prometheus.SummaryVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		RequestsSeconds: promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
			Namespace: "squirreldb",
			Subsystem: "api",
			Name:      "requests_seconds",
			Help:      "Total processing time in seconds (including sending response to client)",
		}, []string{"operation"}),
	}
}

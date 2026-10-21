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

package types

import (
	"context"
	"time"
)

// The health status of a component, as reported on the admin status page
// (/debug/status). They are ordered from the best to the worst, see WorstStatus.
const (
	StatusOK       = "ok"
	StatusUnknown  = "unknown"
	StatusDegraded = "degraded"
	StatusDown     = "down"
)

// statusRank orders the statuses from the best to the worst.
var statusRank = map[string]int{ //nolint:gochecknoglobals
	StatusOK:       0,
	StatusUnknown:  1,
	StatusDegraded: 2,
	StatusDown:     3,
}

// WorstStatus returns the worst of the statuses. An unrecognized status counts as unknown.
func WorstStatus(statuses ...string) string {
	worst := StatusOK

	for _, s := range statuses {
		rank, ok := statusRank[s]
		if !ok {
			s, rank = StatusUnknown, statusRank[StatusUnknown]
		}

		if rank > statusRank[worst] {
			worst = s
		}
	}

	return worst
}

// ComponentStatus is the health of a component (a backend, the cluster, ...).
type ComponentStatus struct {
	// Status is one of StatusOK, StatusDegraded, StatusDown or StatusUnknown.
	Status string `json:"status"`
	// Reason is a short, human readable explanation of a status other than ok.
	Reason string `json:"reason,omitempty"`
	// Details are component-specific values. They must be JSON serializable and
	// must not contain secrets: they are shown as-is on the status page.
	Details any `json:"details,omitempty"`
}

// StatusProvider is implemented by the components (backends, cluster) that can
// report their health on the admin status page. It is an optional capability,
// discovered by type assertion. Status must be cheap: it should read state that
// is already known (e.g. from a health probe) and must honor ctx, which carries a
// short deadline.
type StatusProvider interface {
	Status(ctx context.Context) ComponentStatus
}

// Roles of a backend, see BackendState.
const (
	BackendRolePrimary = "primary"
	BackendRoleExtra   = "extra"
)

// BackendState describes a configured storage backend for the admin status page.
type BackendState struct {
	// Name is the backend name (cassandra, clickhouse or dummy).
	Name string
	// Role is BackendRolePrimary or BackendRoleExtra (the best-effort dual-write target).
	Role string
	// Started tells whether the backend is started (attached, for the extra backend).
	Started bool
	// UnavailableSince is when the extra backend was first found unavailable at
	// startup, zero when it never was. Points written since then were not dual-written.
	UnavailableSince time.Time
	// Backend is the started backend, nil when not started. Its optional
	// capabilities (e.g. StatusProvider) are discovered by type assertion.
	Backend any
	// Info is the BackendInfo of the started backend.
	Info BackendInfo
}

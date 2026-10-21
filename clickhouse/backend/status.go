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

package backend

import (
	"context"
	"fmt"

	"github.com/bleemeo/squirreldb/clickhouse/routing"
	"github.com/bleemeo/squirreldb/clickhouse/tsdb"
	"github.com/bleemeo/squirreldb/types"
)

var _ types.StatusProvider = (*Backend)(nil)

// Host states on the status page.
const (
	hostReady     = "ready"
	hostReachable = "reachable"
	hostDown      = "down"
)

// Status are the details of the ClickHouse backend on the admin status page.
type Status struct {
	Database    string `json:"database"`
	ClusterName string `json:"cluster_name,omitempty"`
	ReadOnly    bool   `json:"read_only"`
	// Hosts is the health view of the routing monitor, refreshed by its periodic probe.
	Hosts      []HostStatus             `json:"hosts"`
	QueryLimit routing.QueryLimitStatus `json:"query_limit"`
	// WAL is nil until the TSDB is built.
	WAL *tsdb.WALStatus `json:"wal"`
}

// HostStatus is the health of one ClickHouse host.
type HostStatus struct {
	routing.HostStatus

	// State is "ready" (routed to), "reachable" (answers but is excluded, see
	// Cause) or "down".
	State string `json:"state"`
}

// Status reports the ClickHouse health (types.StatusProvider). The hosts come from
// the routing health monitor, so the only request it sends is the WAL length read.
func (b *Backend) Status(ctx context.Context) types.ComponentStatus {
	b.l.Lock()
	conn := b.routing
	t := b.tsdb
	b.l.Unlock()

	if conn == nil {
		return types.ComponentStatus{Status: types.StatusDown, Reason: "ClickHouse backend not started"}
	}

	details := Status{
		Database:    b.options.Config.Database,
		ClusterName: b.options.ClusterName,
		ReadOnly:    b.options.ReadOnly,
		QueryLimit:  conn.QueryLimit(),
	}

	health := hostsHealth(conn.Hosts(), &details)

	if t != nil {
		wal, walHealth := t.WALStatus(ctx)
		details.WAL = &wal

		if types.WorstStatus(health.Status, walHealth.Status) != health.Status {
			health.Status, health.Reason = walHealth.Status, walHealth.Reason
		}
	}

	health.Details = details

	return health
}

// hostsHealth fills the hosts of details and returns their overall health: down
// when no host answers, degraded when some host is not ready.
func hostsHealth(hosts []routing.HostStatus, details *Status) types.ComponentStatus {
	var reachable, ready int

	details.Hosts = make([]HostStatus, 0, len(hosts))

	for _, h := range hosts {
		state := hostDown

		switch {
		case h.Ready:
			state = hostReady
			ready++
			reachable++
		case h.Reachable:
			state = hostReachable
			reachable++
		}

		details.Hosts = append(details.Hosts, HostStatus{HostStatus: h, State: state})
	}

	switch {
	case reachable == 0:
		return types.ComponentStatus{Status: types.StatusDown, Reason: "no ClickHouse host is reachable"}
	case ready == 0:
		return types.ComponentStatus{
			Status: types.StatusDegraded,
			Reason: fmt.Sprintf("no ClickHouse host is ready (%d reachable), see the host causes", reachable),
		}
	case ready < len(hosts):
		return types.ComponentStatus{
			Status: types.StatusDegraded,
			Reason: fmt.Sprintf("%d of %d ClickHouse hosts are ready", ready, len(hosts)),
		}
	default:
		return types.ComponentStatus{Status: types.StatusOK}
	}
}

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
	"time"

	"github.com/bleemeo/squirreldb/types"
)

var _ types.StatusProvider = (*Backend)(nil)

// Status are the details of the Cassandra backend on the admin status page.
type Status struct {
	Addresses []string `json:"addresses"`
	Keyspace  string   `json:"keyspace"`
	ReadOnly  bool     `json:"read_only"`
	// Nodes is the number of Cassandra nodes known by the coordinator (system.peers
	// plus itself), nil when it could not be read.
	Nodes *int `json:"nodes"`
	// SessionOpenedAt is when the current session was opened.
	SessionOpenedAt *time.Time `json:"session_opened_at"`
}

// Status checks the Cassandra connectivity (types.StatusProvider) with the query
// the connection health check uses, plus a count of system.peers.
func (b *Backend) Status(ctx context.Context) types.ComponentStatus {
	b.l.Lock()
	conn := b.connection
	b.l.Unlock()

	details := Status{
		Addresses: b.options.CassandraConfig.Addresses,
		Keyspace:  b.options.CassandraConfig.Keyspace,
		ReadOnly:  b.options.ReadOnly,
	}

	if conn == nil {
		return types.ComponentStatus{Status: types.StatusDown, Reason: "Cassandra backend not started", Details: details}
	}

	if opened := conn.LastConnectionEstablished(); !opened.IsZero() {
		details.SessionOpenedAt = &opened
	}

	session, err := conn.SessionReadOnly()
	if err != nil {
		return types.ComponentStatus{
			Status:  types.StatusDown,
			Reason:  "no Cassandra session: " + err.Error(),
			Details: details,
		}
	}

	defer session.Close()

	if err := session.Query("SELECT host_id FROM system.local WHERE key='local'").WithContext(ctx).Exec(); err != nil {
		return types.ComponentStatus{
			Status:  types.StatusDown,
			Reason:  "Cassandra unreachable: " + err.Error(),
			Details: details,
		}
	}

	count := 0
	it := session.Query("SELECT peer FROM system.peers").WithContext(ctx).Iter()

	for it.Scan(nil) {
		count++
	}

	if err := it.Close(); err != nil {
		return types.ComponentStatus{
			Status:  types.StatusDegraded,
			Reason:  "cannot list the Cassandra peers: " + err.Error(),
			Details: details,
		}
	}

	nodes := count + 1 // The coordinator is not in its own peers.
	details.Nodes = &nodes

	return types.ComponentStatus{Status: types.StatusOK, Details: details}
}

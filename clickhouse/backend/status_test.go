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
	"encoding/json"
	"strings"
	"testing"

	"github.com/bleemeo/squirreldb/clickhouse/routing"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/types"

	"github.com/rs/zerolog"
)

// TestHostsHealth checks the health derived from a (fake) routing health view.
func TestHostsHealth(t *testing.T) {
	t.Parallel()

	lag := 2.0
	ready := routing.HostStatus{Address: "ch1:9000", Reachable: true, Ready: true, ReplicaLagSeconds: &lag}
	lagging := routing.HostStatus{Address: "ch2:9000", Reachable: true, Cause: "replica is lagging"}
	down := routing.HostStatus{Address: "ch3:9000", Cause: "connection refused"}

	cases := []struct {
		name       string
		hosts      []routing.HostStatus
		wantStatus string
		wantReason string
		wantStates []string
	}{
		{
			name:       "all ready",
			hosts:      []routing.HostStatus{ready},
			wantStatus: types.StatusOK,
			wantStates: []string{hostReady},
		},
		{
			name:       "one host not ready",
			hosts:      []routing.HostStatus{ready, lagging, down},
			wantStatus: types.StatusDegraded,
			wantReason: "1 of 3 ClickHouse hosts are ready",
			wantStates: []string{hostReady, hostReachable, hostDown},
		},
		{
			name:       "reachable but none ready",
			hosts:      []routing.HostStatus{lagging, down},
			wantStatus: types.StatusDegraded,
			wantReason: "no ClickHouse host is ready (1 reachable)",
			wantStates: []string{hostReachable, hostDown},
		},
		{
			name:       "all down",
			hosts:      []routing.HostStatus{down},
			wantStatus: types.StatusDown,
			wantReason: "no ClickHouse host is reachable",
			wantStates: []string{hostDown},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var details Status

			health := hostsHealth(tc.hosts, &details)
			if health.Status != tc.wantStatus || !strings.Contains(health.Reason, tc.wantReason) {
				t.Errorf("health = %+v, want %q containing %q", health, tc.wantStatus, tc.wantReason)
			}

			if len(details.Hosts) != len(tc.wantStates) {
				t.Fatalf("hosts = %+v, want %d hosts", details.Hosts, len(tc.wantStates))
			}

			for i, want := range tc.wantStates {
				if details.Hosts[i].State != want {
					t.Errorf("host %s state = %q, want %q", details.Hosts[i].Address, details.Hosts[i].State, want)
				}
			}
		})
	}
}

// TestHostStatusJSON checks the routing fields are flattened into the host status.
func TestHostStatusJSON(t *testing.T) {
	t.Parallel()

	lag := 1.5

	data, err := json.Marshal(HostStatus{
		Address:           "ch1:9000",
		Reachable:         true,
		Ready:             true,
		ReplicaLagSeconds: &lag,
		State:             hostReady,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{`"address":"ch1:9000"`, `"state":"ready"`, `"replica_lag_seconds":1.5`, `"last_probe"`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("JSON %s does not contain %s", data, want)
		}
	}
}

func TestStatusNotStarted(t *testing.T) {
	t.Parallel()

	b, err := New(t.Context(), zerolog.Nop(), Options{Config: config.ClickHouse{Password: "secret"}})
	if err != nil {
		t.Fatal(err)
	}

	status := b.Status(t.Context())
	if status.Status != types.StatusDown {
		t.Errorf("status = %+v, want down", status)
	}
}

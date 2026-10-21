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

package status

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/api/auth/authtest"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	dummybackend "github.com/bleemeo/squirreldb/dummy/backend"
	"github.com/bleemeo/squirreldb/types"

	"github.com/rs/zerolog"
)

// fakeBackend is a backend whose status is provided by a function.
type fakeBackend struct {
	status func(ctx context.Context) types.ComponentStatus
}

func (b fakeBackend) Status(ctx context.Context) types.ComponentStatus {
	return b.status(ctx)
}

// fakeCluster is a cluster that reports its status.
type fakeCluster struct {
	dummy.LocalCluster

	status types.ComponentStatus
}

func (c *fakeCluster) Status(context.Context) types.ComponentStatus {
	return c.status
}

func dummyBackendStates(t *testing.T) func() []types.BackendState {
	t.Helper()

	backend, err := dummybackend.New(t.Context(), zerolog.Nop(), dummybackend.Options{Batcher: true})
	if err != nil {
		t.Fatal(err)
	}

	return func() []types.BackendState {
		return []types.BackendState{{
			Name:    config.BackendDummy,
			Role:    types.BackendRolePrimary,
			Started: true,
			Backend: backend,
			Info:    backend.BackendInfo(),
		}}
	}
}

func newTestCollector(t *testing.T, sources Sources) *Collector {
	t.Helper()

	return New(Options{
		Sources:            sources,
		StartTime:          time.Now().Add(-time.Hour),
		ListenAddress:      "localhost:9201",
		AdminListenAddress: "localhost:9202",
		Ready:              func() bool { return true },
		SectionTimeout:     time.Second,
	})
}

// toJSON returns the report as decoded JSON, to check the JSON shape.
func toJSON(t *testing.T, report *Report) map[string]any {
	t.Helper()

	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}

	return result
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()

	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}

// lookup returns the value at the dotted path of a decoded JSON object.
func lookup(t *testing.T, value any, path string) any {
	t.Helper()

	for key := range strings.SplitSeq(path, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("%s: %q is not an object in %v", path, key, value)
		}

		value, ok = object[key]
		if !ok {
			t.Fatalf("%s: missing key %q", path, key)
		}
	}

	return value
}

func TestCollectDummy(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy

	collector := newTestCollector(t, Sources{
		Backends:  dummyBackendStates(t),
		Cluster:   func() types.Cluster { return &dummy.LocalCluster{} },
		Telemetry: func() (string, string) { return "cluster-id", "node-id" },
		Config:    cfg,
	})

	report := collector.Collect(t.Context())
	if report.Status != types.StatusOK {
		t.Errorf("status = %q (%v), want ok", report.Status, report.Reasons)
	}

	got := toJSON(t, report)

	want := map[string]any{
		"status":                            types.StatusOK,
		"build.status":                      types.StatusOK,
		"build.version":                     "unset",
		"node.status":                       types.StatusOK,
		"node.ready":                        true,
		"node.listen_address":               "localhost:9201",
		"node.admin_listen_address":         "localhost:9202",
		"node.telemetry_cluster_id":         "cluster-id",
		"node.telemetry_node_id":            "node-id",
		"backends.status":                   types.StatusOK,
		"backends.primary.name":             config.BackendDummy,
		"backends.primary.started":          true,
		"backends.primary.status":           types.StatusOK,
		"backends.extra.configured":         false,
		"dummy.status":                      types.StatusOK,
		"dummy.details.batcher":             true,
		"cluster.status":                    types.StatusOK,
		"cluster.kind":                      "local",
		"cluster.size":                      float64(1),
		"security.status":                   types.StatusOK,
		"security.auth_enabled":             false,
		"security.tls.enabled":              false,
		"config.status":                     types.StatusOK,
		"config.values.backend":             config.BackendDummy,
		"config.values.listen_address":      cfg.ListenAddress,
		"config.values.batch.size":          cfg.Batch.Size.String(),
		"config.values.cassandra.addresses": []any{cfg.Cassandra.Addresses[0]},
	}

	for path, wantValue := range want {
		gotValue := lookup(t, got, path)

		gotJSON := mustJSON(t, gotValue)
		wantJSON := mustJSON(t, wantValue)

		if gotJSON != wantJSON {
			t.Errorf("%s = %s, want %s", path, gotJSON, wantJSON)
		}
	}

	for _, key := range []string{"clickhouse", "cassandra"} {
		if _, ok := got[key]; ok {
			t.Errorf("section %q is present without the backend", key)
		}
	}
}

// TestCollectSlowAndFailingCollectors checks that a collector that is too slow or
// panics yields an error in its section, not a failed report.
func TestCollectSlowAndFailingCollectors(t *testing.T) {
	t.Parallel()

	block := make(chan struct{})
	defer close(block)

	collector := New(Options{
		Backends: func() []types.BackendState {
			return []types.BackendState{
				{
					Name: config.BackendClickHouse, Role: types.BackendRolePrimary, Started: true,
					Backend: fakeBackend{status: func(context.Context) types.ComponentStatus {
						<-block // Ignores the context, like a stuck collector.

						return types.ComponentStatus{Status: types.StatusOK}
					}},
				},
				{
					Name: config.BackendCassandra, Role: types.BackendRoleExtra, Started: true,
					Backend: fakeBackend{status: func(context.Context) types.ComponentStatus {
						panic("boom")
					}},
				},
			}
		},
		Cluster:        func() types.Cluster { panic("cluster boom") },
		Ready:          func() bool { return true },
		SectionTimeout: 50 * time.Millisecond,
	})

	start := time.Now()
	report := collector.Collect(t.Context())

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Collect took %s, want about the section timeout", elapsed)
	}

	if report.ClickHouse == nil || !strings.Contains(report.ClickHouse.Error, "did not answer") {
		t.Errorf("clickhouse section = %+v, want a timeout error", report.ClickHouse)
	}

	if report.Cassandra == nil || !strings.Contains(report.Cassandra.Error, "boom") {
		t.Errorf("cassandra section = %+v, want the panic error", report.Cassandra)
	}

	if report.Cluster.Status != types.StatusUnknown || !strings.Contains(report.Cluster.Error, "cluster boom") {
		t.Errorf("cluster section = %+v, want the panic error", report.Cluster)
	}

	if report.Backends.Primary.Status != types.StatusUnknown {
		t.Errorf("primary status = %q, want unknown", report.Backends.Primary.Status)
	}

	// The other sections are still collected.
	if report.Node.Status != types.StatusOK || report.Build.Version == "" {
		t.Errorf("node = %+v, build = %+v, want them collected", report.Node, report.Build)
	}

	if report.Status != types.StatusUnknown {
		t.Errorf("status = %q, want unknown", report.Status)
	}
}

func TestBackendsHealth(t *testing.T) {
	t.Parallel()

	healthy := types.ComponentStatus{Status: types.StatusOK}
	down := types.ComponentStatus{Status: types.StatusDown, Reason: "unreachable"}

	cases := []struct {
		name       string
		states     []types.BackendState
		wantStatus string
		wantReason string
	}{
		{
			name: "extra down is degraded",
			states: []types.BackendState{
				{Name: "cassandra", Role: types.BackendRolePrimary, Started: true, Backend: fakeStatus(healthy)},
				{Name: "clickhouse", Role: types.BackendRoleExtra, Started: true, Backend: fakeStatus(down)},
			},
			wantStatus: types.StatusDegraded,
			wantReason: "extra clickhouse: unreachable",
		},
		{
			name: "extra not attached is degraded",
			states: []types.BackendState{
				{Name: "cassandra", Role: types.BackendRolePrimary, Started: true, Backend: fakeStatus(healthy)},
				{
					Name: "clickhouse", Role: types.BackendRoleExtra,
					UnavailableSince: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			wantStatus: types.StatusDegraded,
			wantReason: "repair from 2026-09-01T00:00:00Z",
		},
		{
			name: "primary down",
			states: []types.BackendState{
				{Name: "clickhouse", Role: types.BackendRolePrimary, Started: true, Backend: fakeStatus(down)},
			},
			wantStatus: types.StatusDown,
			wantReason: "primary clickhouse: unreachable",
		},
		{
			name: "without status provider",
			states: []types.BackendState{
				{Name: "cassandra", Role: types.BackendRolePrimary, Started: true, Backend: struct{}{}},
			},
			wantStatus: types.StatusUnknown,
			wantReason: "does not report its status",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			report := newTestCollector(t, Sources{
				Backends: func() []types.BackendState { return tc.states },
			}).Collect(t.Context())

			if report.Backends.Status != tc.wantStatus || !strings.Contains(report.Backends.Reason, tc.wantReason) {
				t.Errorf("backends = %q %q, want %q containing %q",
					report.Backends.Status, report.Backends.Reason, tc.wantStatus, tc.wantReason)
			}

			if report.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", report.Status, tc.wantStatus)
			}
		})
	}
}

func fakeStatus(status types.ComponentStatus) fakeBackend {
	return fakeBackend{status: func(context.Context) types.ComponentStatus { return status }}
}

func TestCollectRedisCluster(t *testing.T) {
	t.Parallel()

	cluster := &fakeCluster{status: types.ComponentStatus{Status: types.StatusDown, Reason: "Redis unreachable"}}

	report := newTestCollector(t, Sources{
		Cluster: func() types.Cluster { return cluster },
	}).Collect(t.Context())

	if report.Cluster.Kind != "redis" || report.Cluster.Status != types.StatusDown {
		t.Errorf("cluster = %+v, want redis down", report.Cluster)
	}

	if !strings.Contains(strings.Join(report.Reasons, "\n"), "cluster: Redis unreachable") {
		t.Errorf("reasons = %v, want the cluster reason", report.Reasons)
	}
}

func TestCollectSecurity(t *testing.T) {
	t.Parallel()

	certs := authtest.WriteCertificates(t, t.TempDir())

	collector := New(Options{
		Ready: func() bool { return true },
		TLS: auth.TLSOptions{
			CertFile:       certs.ServerCertFile,
			KeyFile:        certs.ServerKeyFile,
			ClientCAFile:   certs.CAFile,
			ClientAuthType: "RequireAndVerifyClientCert",
		},
		Auth: auth.Options{
			Enabled: true,
			Users: []auth.User{
				{Name: "admin", PasswordHash: "$2a$10$secret-password-hash", Scopes: []string{"admin"}},
				{
					Name: "tenant", TokenSHA256: "secret-token-hash", Tenant: "acme",
					Scopes: []string{"read"}, ForcedMatcher: `env="prod"`,
				},
			},
		},
		AuthFailures: func() map[string]float64 { return map[string]float64{"invalid_credentials": 3} },
	})

	report := collector.Collect(t.Context())
	security := report.Security

	// The test certificate expires in an hour.
	if security.Status != types.StatusDegraded || !strings.Contains(security.Reason, "expires") {
		t.Errorf("security = %q %q, want degraded because the certificate expires soon",
			security.Status, security.Reason)
	}

	if security.TLS.Certificate == nil || security.TLS.Certificate.ExpiresInDays > 1 {
		t.Errorf("certificate = %+v, want one expiring in less than a day", security.TLS.Certificate)
	}

	if !security.TLS.ClientCA || security.TLS.ClientAuthType != "RequireAndVerifyClientCert" {
		t.Errorf("tls = %+v, want the client CA and auth type", security.TLS)
	}

	wantUsers := []User{
		{Name: "admin", Scopes: []string{"admin"}, Methods: []string{"basic"}},
		{Name: "tenant", Scopes: []string{"read"}, Tenant: "acme", Methods: []string{"bearer"}, HasForcedMatcher: true},
	}

	gotUsers := mustJSON(t, security.Users)
	wantJSON := mustJSON(t, wantUsers)

	if gotUsers != wantJSON {
		t.Errorf("users = %s, want %s", gotUsers, wantJSON)
	}

	if security.AuthFailures["invalid_credentials"] != 3 {
		t.Errorf("auth failures = %v", security.AuthFailures)
	}

	data := mustJSON(t, report)
	for _, secret := range []string{"secret-password-hash", "secret-token-hash", `env=\"prod\"`} {
		if strings.Contains(data, secret) {
			t.Errorf("the report contains %q", secret)
		}
	}
}

func TestCollectTLSCertificateErrors(t *testing.T) {
	t.Parallel()

	report := New(Options{
		Ready: func() bool { return true },
		TLS:   auth.TLSOptions{CertFile: "/does/not/exist.pem", KeyFile: "/does/not/exist.key"},
	}).Collect(t.Context())

	if report.Security.Status != types.StatusDegraded || report.Security.TLS.CertificateError == "" {
		t.Errorf("security = %+v, want degraded with a certificate error", report.Security)
	}
}

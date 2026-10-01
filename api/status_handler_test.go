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

package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/api/status"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	dummybackend "github.com/bleemeo/squirreldb/dummy/backend"
	"github.com/bleemeo/squirreldb/types"

	"github.com/rs/zerolog"
)

// statusSecrets are the secrets of statusTestConfig, that must never be served.
var statusSecrets = []string{ //nolint:gochecknoglobals
	"redis-secret-password",
	"clickhouse-secret-password",
	"sentry-secret-key",
	"$2a$10$secret-bcrypt-hash",
	"secret-token-sha256",
}

func statusTestConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy
	cfg.Redis.Password = "redis-secret-password"
	cfg.ClickHouse.Password = "clickhouse-secret-password"
	cfg.Sentry.DSN = "https://sentry-secret-key@o1.ingest.sentry.io/42"
	cfg.Auth.Users = []config.AuthUser{
		{Name: "admin", PasswordHash: "$2a$10$secret-bcrypt-hash", Scopes: []string{"admin"}},
		{Name: "writer", TokenSHA256: "secret-token-sha256", Scopes: []string{"write"}},
	}

	return cfg
}

func statusTestSources(t *testing.T) status.Sources {
	t.Helper()

	backend, err := dummybackend.New(t.Context(), zerolog.Nop(), dummybackend.Options{})
	if err != nil {
		t.Fatal(err)
	}

	return status.Sources{
		Backends: func() []types.BackendState {
			return []types.BackendState{{
				Name: config.BackendDummy, Role: types.BackendRolePrimary, Started: true, Backend: backend,
			}}
		},
		Cluster: func() types.Cluster { return &dummy.LocalCluster{} },
		Config:  statusTestConfig(),
	}
}

func makeStatusTestAPI(t *testing.T, adminListenAddress string, authOptions auth.Options) *API {
	t.Helper()

	api := makeAPI(dummy.DiscardTSDB{}, nil)
	api.AdminListenAddress = adminListenAddress
	api.Logger = zerolog.Nop()
	api.Auth = authOptions
	api.Status = statusTestSources(t)

	if err := api.init(t.Context()); err != nil {
		t.Fatal(err)
	}

	api.Ready()

	return api
}

func checkStatusSecurityHeaders(t *testing.T, header http.Header) {
	t.Helper()

	want := map[string]string{
		"Cache-Control":          "no-store",
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
	}

	for name, value := range want {
		if got := header.Get(name); got != value {
			t.Errorf("header %s = %q, want %q", name, got, value)
		}
	}

	csp := header.Get("Content-Security-Policy")
	for _, directive := range []string{"default-src 'none'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, directive) {
			t.Errorf("Content-Security-Policy = %q, want %q", csp, directive)
		}
	}
}

func checkNoSecret(t *testing.T, body string) {
	t.Helper()

	for _, secret := range statusSecrets {
		if strings.Contains(body, secret) {
			t.Errorf("the response contains the secret %q", secret)
		}
	}
}

func TestStatusEndpoint(t *testing.T) {
	t.Parallel()

	api := makeStatusTestAPI(t, "", auth.Options{})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/debug/status", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}

	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want JSON", ct)
	}

	checkStatusSecurityHeaders(t, w.Header())

	body, _ := io.ReadAll(w.Body)
	checkNoSecret(t, string(body))

	var report map[string]any
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	for _, key := range []string{"status", "build", "node", "backends", "dummy", "cluster", "security", "config"} {
		if _, ok := report[key]; !ok {
			t.Errorf("missing section %q", key)
		}
	}

	if report["status"] != types.StatusOK {
		t.Errorf("status = %v (%v), want ok", report["status"], report["reasons"])
	}
}

// TestStatusEndpointAuth checks that the status requires the admin scope, and is
// only on the admin listener when admin_listen_address is set.
func TestStatusEndpointAuth(t *testing.T) {
	t.Parallel()

	statusRoute := get("/debug/status")

	t.Run("without admin listener", func(t *testing.T) {
		t.Parallel()

		api := makeStatusTestAPI(t, "", testAuthOptions(t))

		checkAuthRoutes(t, "main", api, []authRouteCheck{
			{routeCheck: statusRoute, wantStatus: http.StatusUnauthorized},
			{routeCheck: statusRoute, user: "reader", wantStatus: http.StatusForbidden},
			{routeCheck: statusRoute, user: "writer", wantStatus: http.StatusForbidden},
			{routeCheck: statusRoute, user: "admin", wantStatus: http.StatusOK},
		})
	})

	t.Run("with admin listener", func(t *testing.T) {
		t.Parallel()

		api := makeStatusTestAPI(t, "localhost:0", testAuthOptions(t))
		admin := http.HandlerFunc(api.serveAdminHTTP)

		checkAuthRoutes(t, "main", api, []authRouteCheck{
			{routeCheck: statusRoute, user: "reader", wantStatus: http.StatusForbidden},
			{routeCheck: statusRoute, user: "admin", wantStatus: http.StatusNotFound},
		})
		checkAuthRoutes(t, "admin", admin, []authRouteCheck{
			{routeCheck: statusRoute, wantStatus: http.StatusUnauthorized},
			{routeCheck: statusRoute, user: "reader", wantStatus: http.StatusForbidden},
			{routeCheck: statusRoute, user: "admin", wantStatus: http.StatusOK},
		})
	})
}

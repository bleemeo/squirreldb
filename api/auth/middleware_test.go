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

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"
)

const (
	readerPassword = "reader-secret"
	writerToken    = "writer-token-with-high-entropy"
	adminPassword  = "admin-secret"
)

func mustHash(t *testing.T, password string) string {
	t.Helper()

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	return string(hash)
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))

	return hex.EncodeToString(sum[:])
}

func testUsers(t *testing.T) []User {
	t.Helper()

	return []User{
		{
			Name:          "reader",
			PasswordHash:  mustHash(t, readerPassword),
			Scopes:        []string{"read"},
			ForcedMatcher: `env="prod"`,
		},
		{
			Name:        "writer",
			TokenSHA256: tokenHash(writerToken),
			Tenant:      "acme",
			Scopes:      []string{"write", "read"},
		},
		{
			Name:         "admin",
			PasswordHash: mustHash(t, adminPassword),
			Scopes:       []string{"admin"},
		},
	}
}

func newTestAuthenticator(t *testing.T, metricsRequireAuth bool) (*Authenticator, *prometheus.Registry) {
	t.Helper()

	reg := prometheus.NewRegistry()

	a, err := New(Options{
		Enabled:            true,
		MetricsRequireAuth: metricsRequireAuth,
		Users:              testUsers(t),
		MetricRegistry:     reg,
		Logger:             zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}

	return a, reg
}

// capturingHandler records the identity and tenant header of the last request it served.
type capturingHandler struct {
	identity *Identity
	tenant   string
	served   bool
}

func (h *capturingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.served = true
	h.identity = IdentityFromContext(r.Context())
	h.tenant = r.Header.Get(types.HeaderTenant)

	w.WriteHeader(http.StatusOK)
}

type credentials func(r *http.Request)

func basic(username, password string) credentials {
	return func(r *http.Request) { r.SetBasicAuth(username, password) }
}

func bearer(token string) credentials {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

func header(name, value string) credentials {
	return func(r *http.Request) { r.Header.Set(name, value) }
}

func serve(t *testing.T, handler http.Handler, method, path string, creds ...credentials) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, path, nil)
	for _, c := range creds {
		c(req)
	}

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	return w
}

func TestDisabledIsPassthrough(t *testing.T) {
	t.Parallel()

	a, err := New(Options{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}

	next := &capturingHandler{}

	if handler := a.Middleware(ClassifyRoute, next); handler != http.Handler(next) {
		t.Error("Middleware changed the handler while authentication is disabled")
	}

	for _, path := range []string{"/api/v1/query", "/api/v1/write", "/debug/flush", "/metrics"} {
		if w := serve(t, a.Middleware(ClassifyRoute, next), http.MethodGet, path); w.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want 200", path, w.Code)
		}
	}

	if next.identity != nil {
		t.Errorf("identity = %v, want nil", next.identity)
	}
}

func TestNewRequiresUsers(t *testing.T) {
	t.Parallel()

	if _, err := New(Options{Enabled: true}); err == nil {
		t.Error("New succeeded with authentication enabled and no user")
	}
}

func TestCredentials(t *testing.T) {
	t.Parallel()

	a, reg := newTestAuthenticator(t, false)

	cases := []struct {
		name       string
		creds      []credentials
		wantStatus int
		wantUser   string
		wantReason string
	}{
		{name: "no credentials", wantStatus: http.StatusUnauthorized, wantReason: reasonMissingCredentials},
		{name: "basic ok", creds: []credentials{basic("reader", readerPassword)}, wantStatus: 200, wantUser: "reader"},
		{
			name:       "basic wrong password",
			creds:      []credentials{basic("reader", "wrong")},
			wantStatus: http.StatusUnauthorized,
			wantReason: reasonInvalidCredentials,
		},
		{
			name:       "basic unknown user",
			creds:      []credentials{basic("nobody", readerPassword)},
			wantStatus: http.StatusUnauthorized,
			wantReason: reasonInvalidCredentials,
		},
		{
			name:       "basic for a token-only user",
			creds:      []credentials{basic("writer", writerToken)},
			wantStatus: http.StatusUnauthorized,
			wantReason: reasonInvalidCredentials,
		},
		{name: "bearer ok", creds: []credentials{bearer(writerToken)}, wantStatus: 200, wantUser: "writer"},
		{
			name:       "bearer wrong token",
			creds:      []credentials{bearer("wrong")},
			wantStatus: http.StatusUnauthorized,
			wantReason: reasonInvalidCredentials,
		},
		{
			name:       "bearer with the token hash",
			creds:      []credentials{bearer(tokenHash(writerToken))},
			wantStatus: http.StatusUnauthorized,
			wantReason: reasonInvalidCredentials,
		},
		{
			name:       "unsupported scheme",
			creds:      []credentials{header("Authorization", "Digest abc")},
			wantStatus: http.StatusUnauthorized,
			wantReason: reasonMalformedCredentials,
		},
		{
			name:       "invalid basic encoding",
			creds:      []credentials{header("Authorization", "Basic !!!")},
			wantStatus: http.StatusUnauthorized,
			wantReason: reasonMalformedCredentials,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Not parallel: the failures metric is checked after each case.
			next := &capturingHandler{}

			var before float64
			if tc.wantReason != "" {
				before = testutil.ToFloat64(a.failures.WithLabelValues(tc.wantReason))
			}

			w := serve(t, a.Middleware(ClassifyRoute, next), http.MethodGet, "/api/v1/query", tc.creds...)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantStatus)
			}

			if tc.wantStatus == http.StatusUnauthorized {
				if got := w.Header().Get("WWW-Authenticate"); got != `Basic realm="SquirrelDB"` {
					t.Errorf("WWW-Authenticate = %q", got)
				}

				if next.served {
					t.Error("the request was served")
				}

				after := testutil.ToFloat64(a.failures.WithLabelValues(tc.wantReason))
				if after != before+1 {
					t.Errorf("failures{reason=%q} = %v, want %v", tc.wantReason, after, before+1)
				}

				if strings.Contains(w.Body.String(), readerPassword) {
					t.Error("the response contains the password")
				}

				return
			}

			if next.identity == nil || next.identity.Name != tc.wantUser {
				t.Errorf("identity = %v, want user %s", next.identity, tc.wantUser)
			}
		})
	}

	if n, err := testutil.GatherAndCount(reg, "squirreldb_api_auth_failures_total"); err != nil || n != 6 {
		t.Errorf("got %d auth failures series (err=%v), want 6", n, err)
	}
}

func TestScopes(t *testing.T) {
	t.Parallel()

	a, _ := newTestAuthenticator(t, false)

	reader := basic("reader", readerPassword)
	writer := bearer(writerToken)
	admin := basic("admin", adminPassword)

	cases := []struct {
		method   string
		path     string
		classify RouteClassifier
		scope    Scope
	}{
		{http.MethodGet, "/api/v1/query", ClassifyRoute, ScopeRead},
		{http.MethodPost, "/api/v1/query_range", ClassifyRoute, ScopeRead},
		{http.MethodGet, "/api/v1/series", ClassifyRoute, ScopeRead},
		{http.MethodGet, "/api/v1/labels", ClassifyRoute, ScopeRead},
		{http.MethodGet, "/api/v1/label/job/values", ClassifyRoute, ScopeRead},
		{http.MethodGet, "/api/v1/metadata", ClassifyRoute, ScopeRead},
		{http.MethodGet, "/api/v1/query_exemplars", ClassifyRoute, ScopeRead},
		{http.MethodGet, "/api/v1/status/buildinfo", ClassifyRoute, ScopeRead},
		{http.MethodPost, "/api/v1/read", ClassifyRoute, ScopeRead},
		{http.MethodPost, "/api/v1/write", ClassifyRoute, ScopeWrite},
		{http.MethodPost, "/api/v1/otlp/v1/metrics", ClassifyRoute, ScopeWrite},
		// Destructive Prometheus API, admin endpoints and unknown routes require admin.
		{http.MethodDelete, "/api/v1/series", ClassifyRoute, ScopeAdmin},
		{http.MethodPost, "/api/v1/admin/tsdb/snapshot", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/api/v1/label/a/b/values", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/api/v1/query/", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/api/v1/status/", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/api/v1/some_new_route", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/api/v1", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/debug/index_info", ClassifyRoute, ScopeAdmin},
		{http.MethodPost, "/debug/flush", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/debug/clickhouse/info", ClassifyRoute, ScopeAdmin},
		{http.MethodPost, "/mutable/names", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/unknown", ClassifyRoute, ScopeAdmin},
		{http.MethodGet, "/", ClassifyRoute, ScopeAdmin},
		// On the admin listener, everything requires admin.
		{http.MethodPost, "/debug/flush", ClassifyAdminRoute, ScopeAdmin},
		{http.MethodPost, "/mutable/values", ClassifyAdminRoute, ScopeAdmin},
		{http.MethodGet, "/api/v1/query", ClassifyAdminRoute, ScopeAdmin},
		{http.MethodPost, "/api/v1/write", ClassifyAdminRoute, ScopeAdmin},
	}

	users := map[Scope]credentials{ScopeRead: reader, ScopeWrite: writer, ScopeAdmin: admin}
	// writer also has the read scope.
	granted := map[Scope][]Scope{ScopeRead: {ScopeRead}, ScopeWrite: {ScopeWrite, ScopeRead}, ScopeAdmin: {ScopeAdmin}}

	for _, tc := range cases {
		if got := tc.classify(httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, nil)); got != tc.scope {
			t.Errorf("%s %s: classified as %q, want %q", tc.method, tc.path, got, tc.scope)
		}

		for userScope, creds := range users {
			want := http.StatusForbidden

			for _, s := range granted[userScope] {
				if s == tc.scope {
					want = http.StatusOK
				}
			}

			w := serve(t, a.Middleware(tc.classify, &capturingHandler{}), tc.method, tc.path, creds)
			if w.Code != want {
				t.Errorf("%s %s as %s user: status = %d, want %d", tc.method, tc.path, userScope, w.Code, want)
			}
		}

		if w := serve(t, a.Middleware(tc.classify, &capturingHandler{}), tc.method, tc.path); w.Code != 401 {
			t.Errorf("%s %s without credentials: status = %d, want 401", tc.method, tc.path, w.Code)
		}
	}
}

func TestPublicRoutes(t *testing.T) {
	t.Parallel()

	for _, metricsRequireAuth := range []bool{false, true} {
		a, _ := newTestAuthenticator(t, metricsRequireAuth)

		for _, classify := range []RouteClassifier{ClassifyRoute, ClassifyAdminRoute} {
			if w := serve(t, a.Middleware(classify, &capturingHandler{}), http.MethodGet, "/ready"); w.Code != 200 {
				t.Errorf("/ready: status = %d, want 200", w.Code)
			}

			wantMetrics := http.StatusOK
			if metricsRequireAuth {
				wantMetrics = http.StatusUnauthorized
			}

			if w := serve(t, a.Middleware(classify, &capturingHandler{}), http.MethodGet, "/metrics"); w.Code != wantMetrics {
				t.Errorf("/metrics (metrics_require_auth=%v): status = %d, want %d", metricsRequireAuth, w.Code, wantMetrics)
			}

			// Any authenticated user can read /metrics, whatever its scopes.
			w := serve(t, a.Middleware(classify, &capturingHandler{}), http.MethodGet, "/metrics", bearer(writerToken))
			if w.Code != http.StatusOK {
				t.Errorf("/metrics authenticated: status = %d, want 200", w.Code)
			}
		}
	}
}

func TestTenantBinding(t *testing.T) {
	t.Parallel()

	a, _ := newTestAuthenticator(t, false)

	cases := []struct {
		name         string
		creds        []credentials
		wantStatus   int
		wantTenant   string
		wantIdTenant string
	}{
		{
			name:         "bound user without header",
			creds:        []credentials{bearer(writerToken)},
			wantStatus:   http.StatusOK,
			wantIdTenant: "acme",
		},
		{
			name:         "bound user with the same header",
			creds:        []credentials{bearer(writerToken), header(types.HeaderTenant, "acme")},
			wantStatus:   http.StatusOK,
			wantTenant:   "acme",
			wantIdTenant: "acme",
		},
		{
			name:       "bound user with another tenant",
			creds:      []credentials{bearer(writerToken), header(types.HeaderTenant, "other")},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "unbound user keeps the header",
			creds:      []credentials{basic("reader", readerPassword), header(types.HeaderTenant, "other")},
			wantStatus: http.StatusOK,
			wantTenant: "other",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			next := &capturingHandler{}

			w := serve(t, a.Middleware(ClassifyRoute, next), http.MethodGet, "/api/v1/query", tc.creds...)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantStatus)
			}

			if tc.wantStatus != http.StatusOK {
				if next.served {
					t.Error("the request was served")
				}

				return
			}

			if next.tenant != tc.wantTenant {
				t.Errorf("tenant header = %q, want %q", next.tenant, tc.wantTenant)
			}

			if next.identity.Tenant != tc.wantIdTenant {
				t.Errorf("identity tenant = %q, want %q", next.identity.Tenant, tc.wantIdTenant)
			}
		})
	}
}

func TestIdentityForcedMatcher(t *testing.T) {
	t.Parallel()

	a, _ := newTestAuthenticator(t, false)
	next := &capturingHandler{}

	serve(t, a.Middleware(ClassifyRoute, next), http.MethodGet, "/api/v1/query", basic("reader", readerPassword))

	if next.identity == nil || len(next.identity.ForcedMatchers) != 1 ||
		next.identity.ForcedMatchers[0].String() != `env="prod"` {
		t.Errorf("identity = %+v, want the forced matcher env=\"prod\"", next.identity)
	}
}

func TestBcryptCache(t *testing.T) {
	t.Parallel()

	a, _ := newTestAuthenticator(t, false)

	var calls atomic.Int32

	a.compareHash = func(hash, password []byte) error {
		calls.Add(1)

		return bcrypt.CompareHashAndPassword(hash, password)
	}

	now := time.Now()
	a.cache.now = func() time.Time { return now }

	handler := a.Middleware(ClassifyRoute, &capturingHandler{})
	request := func(password string) int {
		return serve(t, handler, http.MethodGet, "/api/v1/query", basic("reader", password)).Code
	}

	for range 3 {
		if status := request(readerPassword); status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("bcrypt called %d times for 3 requests, want 1", got)
	}

	// A wrong password is never cached nor accepted.
	for range 2 {
		if status := request("wrong"); status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
	}

	if got := calls.Load(); got != 3 {
		t.Errorf("bcrypt called %d times, want 3", got)
	}

	// The cache expires.
	now = now.Add(verifyCacheTTL + time.Second)

	if status := request(readerPassword); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}

	if got := calls.Load(); got != 4 {
		t.Errorf("bcrypt called %d times after expiration, want 4", got)
	}
}

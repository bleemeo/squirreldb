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
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/api/auth/authtest"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"
)

const (
	testReaderPassword = "reader-password"
	testAdminPassword  = "admin-password"
	testWriterToken    = "writer-token"
)

func testAuthOptions(t *testing.T) auth.Options {
	t.Helper()

	hash := func(password string) string {
		h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}

		return string(h)
	}

	tokenSum := sha256.Sum256([]byte(testWriterToken))

	return auth.Options{
		Enabled: true,
		Users: []auth.User{
			{Name: "reader", PasswordHash: hash(testReaderPassword), Scopes: []string{"read"}},
			{Name: "writer", TokenSHA256: hex.EncodeToString(tokenSum[:]), Tenant: "acme", Scopes: []string{"write"}},
			{Name: "admin", PasswordHash: hash(testAdminPassword), Scopes: []string{"admin"}},
		},
	}
}

type authRouteCheck struct {
	routeCheck

	user   string
	tenant string
	// wantStatus is the expected status, 0 means served (not 401, 403, 404 nor 405).
	wantStatus int
}

func doAuthRequest(t *testing.T, handler http.Handler, rc authRouteCheck) int {
	t.Helper()

	var body io.Reader
	if rc.body != "" {
		body = strings.NewReader(rc.body)
	}

	req := httptest.NewRequestWithContext(t.Context(), rc.method, rc.path, body)

	switch rc.user {
	case "reader":
		req.SetBasicAuth("reader", testReaderPassword)
	case "admin":
		req.SetBasicAuth("admin", testAdminPassword)
	case "writer":
		req.Header.Set("Authorization", "Bearer "+testWriterToken)
	}

	if rc.tenant != "" {
		req.Header.Set(types.HeaderTenant, rc.tenant)
	}

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	return w.Code
}

func checkAuthRoutes(t *testing.T, listener string, handler http.Handler, checks []authRouteCheck) {
	t.Helper()

	for _, rc := range checks {
		status := doAuthRequest(t, handler, rc)

		if rc.wantStatus == 0 {
			switch status {
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed:
				t.Errorf("%s %s as %q on %s listener: status = %d, want served", rc.method, rc.path, rc.user, listener, status)
			}

			continue
		}

		if status != rc.wantStatus {
			t.Errorf("%s %s as %q on %s listener: status = %d, want %d",
				rc.method, rc.path, rc.user, listener, status, rc.wantStatus)
		}
	}
}

func get(path string) routeCheck {
	return routeCheck{method: http.MethodGet, path: path}
}

func post(path string) routeCheck {
	return routeCheck{method: http.MethodPost, path: path}
}

// TestAuthenticationRoutes checks the scopes required on both listeners.
func TestAuthenticationRoutes(t *testing.T) {
	t.Parallel()

	common := []authRouteCheck{
		{routeCheck: get("/ready")},
		{routeCheck: get("/metrics")},
		{routeCheck: get("/api/v1/labels"), wantStatus: http.StatusUnauthorized},
		{routeCheck: get("/api/v1/labels"), user: "reader"},
		{routeCheck: get("/api/v1/labels"), user: "writer", wantStatus: http.StatusForbidden},
		{routeCheck: get("/api/v1/labels"), user: "admin", wantStatus: http.StatusForbidden},
		{routeCheck: post("/api/v1/write"), user: "reader", wantStatus: http.StatusForbidden},
		// The (empty) body is rejected after the authentication.
		{routeCheck: post("/api/v1/write"), user: "writer", wantStatus: http.StatusBadRequest},
		{routeCheck: post("/api/v1/write"), user: "writer", tenant: "acme", wantStatus: http.StatusBadRequest},
		{routeCheck: post("/api/v1/write"), user: "writer", tenant: "other", wantStatus: http.StatusForbidden},
	}

	adminRoutes := []authRouteCheck{
		{routeCheck: get("/debug/"), wantStatus: http.StatusUnauthorized},
		{routeCheck: get("/debug/"), user: "reader", wantStatus: http.StatusForbidden},
		{routeCheck: post("/debug/fake/op"), user: "writer", wantStatus: http.StatusForbidden},
		{routeCheck: get("/debug/"), user: "admin"},
		{routeCheck: post("/debug/fake/op"), user: "admin"},
		{method: http.MethodPost, path: "/mutable/names", body: "invalid", user: "admin"},
	}

	t.Run("without admin listener", func(t *testing.T) {
		t.Parallel()

		api := makeAdminTestAPIWithAuth(t, "", testAuthOptions(t))

		checkAuthRoutes(t, "main", api, common)
		checkAuthRoutes(t, "main", api, adminRoutes)
		// Unknown routes require the admin scope.
		checkAuthRoutes(t, "main", api, []authRouteCheck{
			{routeCheck: get("/unknown"), user: "reader", wantStatus: http.StatusForbidden},
			{routeCheck: get("/unknown"), user: "admin", wantStatus: http.StatusNotFound},
		})
	})

	t.Run("with admin listener", func(t *testing.T) {
		t.Parallel()

		api := makeAdminTestAPIWithAuth(t, "localhost:0", testAuthOptions(t))
		admin := http.HandlerFunc(api.serveAdminHTTP)

		checkAuthRoutes(t, "main", api, common)
		checkAuthRoutes(t, "admin", admin, adminRoutes)
		checkAuthRoutes(t, "admin", admin, []authRouteCheck{
			{routeCheck: get("/ready")},
			{routeCheck: get("/metrics")},
			// The read and write routes are not served on the admin listener, and need admin.
			{routeCheck: get("/api/v1/labels"), user: "reader", wantStatus: http.StatusForbidden},
			{routeCheck: get("/api/v1/labels"), user: "admin", wantStatus: http.StatusNotFound},
		})
		// The admin endpoints are not on the main listener, but the admin scope is
		// required before the 404.
		checkAuthRoutes(t, "main", api, []authRouteCheck{
			{routeCheck: get("/debug/"), user: "reader", wantStatus: http.StatusForbidden},
			{routeCheck: get("/debug/"), user: "admin", wantStatus: http.StatusNotFound},
		})
	})
}

func makeAdminTestAPIWithAuth(t *testing.T, adminListenAddress string, authOptions auth.Options) *API {
	t.Helper()

	api := makeAPI(dummy.DiscardTSDB{}, nil)
	api.AdminListenAddress = adminListenAddress
	api.BackendName = "fake"
	api.BackendDebug = fakeBackendDebug{}
	api.Logger = zerolog.Nop()
	api.Auth = authOptions

	if err := api.init(t.Context()); err != nil {
		t.Fatal(err)
	}

	api.Ready()

	return api
}

// TestRunWithTLS checks that both listeners are served over TLS, and that
// the client certificate is required with mTLS.
func TestRunWithTLS(t *testing.T) {
	t.Parallel()

	certs := authtest.WriteCertificates(t, t.TempDir())

	api := makeAPI(dummy.DiscardTSDB{}, nil)
	api.ListenAddress = "127.0.0.1:0"
	api.AdminListenAddress = "127.0.0.1:0"
	api.TLS = auth.TLSOptions{
		CertFile:       certs.ServerCertFile,
		KeyFile:        certs.ServerKeyFile,
		ClientCAFile:   certs.CAFile,
		ClientAuthType: "RequireAndVerifyClientCert",
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	readiness := make(chan error)
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		api.Run(ctx, readiness)
	}()

	if err := <-readiness; err != nil {
		t.Fatal(err)
	}

	api.Ready()

	do := func(tlsConfig *tls.Config, scheme string, port int, path string) (int, error) {
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
		defer client.CloseIdleConnections()

		url := fmt.Sprintf("%s://127.0.0.1:%d%s", scheme, port, path)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}

		_ = resp.Body.Close()

		return resp.StatusCode, nil
	}

	for _, listener := range []struct {
		name string
		port int
		path string
	}{
		{name: "main", port: api.ListenPort(), path: "/api/v1/labels"},
		{name: "admin", port: api.AdminListenPort(), path: "/debug/"},
	} {
		status, err := do(certs.ClientConfig(t, true), "https", listener.port, listener.path)
		if err != nil || status != http.StatusOK {
			t.Errorf("%s listener with client certificate: status = %d, err = %v, want 200", listener.name, status, err)
		}

		if _, err := do(certs.ClientConfig(t, false), "https", listener.port, "/ready"); err == nil {
			t.Errorf("%s listener accepted a client without certificate", listener.name)
		}

		// A plain HTTP request gets a 400 from the TLS server.
		if status, err := do(nil, "http", listener.port, "/ready"); err == nil && status == http.StatusOK {
			t.Errorf("%s listener answered a plain HTTP request", listener.name)
		}
	}

	cancel()

	select {
	case <-stopped:
	case <-time.After(httpServerShutdownTimeout):
		t.Fatal("Run didn't stop")
	}
}

// fakeListener is a listener on a given address, only used for its address.
type fakeListener struct {
	net.Listener

	addr net.Addr
}

func (l fakeListener) Addr() net.Addr {
	return l.addr
}

func TestInsecureListenerWarnings(t *testing.T) {
	t.Parallel()

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	cases := []struct {
		name        string
		ip          net.IP
		authEnabled bool
		tlsConfig   *tls.Config
		wantWarning string
	}{
		{name: "loopback without auth", ip: net.IPv4(127, 0, 0, 1)},
		{name: "ipv6 loopback without auth", ip: net.IPv6loopback},
		{name: "any address without auth", ip: net.IPv4zero, wantWarning: "Authentication is disabled"},
		{name: "ipv6 any address without auth", ip: net.IPv6unspecified, wantWarning: "Authentication is disabled"},
		{name: "public ip without auth", ip: net.IPv4(192, 0, 2, 1), wantWarning: "Authentication is disabled"},
		{name: "loopback with auth without TLS", ip: net.IPv4(127, 0, 0, 1), authEnabled: true},
		{name: "any address with auth without TLS", ip: net.IPv4zero, authEnabled: true, wantWarning: "clear text"},
		{name: "any address with auth and TLS", ip: net.IPv4zero, authEnabled: true, tlsConfig: tlsConfig},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logs := &bytes.Buffer{}

			authOptions := auth.Options{}
			if tc.authEnabled {
				authOptions = testAuthOptions(t)
			}

			authenticator, err := auth.New(authOptions)
			if err != nil {
				t.Fatal(err)
			}

			api := &API{Logger: zerolog.New(logs), authenticator: authenticator, tlsConfig: tc.tlsConfig}
			api.warnInsecureListener(&apiServer{
				name:     "API",
				listener: fakeListener{addr: &net.TCPAddr{IP: tc.ip, Port: 9201}},
			})

			if tc.wantWarning == "" && logs.Len() > 0 {
				t.Errorf("unexpected warning: %s", logs.String())
			}

			if tc.wantWarning != "" && !strings.Contains(logs.String(), tc.wantWarning) {
				t.Errorf("logs = %q, want a warning containing %q", logs.String(), tc.wantWarning)
			}
		})
	}
}

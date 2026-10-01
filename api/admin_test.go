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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/rs/zerolog"
)

// fakeBackendDebug registers one read-only and one mutating backend debug endpoint.
type fakeBackendDebug struct{}

func (fakeBackendDebug) RegisterHTTPEndpoints(
	register func(method, path, description string, handler http.HandlerFunc),
) {
	ok := func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") }

	register(http.MethodGet, "/info", "read-only", ok)
	register(http.MethodPost, "/op", "mutating", ok)
}

type routeCheck struct {
	method string
	path   string
	body   string
}

// adminRoutes are a sample of the admin endpoints, covering each kind of route.
var adminRoutes = []routeCheck{ //nolint:gochecknoglobals
	{method: http.MethodGet, path: "/debug/"},
	{method: http.MethodGet, path: "/debug/index_info"},
	{method: http.MethodGet, path: "/debug/flush"},
	{method: http.MethodPost, path: "/debug/flush"},
	{method: http.MethodGet, path: "/debug/index_block"},
	{method: http.MethodPost, path: "/debug/index_unblock"},
	{method: http.MethodPost, path: "/debug/toggle_debug_query"},
	{method: http.MethodPost, path: "/debug/preaggregate"},
	{method: http.MethodGet, path: "/debug/pprof/"},
	{method: http.MethodGet, path: "/debug/fake/info"},
	{method: http.MethodPost, path: "/debug/fake/op"},
	// An invalid body is rejected with a 400 before the (nil) mutable label writer is used.
	{method: http.MethodPost, path: "/mutable/names", body: "invalid"},
	{method: http.MethodDelete, path: "/mutable/values", body: "invalid"},
}

// commonRoutes are served on every listener.
var commonRoutes = []routeCheck{ //nolint:gochecknoglobals
	{method: http.MethodGet, path: "/metrics"},
	{method: http.MethodGet, path: "/ready"},
}

// publicRoutes are only served on the main listener.
var publicRoutes = []routeCheck{ //nolint:gochecknoglobals
	{method: http.MethodGet, path: "/api/v1/labels"},
}

func makeAdminTestAPI(t *testing.T, adminListenAddress string, logs io.Writer) *API {
	t.Helper()

	api := makeAPI(dummy.DiscardTSDB{}, nil)
	api.AdminListenAddress = adminListenAddress
	api.BackendName = "fake"
	api.BackendDebug = fakeBackendDebug{}
	api.Logger = zerolog.New(logs)

	if err := api.init(t.Context()); err != nil {
		t.Fatal(err)
	}

	api.Ready()

	return api
}

func doRequest(t *testing.T, handler http.Handler, rc routeCheck) int {
	t.Helper()

	var body io.Reader
	if rc.body != "" {
		body = strings.NewReader(rc.body)
	}

	req := httptest.NewRequestWithContext(t.Context(), rc.method, rc.path, body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	return w.Result().StatusCode
}

func checkServed(t *testing.T, listener string, handler http.Handler, routes []routeCheck, wantServed bool) {
	t.Helper()

	for _, rc := range routes {
		status := doRequest(t, handler, rc)
		served := status != http.StatusNotFound && status != http.StatusMethodNotAllowed

		if served != wantServed {
			t.Errorf("%s %s on %s listener: status = %d, want served = %v", rc.method, rc.path, listener, status, wantServed)
		}
	}
}

// TestAdminRoutesWithoutAdminListener checks that every endpoint stays on the
// main listener when admin_listen_address is empty.
func TestAdminRoutesWithoutAdminListener(t *testing.T) {
	t.Parallel()

	api := makeAdminTestAPI(t, "", io.Discard)

	if api.adminRouter != nil {
		t.Error("adminRouter is set, want nil without AdminListenAddress")
	}

	checkServed(t, "main", api, adminRoutes, true)
	checkServed(t, "main", api, commonRoutes, true)
	checkServed(t, "main", api, publicRoutes, true)
}

// TestAdminRoutesWithAdminListener checks that the admin endpoints move to the
// admin listener when admin_listen_address is set.
func TestAdminRoutesWithAdminListener(t *testing.T) {
	t.Parallel()

	api := makeAdminTestAPI(t, "localhost:0", io.Discard)
	admin := http.HandlerFunc(api.serveAdminHTTP)

	checkServed(t, "main", api, adminRoutes, false)
	checkServed(t, "main", api, commonRoutes, true)
	checkServed(t, "main", api, publicRoutes, true)

	checkServed(t, "admin", admin, adminRoutes, true)
	checkServed(t, "admin", admin, commonRoutes, true)
	checkServed(t, "admin", admin, publicRoutes, false)
}

// TestAdminListenerNotReady checks that the admin listener waits for the API
// to be ready, like the main listener.
func TestAdminListenerNotReady(t *testing.T) {
	t.Parallel()

	api := makeAPI(dummy.DiscardTSDB{}, nil)
	api.AdminListenAddress = "localhost:0"

	if err := api.init(t.Context()); err != nil {
		t.Fatal(err)
	}

	status := doRequest(t, http.HandlerFunc(api.serveAdminHTTP), routeCheck{method: http.MethodGet, path: "/ready"})
	if status != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", status, http.StatusServiceUnavailable)
	}
}

// TestMutatingEndpointsMethods checks that the endpoints that change state accept
// POST, and still accept GET with a deprecation warning logged once per endpoint.
func TestMutatingEndpointsMethods(t *testing.T) {
	t.Parallel()

	paths := []string{
		"/debug/flush",
		"/debug/toggle_debug_query",
		"/debug/index_block",
		"/debug/index_unblock",
		"/debug/index_verify?fix=1",
		"/debug/preaggregate?from=2006-01-02&to=2006-01-02",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			logs := &bytes.Buffer{}
			api := makeAdminTestAPI(t, "", logs)

			for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodGet} {
				status := doRequest(t, api, routeCheck{method: method, path: path})
				if status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
					t.Errorf("%s %s: status = %d, want the endpoint to be served", method, path, status)
				}
			}

			if count := strings.Count(logs.String(), "is deprecated"); count != 1 {
				t.Errorf("got %d deprecation warnings, want 1. Logs:\n%s", count, logs.String())
			}
		})
	}
}

// TestNonMutatingGetNotDeprecated checks that read-only GET requests don't log
// a deprecation warning.
func TestNonMutatingGetNotDeprecated(t *testing.T) {
	t.Parallel()

	logs := &bytes.Buffer{}
	api := makeAdminTestAPI(t, "", logs)

	for _, path := range []string{"/debug/index_verify", "/debug/index_info", "/debug/"} {
		doRequest(t, api, routeCheck{method: http.MethodGet, path: path})
	}

	if strings.Contains(logs.String(), "is deprecated") {
		t.Errorf("unexpected deprecation warning:\n%s", logs.String())
	}
}

// TestRunWithAdminListener checks that Run serves both listeners and stops both.
func TestRunWithAdminListener(t *testing.T) {
	t.Parallel()

	api := makeAPI(dummy.DiscardTSDB{}, nil)
	api.ListenAddress = "127.0.0.1:0"
	api.AdminListenAddress = "127.0.0.1:0"

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

	getStatus := func(port int, path string) int {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		_ = resp.Body.Close()

		return resp.StatusCode
	}

	if status := getStatus(api.ListenPort(), "/debug/"); status != http.StatusNotFound {
		t.Errorf("/debug/ on main listener: status = %d, want %d", status, http.StatusNotFound)
	}

	if status := getStatus(api.AdminListenPort(), "/debug/"); status != http.StatusOK {
		t.Errorf("/debug/ on admin listener: status = %d, want %d", status, http.StatusOK)
	}

	if status := getStatus(api.AdminListenPort(), "/ready"); status != http.StatusOK {
		t.Errorf("/ready on admin listener: status = %d, want %d", status, http.StatusOK)
	}

	cancel()

	select {
	case <-stopped:
	case <-time.After(httpServerShutdownTimeout):
		t.Fatal("Run didn't stop")
	}

	dialer := &net.Dialer{Timeout: time.Second}

	for _, port := range []int{api.ListenPort(), api.AdminListenPort()} {
		conn, err := dialer.DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			_ = conn.Close()

			t.Errorf("port %d still accepts connections after Run stopped", port)
		}
	}
}

// TestRunAdminListenFailure checks that Run reports an error on readiness when
// the admin listener can't bind.
func TestRunAdminListenFailure(t *testing.T) {
	t.Parallel()

	busy, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer busy.Close()

	api := makeAPI(dummy.DiscardTSDB{}, nil)
	api.ListenAddress = "127.0.0.1:0"
	api.AdminListenAddress = busy.Addr().String()

	readiness := make(chan error)

	go api.Run(t.Context(), readiness)

	if err := <-readiness; err == nil {
		t.Fatal("Run succeeded, want an error because the admin address is already in use")
	}
}

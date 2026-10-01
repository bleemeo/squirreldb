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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/core"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

const (
	routingDemoToken = "demo-token"
	routingFullToken = "full-token"
)

// newRoutingAuthAPI returns an API with authentication on a dummy primary backend
// with a routed sandbox backend, and the registries of the core and API metrics.
// The user "demo" is restricted to the sandbox backend, "full" isn't restricted.
func newRoutingAuthAPI(t *testing.T) (*API, *prometheus.Registry, *prometheus.Registry) {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy
	cfg.Internal.Dummy.Batcher = false
	cfg.Routing = config.Routing{
		Backends: []string{config.BackendSandbox},
		Rules:    []config.RoutingRule{{Tenants: []string{"load-*"}, Backend: config.BackendDummy}},
	}

	coreRegistry := prometheus.NewRegistry()
	squirrelDB := core.New(core.Deps{Logger: zerolog.Nop(), MetricRegistry: coreRegistry}, cfg,
		core.SquirrelDBOption{})

	t.Cleanup(func() { squirrelDB.CloseAll(context.Background()) })

	queryable, err := squirrelDB.Queryable(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	appendable, err := squirrelDB.Appendable(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	tokenHash := func(token string) string {
		sum := sha256.Sum256([]byte(token))

		return hex.EncodeToString(sum[:])
	}

	apiRegistry := prometheus.NewRegistry()
	squirrelDBAPI := makeAPI(nil, nil)
	squirrelDBAPI.Queryable = queryable
	squirrelDBAPI.Appendable = appendable
	squirrelDBAPI.BackendResolver = squirrelDB
	squirrelDBAPI.MetricRegistry = apiRegistry
	squirrelDBAPI.Auth = auth.Options{
		Enabled: true,
		Users: []auth.User{
			{
				Name:        "demo",
				TokenSHA256: tokenHash(routingDemoToken),
				Scopes:      []string{"read", "write"},
				Backends:    []string{config.BackendSandbox},
			},
			{Name: "full", TokenSHA256: tokenHash(routingFullToken), Scopes: []string{"read", "write"}},
		},
	}

	if err := squirrelDBAPI.init(t.Context()); err != nil {
		t.Fatal(err)
	}

	squirrelDBAPI.Ready()

	return squirrelDBAPI, coreRegistry, apiRegistry
}

// TestBackendRoutingWithAuth checks the backends of the users (auth.users[].backends)
// through the API: a user restricted to the sandbox gets the sandbox data without
// header, and a 403 when it's routed to another backend, for reads and writes.
func TestBackendRoutingWithAuth(t *testing.T) {
	t.Parallel()

	squirrelDBAPI, coreRegistry, apiRegistry := newRoutingAuthAPI(t)

	end := time.Now()
	start := end.Add(-time.Hour)

	tests := []struct {
		name    string
		token   string
		headers map[string]string
		// wantStatus is the status of the query, wantSeries whether it returns
		// series when it succeeds.
		wantStatus int
		wantSeries bool
	}{
		{name: "restricted user without header", token: routingDemoToken, wantStatus: http.StatusOK, wantSeries: true},
		{
			name:       "restricted user with an allowed header",
			token:      routingDemoToken,
			headers:    map[string]string{types.HeaderBackend: config.BackendSandbox},
			wantStatus: http.StatusOK,
			wantSeries: true,
		},
		{
			name:       "restricted user with a header not allowed",
			token:      routingDemoToken,
			headers:    map[string]string{types.HeaderBackend: config.BackendDummy},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "restricted user routed by a rule to a backend not allowed",
			token:      routingDemoToken,
			headers:    map[string]string{types.HeaderTenant: "load-1"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "restricted user with a header not enabled",
			token:      routingDemoToken,
			headers:    map[string]string{types.HeaderBackend: config.BackendCassandra},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unrestricted user without header",
			token:      routingFullToken,
			wantStatus: http.StatusOK,
			wantSeries: false,
		},
		{
			name:       "unrestricted user with a header",
			token:      routingFullToken,
			headers:    map[string]string{types.HeaderBackend: config.BackendSandbox},
			wantStatus: http.StatusOK,
			wantSeries: true,
		},
	}

	// The parallel queries are grouped so they are done before the counters are checked.
	t.Run("queries", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				form := url.Values{
					"query": {"agent_status"},
					"start": {strconv.FormatInt(start.Unix(), 10)},
					"end":   {strconv.FormatInt(end.Unix(), 10)},
					"step":  {"60"},
				}

				req := httptest.NewRequestWithContext(
					t.Context(), http.MethodPost, "/api/v1/query_range", strings.NewReader(form.Encode()),
				)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.Header.Set("Authorization", "Bearer "+tt.token)

				for k, v := range tt.headers {
					req.Header.Set(k, v)
				}

				rec := httptest.NewRecorder()
				squirrelDBAPI.ServeHTTP(rec, req)

				if rec.Code != tt.wantStatus {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, tt.wantStatus, rec.Body.String())
				}

				if tt.wantStatus == http.StatusForbidden {
					body := rec.Body.String()
					if !strings.Contains(body, types.ErrBackendNotAllowed.Error()) || !strings.Contains(body, `\"dummy\"`) {
						t.Errorf("body = %q, want it to tell the backend dummy isn't allowed", body)
					}
				}

				if tt.wantStatus != http.StatusOK {
					return
				}

				var response struct {
					Data struct {
						Result []json.RawMessage `json:"result"`
					} `json:"data"`
				}

				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}

				if got := len(response.Data.Result) > 0; got != tt.wantSeries {
					t.Errorf("got %d series, want series = %v", len(response.Data.Result), tt.wantSeries)
				}
			})
		}
	})

	t.Run("writes", func(t *testing.T) {
		write := func(token string, headers map[string]string) int {
			t.Helper()

			body, err := writeRequestToBody(metricsToWriteRequest([]types.Timeseries{{
				Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}},
				Labels: labels.FromStrings("__name__", "cpu_used"),
			}}))
			if err != nil {
				t.Fatal(err)
			}

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/write", body)
			req.Header.Set("Authorization", "Bearer "+token)

			for k, v := range headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			squirrelDBAPI.ServeHTTP(rec, req)

			return rec.Code
		}

		if status := write(routingDemoToken, nil); status != http.StatusNoContent {
			t.Errorf("restricted user write without header: status = %d, want %d", status, http.StatusNoContent)
		}

		forbidden := []map[string]string{
			{types.HeaderBackend: config.BackendDummy},
			{types.HeaderTenant: "load-1"},
		}

		for _, headers := range forbidden {
			if status := write(routingDemoToken, headers); status != http.StatusForbidden {
				t.Errorf("restricted user write with %v: status = %d, want %d", headers, status, http.StatusForbidden)
			}
		}

		if status := write(routingFullToken, nil); status != http.StatusNoContent {
			t.Errorf("unrestricted user write: status = %d, want %d", status, http.StatusNoContent)
		}

		// The restricted write went to the sandbox, the unrestricted one to the default backend.
		wantRouted := map[[3]string]float64{
			{config.BackendSandbox, "write", "user"}:  1,
			{config.BackendDummy, "write", "default"}: 1,
		}

		for lbls, want := range wantRouted {
			got := counterValue(t, coreRegistry, "squirreldb_routing_requests_total", map[string]string{
				"backend": lbls[0], "operation": lbls[1], "source": lbls[2],
			})
			if got != want {
				t.Errorf("squirreldb_routing_requests_total%v = %v, want %v", lbls, got, want)
			}
		}
	})

	// Every 403 above (2 queries and 2 writes) is counted as an authentication failure.
	got := counterValue(t, apiRegistry, "squirreldb_api_auth_failures_total", map[string]string{
		"reason": "backend_not_allowed",
	})
	if got != 4 {
		t.Errorf(`squirreldb_api_auth_failures_total{reason="backend_not_allowed"} = %v, want 4`, got)
	}

	// The restricted query without header was served by the sandbox, as the user's backend.
	got = counterValue(t, coreRegistry, "squirreldb_routing_requests_total", map[string]string{
		"backend": config.BackendSandbox, "operation": "read", "source": "user",
	})
	if got == 0 {
		t.Error(`squirreldb_routing_requests_total{backend="sandbox",operation="read",source="user"} = 0, want > 0`)
	}
}

// counterValue returns the value of the counter name with the labels lbls, 0 when
// it doesn't exist.
func counterValue(t *testing.T, gatherer prometheus.Gatherer, name string, lbls map[string]string) float64 {
	t.Helper()

	families, err := gatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

	metrics:
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if lbls[pair.GetName()] != pair.GetValue() {
					continue metrics
				}
			}

			return metric.GetCounter().GetValue()
		}
	}

	return 0
}

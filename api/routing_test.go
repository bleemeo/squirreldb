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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/core"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog"
)

// TestBackendRouting runs PromQL queries through the API on a dummy primary
// backend with a routed sandbox backend, selected by the X-SquirrelDB-Backend
// header or by a tenant rule.
func TestBackendRouting(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy
	cfg.Internal.Dummy.Batcher = false
	cfg.Routing = config.Routing{
		Backends: []string{config.BackendSandbox},
		Rules:    []config.RoutingRule{{Tenants: []string{"demo-*"}, Backend: config.BackendSandbox}},
	}

	squirrelDB := core.New(core.Deps{Logger: zerolog.Nop(), MetricRegistry: prometheus.NewRegistry()}, cfg,
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

	squirrelDBAPI := makeAPI(nil, nil)
	squirrelDBAPI.Queryable = queryable
	squirrelDBAPI.Appendable = appendable
	squirrelDBAPI.BackendResolver = squirrelDB

	if err := squirrelDBAPI.init(t.Context()); err != nil {
		t.Fatal(err)
	}

	squirrelDBAPI.Ready()

	end := time.Now()
	start := end.Add(-time.Hour)

	tests := []struct {
		name       string
		headers    map[string]string
		wantSeries bool
	}{
		{name: "default route", wantSeries: false},
		{name: "header", headers: map[string]string{types.HeaderBackend: config.BackendSandbox}, wantSeries: true},
		{name: "tenant rule", headers: map[string]string{types.HeaderTenant: "demo-1"}, wantSeries: true},
		{
			name: "header takes precedence over rules",
			headers: map[string]string{
				types.HeaderTenant:  "demo-1",
				types.HeaderBackend: config.BackendDummy,
			},
			wantSeries: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := api.NewClient(api.Config{
				RoundTripper: roundTripperFromHTTPHandler{handler: squirrelDBAPI, headers: tt.headers},
			})
			if err != nil {
				t.Fatal(err)
			}

			response, _, _, err := v1.NewAPI(client).QueryRange(
				t.Context(),
				"agent_status",
				v1.Range{Start: start, End: end, Step: time.Minute},
			)
			if err != nil {
				t.Fatal(err)
			}

			matrix, ok := response.(model.Matrix)
			if !ok {
				t.Fatalf("expected a matrix, got %v", response)
			}

			if got := len(matrix) > 0; got != tt.wantSeries {
				t.Errorf("got %d series, want series = %v", len(matrix), tt.wantSeries)
			}

			for _, series := range matrix {
				tenant := tt.headers[types.HeaderTenant]
				if got := string(series.Metric[model.LabelName(cfg.TenantLabelName)]); got != tenant {
					t.Errorf("series %s: tenant label = %q, want %q", series.Metric, got, tenant)
				}
			}
		})
	}

	t.Run("invalid header", func(t *testing.T) {
		t.Parallel()

		for _, path := range []string{"/api/v1/query_range", "/api/v1/write"} {
			form := url.Values{
				"query": {"agent_status"},
				"start": {strconv.FormatInt(start.Unix(), 10)},
				"end":   {strconv.FormatInt(end.Unix(), 10)},
				"step":  {"60"},
			}

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set(types.HeaderBackend, config.BackendCassandra)

			rec := httptest.NewRecorder()
			squirrelDBAPI.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s: status = %d, want %d", path, rec.Code, http.StatusBadRequest)
			}

			if body := rec.Body.String(); !strings.Contains(body, types.ErrBackendNotSelectable.Error()) {
				t.Errorf("%s: body = %q, want it to explain the backend isn't selectable", path, body)
			}
		}
	})
}

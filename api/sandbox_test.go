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
	"slices"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	sandboxbackend "github.com/bleemeo/squirreldb/sandbox/backend"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog"
)

// TestSandboxBackend runs PromQL queries through the API on the sandbox backend,
// with and without tenant.
func TestSandboxBackend(t *testing.T) {
	t.Parallel()

	tenantLabel := config.DefaultConfig().TenantLabelName

	backend, err := sandboxbackend.New(t.Context(), zerolog.Nop(), sandboxbackend.Options{
		MetricRegistry: prometheus.NewRegistry(),
		Cluster:        &dummy.LocalCluster{},
		TenantLabel:    tenantLabel,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := backend.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { backend.CloseAll(context.Background()) })

	tsdb, err := backend.TSDB(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	squirrelDBAPI := makeAPI(tsdb, nil)

	if err := squirrelDBAPI.init(t.Context()); err != nil {
		t.Fatal(err)
	}

	squirrelDBAPI.Ready()

	end := time.Now()
	start := end.Add(-time.Hour)

	tests := []struct {
		name   string
		tenant string
	}{
		{name: "without tenant"},
		{name: "with tenant", tenant: "1234"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			headers := map[string]string{}
			if tt.tenant != "" {
				headers[types.HeaderTenant] = tt.tenant
			}

			client, err := api.NewClient(api.Config{
				RoundTripper: roundTripperFromHTTPHandler{handler: squirrelDBAPI, headers: headers},
			})
			if err != nil {
				t.Fatal(err)
			}

			apiClient := v1.NewAPI(client)

			response, _, _, err := apiClient.QueryRange(
				t.Context(),
				"agent_status",
				v1.Range{Start: start, End: end, Step: time.Minute},
			)
			if err != nil {
				t.Fatal(err)
			}

			matrix, ok := response.(model.Matrix)
			if !ok || len(matrix) == 0 {
				t.Fatalf("expected a non-empty matrix, got %v", response)
			}

			for _, series := range matrix {
				if got := string(series.Metric[model.LabelName(tenantLabel)]); got != tt.tenant {
					t.Errorf("series %s: tenant label = %q, want %q", series.Metric, got, tt.tenant)
				}

				if len(series.Values) != 61 {
					t.Errorf("series %s has %d points, want 61", series.Metric, len(series.Values))
				}
			}

			names, _, _, err := apiClient.LabelValues(t.Context(), model.MetricNameLabel, nil, start, end)
			if err != nil {
				t.Fatal(err)
			}

			if !slices.Contains(names, "agent_status") || !slices.Contains(names, "probe_domain_expiry") {
				t.Errorf("label values of __name__ = %v, want agent_status and probe_domain_expiry", names)
			}

			if !slices.IsSorted(names) || len(slices.Compact(slices.Clone(names))) != len(names) {
				t.Errorf("label values of __name__ must be sorted and deduplicated, got %v", names)
			}

			tenants, _, _, err := apiClient.LabelValues(t.Context(), tenantLabel, nil, start, end)
			if err != nil {
				t.Fatal(err)
			}

			wantTenants := model.LabelValues{}
			if tt.tenant != "" {
				wantTenants = model.LabelValues{model.LabelValue(tt.tenant)}
			}

			if !slices.Equal(tenants, wantTenants) {
				t.Errorf("label values of %s = %v, want %v", tenantLabel, tenants, wantTenants)
			}
		})
	}
}

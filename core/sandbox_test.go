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

package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	apipromql "github.com/bleemeo/squirreldb/api/promql"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

// TestSandboxBackendFromConfig checks that a "backend: sandbox" configuration
// starts the sandbox backend and that a range query through the read path
// returns data from the embedded dataset.
func TestSandboxBackendFromConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "squirreldb.conf")

	err := os.WriteFile(configPath, []byte("backend: sandbox\nsandbox:\n  dataset_config: \"\"\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	cfg, warnings, err := config.LoadConfig([]string{configPath})
	if err != nil {
		t.Fatal(err)
	}

	if len(warnings) > 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}

	if cfg.Backend != config.BackendSandbox {
		t.Fatalf("backend = %q, want %q", cfg.Backend, config.BackendSandbox)
	}

	ctx := t.Context()
	reg := prometheus.NewRegistry()
	squirrelDB := New(Deps{Logger: zerolog.Nop(), MetricRegistry: reg}, cfg, SquirrelDBOption{})

	defer squirrelDB.CloseAll(context.Background())

	queryable, err := squirrelDB.Queryable(ctx)
	if err != nil {
		t.Fatalf("Queryable: %v", err)
	}

	// Writes are accepted.
	appendable, err := squirrelDB.Appendable(ctx)
	if err != nil {
		t.Fatalf("Appendable: %v", err)
	}

	app := appendable.AppenderWithOptions(ctx, types.AppenderOptions{})
	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	engine := apipromql.NewEngine(zerolog.Nop(), reg)
	promQueryable := storage.QueryableFunc(func(mint, maxt int64) (storage.Querier, error) {
		return queryable.QuerierWithOptions(types.QuerierOptions{}, mint, maxt)
	})

	end := time.Now()
	start := end.Add(-time.Hour)

	query, err := engine.NewRangeQuery(ctx, promQueryable, nil, "agent_status", start, end, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	defer query.Close()

	result := query.Exec(ctx)
	if result.Err != nil {
		t.Fatal(result.Err)
	}

	matrix, err := result.Matrix()
	if err != nil {
		t.Fatal(err)
	}

	if len(matrix) == 0 {
		t.Fatal("agent_status returned no series")
	}

	for _, series := range matrix {
		if len(series.Floats) != 61 {
			t.Errorf("series %s has %d points, want 61", series.Metric, len(series.Floats))
		}
	}
}

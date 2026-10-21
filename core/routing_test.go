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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apipromql "github.com/bleemeo/squirreldb/api/promql"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

func TestResolveBackend(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendClickHouse
	cfg.Routing = config.Routing{
		Backends: []string{config.BackendSandbox, config.BackendDummy},
		Rules: []config.RoutingRule{
			{Tenants: []string{"demo-*", "1234"}, Backend: config.BackendSandbox},
			{Tenants: []string{"demo-load-*", "12?4"}, Backend: config.BackendDummy},
			{Tenants: []string{"5678"}, Backend: config.BackendClickHouse},
		},
	}

	router := newBackendRouter(cfg)

	cases := []struct {
		name        string
		tenant      string
		requested   string
		wantBackend string
		wantSource  string
		wantErr     bool
	}{
		{name: "no tenant, no header", wantBackend: config.BackendClickHouse, wantSource: routeSourceDefault},
		{
			name:        "tenant matching no rule",
			tenant:      "42",
			wantBackend: config.BackendClickHouse,
			wantSource:  routeSourceDefault,
		},
		{name: "exact rule", tenant: "1234", wantBackend: config.BackendSandbox, wantSource: routeSourceRule},
		{name: "glob rule", tenant: "demo-42", wantBackend: config.BackendSandbox, wantSource: routeSourceRule},
		{
			name:        "first matching rule wins",
			tenant:      "demo-load-1",
			wantBackend: config.BackendSandbox,
			wantSource:  routeSourceRule,
		},
		{name: "second rule", tenant: "1224", wantBackend: config.BackendDummy, wantSource: routeSourceRule},
		{
			name:        "rule to the default backend",
			tenant:      "5678",
			wantBackend: config.BackendClickHouse,
			wantSource:  routeSourceRule,
		},
		{
			name:        "header without tenant",
			requested:   config.BackendSandbox,
			wantBackend: config.BackendSandbox,
			wantSource:  routeSourceHeader,
		},
		{
			name:        "header takes precedence over rules",
			tenant:      "demo-42",
			requested:   config.BackendClickHouse,
			wantBackend: config.BackendClickHouse,
			wantSource:  routeSourceHeader,
		},
		{name: "header with a backend not enabled", requested: config.BackendCassandra, wantErr: true},
		{name: "header with an unknown backend", tenant: "demo-42", requested: "postgres", wantErr: true},
		{name: "header is case sensitive", requested: "Sandbox", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			name, source, err := router.resolveBackend(tc.tenant, tc.requested, nil)
			if tc.wantErr {
				if !errors.Is(err, types.ErrBackendNotSelectable) {
					t.Fatalf("err = %v, want %v", err, types.ErrBackendNotSelectable)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if name != tc.wantBackend || source != tc.wantSource {
				t.Errorf("resolveBackend = (%q, %q), want (%q, %q)", name, source, tc.wantBackend, tc.wantSource)
			}
		})
	}
}

// TestResolveBackendAllowed checks the resolution for a user restricted to some
// backends (auth.users[].backends).
func TestResolveBackendAllowed(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendClickHouse
	cfg.Routing = config.Routing{
		Backends: []string{config.BackendSandbox, config.BackendDummy},
		Rules: []config.RoutingRule{
			{Tenants: []string{"demo-*"}, Backend: config.BackendSandbox},
			{Tenants: []string{"load-*"}, Backend: config.BackendDummy},
		},
	}

	router := newBackendRouter(cfg)

	sandboxOnly := []string{config.BackendSandbox}
	sandboxThenDummy := []string{config.BackendSandbox, config.BackendDummy}
	withDefault := []string{config.BackendDummy, config.BackendClickHouse}

	cases := []struct {
		name        string
		tenant      string
		requested   string
		allowed     []string
		wantBackend string
		wantSource  string
		wantErr     error
	}{
		// An unrestricted user is routed as without authentication.
		{name: "unrestricted default", wantBackend: config.BackendClickHouse, wantSource: routeSourceDefault},
		{
			name:        "unrestricted header",
			requested:   config.BackendDummy,
			wantBackend: config.BackendDummy,
			wantSource:  routeSourceHeader,
		},
		{
			name:        "unrestricted rule",
			tenant:      "load-1",
			wantBackend: config.BackendDummy,
			wantSource:  routeSourceRule,
		},
		{
			name:        "header allowed",
			requested:   config.BackendSandbox,
			allowed:     sandboxOnly,
			wantBackend: config.BackendSandbox,
			wantSource:  routeSourceHeader,
		},
		{
			name:      "header to the default backend not allowed",
			requested: config.BackendClickHouse,
			allowed:   sandboxOnly,
			wantErr:   types.ErrBackendNotAllowed,
		},
		{
			name:      "header to a routed backend not allowed",
			tenant:    "demo-1",
			requested: config.BackendDummy,
			allowed:   sandboxOnly,
			wantErr:   types.ErrBackendNotAllowed,
		},
		{
			name:      "header to a backend not enabled",
			requested: config.BackendCassandra,
			allowed:   sandboxOnly,
			wantErr:   types.ErrBackendNotSelectable,
		},
		{
			name:        "rule allowed",
			tenant:      "demo-1",
			allowed:     sandboxOnly,
			wantBackend: config.BackendSandbox,
			wantSource:  routeSourceRule,
		},
		{name: "rule not allowed", tenant: "load-1", allowed: sandboxOnly, wantErr: types.ErrBackendNotAllowed},
		{
			name:        "default replaced by the first allowed backend",
			allowed:     sandboxThenDummy,
			wantBackend: config.BackendSandbox,
			wantSource:  routeSourceUser,
		},
		{
			name:        "tenant matching no rule uses the first allowed backend",
			tenant:      "42",
			allowed:     sandboxOnly,
			wantBackend: config.BackendSandbox,
			wantSource:  routeSourceUser,
		},
		{
			name:        "allowed default backend is kept",
			allowed:     withDefault,
			wantBackend: config.BackendClickHouse,
			wantSource:  routeSourceDefault,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			name, source, err := router.resolveBackend(tc.tenant, tc.requested, tc.allowed)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}

				// The two errors are answered with different status codes (400 and 403).
				if errors.Is(err, types.ErrBackendNotAllowed) && errors.Is(err, types.ErrBackendNotSelectable) {
					t.Errorf("err = %v wraps both errors", err)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if name != tc.wantBackend || source != tc.wantSource {
				t.Errorf("resolveBackend = (%q, %q), want (%q, %q)", name, source, tc.wantBackend, tc.wantSource)
			}
		})
	}
}

// TestRoutingFromConfig starts a dummy primary with a routed sandbox backend and
// checks that the queries are served by the backend selected by the header or
// the tenant rules.
func TestRoutingFromConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "squirreldb.conf")
	configContent := `
backend: dummy
internal:
  dummy:
    batcher: false
routing:
  backends: [sandbox]
  rules:
    - tenants: ["demo-*", "1234"]
      backend: sandbox
`

	if err := os.WriteFile(configPath, []byte(configContent), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, warnings, err := config.LoadConfig(nil, []string{configPath})
	if err != nil {
		t.Fatal(err)
	}

	if len(warnings) > 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}

	ctx := t.Context()
	reg := prometheus.NewRegistry()
	squirrelDB := New(Deps{Logger: zerolog.Nop(), MetricRegistry: reg}, cfg, SquirrelDBOption{})

	defer squirrelDB.CloseAll(context.Background())

	queryable, err := squirrelDB.Queryable(ctx)
	if err != nil {
		t.Fatalf("Queryable: %v", err)
	}

	appendable, err := squirrelDB.Appendable(ctx)
	if err != nil {
		t.Fatalf("Appendable: %v", err)
	}

	engine := apipromql.NewEngine(zerolog.Nop(), reg)
	tenantLabel := cfg.TenantLabelName

	cases := []struct {
		name       string
		options    types.QuerierOptions
		wantSeries bool
		wantErr    bool
	}{
		{name: "default route", options: types.QuerierOptions{}, wantSeries: false},
		{name: "tenant matching no rule", options: types.QuerierOptions{Tenant: "42"}, wantSeries: false},
		{name: "header", options: types.QuerierOptions{Backend: config.BackendSandbox}, wantSeries: true},
		{name: "tenant rule", options: types.QuerierOptions{Tenant: "demo-42"}, wantSeries: true},
		{
			name:       "header takes precedence over rules",
			options:    types.QuerierOptions{Tenant: "1234", Backend: config.BackendDummy},
			wantSeries: false,
		},
		{name: "invalid header", options: types.QuerierOptions{Backend: config.BackendCassandra}, wantErr: true},
		{
			name:       "restricted user without header nor rule",
			options:    types.QuerierOptions{Tenant: "42", AllowedBackends: []string{config.BackendSandbox}},
			wantSeries: true,
		},
	}

	for _, tc := range cases {
		matrix, err := rangeQuery(ctx, engine, queryable, tc.options, "agent_status")
		if tc.wantErr {
			if !errors.Is(err, types.ErrBackendNotSelectable) {
				t.Errorf("%s: err = %v, want %v", tc.name, err, types.ErrBackendNotSelectable)
			}

			continue
		}

		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}

		if got := len(matrix) > 0; got != tc.wantSeries {
			t.Errorf("%s: got %d series, want series = %v", tc.name, len(matrix), tc.wantSeries)
		}

		// The tenant reaches the sandbox backend, which adds the tenant label.
		for _, series := range matrix {
			if got := series.Metric.Get(tenantLabel); got != tc.options.Tenant {
				t.Errorf("%s: series %s has tenant %q, want %q", tc.name, series.Metric, got, tc.options.Tenant)
			}
		}
	}

	// Writes routed to the sandbox are accepted (and discarded).
	app := appendable.AppenderWithOptions(ctx, types.AppenderOptions{Tenant: "demo-42"})
	if _, err := app.Append(0, labels.FromStrings("__name__", "cpu_used"), time.Now().UnixMilli(), 42); err != nil {
		t.Fatal(err)
	}

	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}

	app = appendable.AppenderWithOptions(ctx, types.AppenderOptions{Backend: "postgres"})
	if err := app.Commit(); !errors.Is(err, types.ErrBackendNotSelectable) {
		t.Errorf("Commit with an invalid backend: err = %v, want %v", err, types.ErrBackendNotSelectable)
	}

	wantCounters := map[[3]string]float64{
		{config.BackendDummy, routeOperationRead, routeSourceDefault}:  2,
		{config.BackendDummy, routeOperationRead, routeSourceHeader}:   1,
		{config.BackendSandbox, routeOperationRead, routeSourceHeader}: 1,
		{config.BackendSandbox, routeOperationRead, routeSourceRule}:   1,
		{config.BackendSandbox, routeOperationWrite, routeSourceRule}:  1,
		{config.BackendSandbox, routeOperationRead, routeSourceUser}:   1,
	}

	for lbls, want := range wantCounters {
		got := testutil.ToFloat64(squirrelDB.routingMetrics.RequestsTotal.WithLabelValues(lbls[0], lbls[1], lbls[2]))
		if got != want {
			t.Errorf("squirreldb_routing_requests_total%v = %v, want %v", lbls, got, want)
		}
	}
}

// TestRoutingWrites checks that a write routed to a routed backend is sent only
// to it, and that the default route is still dual-written.
func TestRoutingWrites(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy
	cfg.ExtraWriteBackend = config.BackendCassandra
	cfg.Routing = config.Routing{
		Backends: []string{config.BackendSandbox},
		Rules:    []config.RoutingRule{{Tenants: []string{"demo-*"}, Backend: config.BackendSandbox}},
	}

	squirrelDB := New(Deps{Logger: zerolog.Nop(), MetricRegistry: prometheus.NewRegistry()}, cfg, SquirrelDBOption{})

	var commitOrder []string

	primary := newFakeBackend("primary", &commitOrder)
	extra := newFakeBackend("extra", &commitOrder)
	routed := newFakeBackend("sandbox", &commitOrder)

	// The backends are already "started": Primary, Extra and Routed return them.
	squirrelDB.primary = primary
	squirrelDB.extra = extra
	squirrelDB.routed = map[string]Backend{config.BackendSandbox: routed}

	appendable, err := squirrelDB.Appendable(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	write := func(options types.AppenderOptions) {
		t.Helper()

		app := appendable.AppenderWithOptions(t.Context(), options)
		if _, err := app.Append(0, labels.FromStrings("__name__", "cpu_used"), 1, 2); err != nil {
			t.Fatal(err)
		}

		if err := app.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	write(types.AppenderOptions{Backend: config.BackendSandbox, Tenant: "1234", WriteTTL: 42})
	write(types.AppenderOptions{Tenant: "demo-1"})

	if got := strings.Join(commitOrder, ","); got != "sandbox,sandbox" {
		t.Errorf("commits = %s, want only the routed backend", got)
	}

	if got := routed.tsdb.options[0]; got.Tenant != "1234" || got.WriteTTL != 42 {
		t.Errorf("routed backend options = %+v, want the request options", got)
	}

	commitOrder = commitOrder[:0]

	write(types.AppenderOptions{Tenant: "42"})

	if got := strings.Join(commitOrder, ","); got != "primary,extra" {
		t.Errorf("commits = %s, want the primary then the extra (dual-write)", got)
	}

	// A user restricted to the sandbox writes to it without header nor rule, and
	// can't write to the default backend.
	commitOrder = commitOrder[:0]

	allowed := []string{config.BackendSandbox}

	write(types.AppenderOptions{Tenant: "42", AllowedBackends: allowed})

	if got := strings.Join(commitOrder, ","); got != "sandbox" {
		t.Errorf("commits = %s, want the first allowed backend", got)
	}

	app := appendable.AppenderWithOptions(t.Context(), types.AppenderOptions{
		Backend:         config.BackendDummy,
		AllowedBackends: allowed,
	})
	if err := app.Commit(); !errors.Is(err, types.ErrBackendNotAllowed) {
		t.Errorf("Commit to a backend not allowed: err = %v, want %v", err, types.ErrBackendNotAllowed)
	}

	if got := strings.Join(commitOrder, ","); got != "sandbox" {
		t.Errorf("commits = %s, the write to a backend not allowed was committed", got)
	}
}

// TestRoutingDisabled checks that without routing the read and write paths are
// the primary's, as before routing existed.
func TestRoutingDisabled(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy
	cfg.Internal.Dummy.Batcher = false

	ctx := t.Context()
	reg := prometheus.NewRegistry()
	squirrelDB := New(Deps{Logger: zerolog.Nop(), MetricRegistry: reg}, cfg, SquirrelDBOption{})

	defer squirrelDB.CloseAll(context.Background())

	queryable, err := squirrelDB.Queryable(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := queryable.(*routingQueryable); ok {
		t.Error("Queryable is a routing queryable without routing")
	}

	appendable, err := squirrelDB.Appendable(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := appendable.(*routingAppendable); ok {
		t.Error("Appendable is a routing appendable without routing")
	}

	routed, err := squirrelDB.Routed(ctx)
	if err != nil || len(routed) != 0 {
		t.Errorf("Routed = %v, %v, want no backend", routed, err)
	}

	// The header may only name the primary backend.
	if name, err := squirrelDB.ResolveBackend(ctx, "1234", config.BackendDummy, nil); err != nil ||
		name != config.BackendDummy {
		t.Errorf("ResolveBackend(primary) = %q, %v", name, err)
	}

	if _, err := squirrelDB.ResolveBackend(ctx, "", config.BackendSandbox, nil); !errors.Is(
		err, types.ErrBackendNotSelectable,
	) {
		t.Errorf("ResolveBackend(sandbox) err = %v, want %v", err, types.ErrBackendNotSelectable)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "squirreldb_routing_") {
			t.Errorf("metric %s registered without routing", family.GetName())
		}
	}
}

// TestRoutedBackendStartFailure checks that a routed backend failing to start
// fails the read and write paths, hence the daemon startup.
func TestRoutedBackendStartFailure(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy
	cfg.Internal.Dummy.Batcher = false
	cfg.Sandbox.DatasetConfig = filepath.Join(t.TempDir(), "missing.yaml")
	cfg.Routing.Backends = []string{config.BackendSandbox}

	squirrelDB := New(Deps{Logger: zerolog.Nop(), MetricRegistry: prometheus.NewRegistry()}, cfg, SquirrelDBOption{})

	defer squirrelDB.CloseAll(context.Background())

	if _, err := squirrelDB.Queryable(t.Context()); err == nil {
		t.Error("Queryable succeeded with a routed backend failing to start")
	}

	if _, err := squirrelDB.Appendable(t.Context()); err == nil {
		t.Error("Appendable succeeded with a routed backend failing to start")
	}
}

// TestRoutingWithLabeledPrimaryMetrics checks that a routed backend starts when
// the primary registered the mutable label metrics with a "backend" label, as the
// Cassandra and ClickHouse backends do: the metrics of a routed backend must have
// the same label names to share the registry.
func TestRoutingWithLabeledPrimaryMetrics(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy
	cfg.Internal.Dummy.Batcher = false
	cfg.Routing.Backends = []string{config.BackendSandbox}

	reg := prometheus.NewRegistry()

	// Register the mutable label metrics as a Cassandra primary would; the
	// Cassandra backend itself requires a database.
	mutable.NewProviderAndProcessor(
		t.Context(),
		prometheus.WrapRegistererWith(map[string]string{"backend": config.BackendCassandra}, reg),
		&dummy.LocalCluster{},
		dummy.NewMutableLabelStore(dummy.MutableLabels{}),
		cfg.TenantLabelName,
		zerolog.Nop(),
	)

	squirrelDB := New(Deps{Logger: zerolog.Nop(), MetricRegistry: reg}, cfg, SquirrelDBOption{})

	defer squirrelDB.CloseAll(context.Background())

	if _, err := squirrelDB.Queryable(t.Context()); err != nil {
		t.Fatalf("Queryable: %v", err)
	}

	if _, err := squirrelDB.Appendable(t.Context()); err != nil {
		t.Fatalf("Appendable: %v", err)
	}
}

// TestRoutingCloseAll checks that CloseAll closes the routed backends too.
func TestRoutingCloseAll(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Backend = config.BackendDummy
	cfg.Routing.Backends = []string{config.BackendSandbox}

	squirrelDB := New(Deps{Logger: zerolog.Nop(), MetricRegistry: prometheus.NewRegistry()}, cfg, SquirrelDBOption{})

	var commitOrder []string

	primary := newFakeBackend("primary", &commitOrder)
	routed := newFakeBackend("sandbox", &commitOrder)

	squirrelDB.primary = primary
	squirrelDB.routed = map[string]Backend{config.BackendSandbox: routed}

	squirrelDB.CloseAll(t.Context())

	if !primary.closed.Load() || !routed.closed.Load() {
		t.Errorf("closed: primary = %v, routed = %v, want both", primary.closed.Load(), routed.closed.Load())
	}
}

// TestRoutedRegisterer checks that a routed backend registering the same metric
// as the primary doesn't fail, and doesn't unregister the primary's metric.
func TestRoutedRegisterer(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	newCounter := func() prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: "test_total", Help: "test"})
	}

	primaryCounter := newCounter()
	reg.MustRegister(primaryCounter)

	routedReg := &routedRegisterer{reg: reg}
	routedCounter := newCounter()

	routedReg.MustRegister(routedCounter)

	if routedReg.Unregister(routedCounter) {
		t.Error("Unregister of an ignored duplicate returned true")
	}

	if !reg.Unregister(primaryCounter) {
		t.Error("the primary's counter was unregistered")
	}

	otherCounter := prometheus.NewCounter(prometheus.CounterOpts{Name: "other_total", Help: "test"})
	routedReg.MustRegister(otherCounter)

	if !routedReg.Unregister(otherCounter) {
		t.Error("Unregister of a registered collector returned false")
	}
}

func rangeQuery(
	ctx context.Context,
	engine promql.QueryEngine,
	queryable types.QueryableWithOptions,
	options types.QuerierOptions,
	expr string,
) (promql.Matrix, error) {
	promQueryable := storage.QueryableFunc(func(mint, maxt int64) (storage.Querier, error) {
		return queryable.QuerierWithOptions(options, mint, maxt)
	})

	end := time.Now()

	query, err := engine.NewRangeQuery(ctx, promQueryable, nil, expr, end.Add(-time.Hour), end, time.Minute)
	if err != nil {
		return nil, err
	}

	defer query.Close()

	result := query.Exec(ctx)
	if result.Err != nil {
		return nil, result.Err
	}

	return result.Matrix()
}

// fakeBackend is a started Backend whose TSDB records the writes. Only the
// methods used by the routing are implemented.
type fakeBackend struct {
	Backend

	tsdb   *recordingTSDB
	closed atomic.Bool
}

func newFakeBackend(name string, commitOrder *[]string) *fakeBackend {
	return &fakeBackend{
		tsdb: &recordingTSDB{appender: &recordingAppender{name: name, commitOrder: commitOrder}},
	}
}

func (f *fakeBackend) TSDB(context.Context) (types.TSDB, error) {
	return f.tsdb, nil
}

func (f *fakeBackend) BackendInfo() types.BackendInfo {
	return types.BackendInfo{ApplyMutableLabelInternally: true}
}

func (f *fakeBackend) CloseAll(context.Context) {
	f.closed.Store(true)
}

// recordingTSDB is a TSDB serving empty reads whose appenders record the writes
// and the options they were created with.
type recordingTSDB struct {
	dummy.DiscardTSDB

	appender *recordingAppender
	options  []types.AppenderOptions
}

func (r *recordingTSDB) AppenderWithOptions(_ context.Context, options types.AppenderOptions) storage.Appender {
	r.options = append(r.options, options)

	return r.appender
}

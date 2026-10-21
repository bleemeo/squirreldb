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

package requestscoped

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// recordingQueryable records the options and matchers of the last Select.
type recordingQueryable struct {
	opts     types.QuerierOptions
	matchers []*labels.Matcher
}

func (q *recordingQueryable) QuerierWithOptions(opts types.QuerierOptions, _, _ int64) (storage.Querier, error) {
	q.opts = opts

	return recordingQuerier{parent: q}, nil
}

func (q *recordingQueryable) ChunkQuerierWithOptions(types.QuerierOptions, int64, int64) (storage.ChunkQuerier, error) {
	panic("not implemented")
}

type recordingQuerier struct {
	storage.Querier

	parent *recordingQueryable
}

func (q recordingQuerier) Select(
	_ context.Context,
	_ bool,
	_ *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	q.parent.matchers = matchers

	return storage.EmptySeriesSet()
}

func (q recordingQuerier) Close() error {
	return nil
}

// TestIdentityTenantAndForcedMatchers checks that the tenant of the authenticated user
// is used, and that its forced matchers are combined with the forced matcher header.
func TestIdentityTenantAndForcedMatchers(t *testing.T) {
	t.Parallel()

	userMatchers, err := auth.ParseForcedMatcher(`env="prod",team=~"a|b"`)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name         string
		identity     *auth.Identity
		headers      map[string]string
		wantTenant   string
		wantMatchers []string
	}{
		{
			name:         "no identity uses the headers",
			headers:      map[string]string{types.HeaderTenant: "t1", types.HeaderForcedMatcher: "zone=eu"},
			wantTenant:   "t1",
			wantMatchers: []string{`__name__="up"`, `zone="eu"`, `__account_id="t1"`},
		},
		{
			name:         "tenant-bound user without header",
			identity:     &auth.Identity{Name: "u", Tenant: "acme"},
			wantTenant:   "acme",
			wantMatchers: []string{`__name__="up"`, `__account_id="acme"`},
		},
		{
			name:         "user without tenant keeps the tenant header",
			identity:     &auth.Identity{Name: "u", ForcedMatchers: userMatchers},
			headers:      map[string]string{types.HeaderTenant: "t1"},
			wantTenant:   "t1",
			wantMatchers: []string{`__name__="up"`, `env="prod"`, `team=~"a|b"`, `__account_id="t1"`},
		},
		{
			name:     "user forced matchers are combined with the header",
			identity: &auth.Identity{Name: "u", Tenant: "acme", ForcedMatchers: userMatchers},
			headers:  map[string]string{types.HeaderForcedMatcher: "env=dev"},
			// Both env matchers apply (AND): the header can't replace the user's.
			wantTenant:   "acme",
			wantMatchers: []string{`__name__="up"`, `env="dev"`, `env="prod"`, `team=~"a|b"`, `__account_id="acme"`},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			queryable := &recordingQueryable{}
			store := NewStore(Options{
				TenantLabelName: "__account_id",
				Queryable:       queryable,
				MetricRegistry:  prometheus.NewRegistry(),
			})

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/query", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}

			if tc.identity != nil {
				req = req.WithContext(auth.ContextWithIdentity(req.Context(), tc.identity))
			}

			ctx, _, err := store.ContextWithRequest(req.Context(), req, "query")
			if err != nil {
				t.Fatal(err)
			}

			querier, err := store.Querier(0, 1000)
			if err != nil {
				t.Fatal(err)
			}

			set := querier.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"))
			if set.Err() != nil {
				t.Fatal(set.Err())
			}

			if queryable.opts.Tenant != tc.wantTenant {
				t.Errorf("tenant = %q, want %q", queryable.opts.Tenant, tc.wantTenant)
			}

			got := make([]string, 0, len(queryable.matchers))
			for _, m := range queryable.matchers {
				got = append(got, m.String())
			}

			if !slices.Equal(got, tc.wantMatchers) {
				t.Errorf("matchers = %v, want %v", got, tc.wantMatchers)
			}
		})
	}
}

// recordingResolver records the arguments of the last ResolveBackend.
type recordingResolver struct {
	tenant, requested string
	allowed           []string
}

func (r *recordingResolver) ResolveBackend(
	_ context.Context,
	tenant, requested string,
	allowed []string,
) (string, error) {
	r.tenant, r.requested, r.allowed = tenant, requested, allowed

	return requested, nil
}

// TestIdentityBackends checks that the backends of the authenticated user are
// given to the backend resolver and carried by the querier options, so the
// routing queryable resolves the same backend as the early check.
func TestIdentityBackends(t *testing.T) {
	t.Parallel()

	queryable := &recordingQueryable{}
	resolver := &recordingResolver{}
	store := NewStore(Options{
		TenantLabelName: "__account_id",
		Queryable:       queryable,
		BackendResolver: resolver,
		MetricRegistry:  prometheus.NewRegistry(),
	})

	identity := &auth.Identity{Name: "demo", Tenant: "acme", Backends: []string{"sandbox"}}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/query", nil)
	req.Header.Set(types.HeaderBackend, "sandbox")
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), identity))

	ctx, _, err := store.ContextWithRequest(req.Context(), req, "query")
	if err != nil {
		t.Fatal(err)
	}

	if resolver.tenant != "acme" || resolver.requested != "sandbox" || !slices.Equal(resolver.allowed, identity.Backends) {
		t.Errorf("ResolveBackend(%q, %q, %v), want the tenant, header and backends of the user",
			resolver.tenant, resolver.requested, resolver.allowed)
	}

	querier, err := store.Querier(0, 1000)
	if err != nil {
		t.Fatal(err)
	}

	set := querier.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"))
	if set.Err() != nil {
		t.Fatal(set.Err())
	}

	if got := queryable.opts; got.Backend != "sandbox" || !slices.Equal(got.AllowedBackends, identity.Backends) {
		t.Errorf("querier options backend = %q, allowed = %v, want the header and the backends of the user",
			got.Backend, got.AllowedBackends)
	}
}

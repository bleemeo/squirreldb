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
	"fmt"
	"net/http"
	"path"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/bleemeo/squirreldb/appendable/fixedappender"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/storage"
)

// Sources of a routing decision, the "source" label of squirreldb_routing_requests_total.
const (
	routeSourceHeader  = "header"
	routeSourceRule    = "rule"
	routeSourceDefault = "default"
	// routeSourceUser is a request which would have gone to the default backend,
	// served by the first backend its user is allowed to use instead.
	routeSourceUser = "user"
)

// Operations of a routed request, the "operation" label of squirreldb_routing_requests_total.
const (
	routeOperationRead  = "read"
	routeOperationWrite = "write"
)

// backendRouter selects the backend serving a request: the backend requested by
// the header if any, else the backend of the first rule matching the tenant,
// else the default (primary) backend. See docs/routing.md.
type backendRouter struct {
	defaultBackend string
	// selectable are the backends a request may be routed to: the default
	// backend and the routed backends.
	selectable []string
	rules      []config.RoutingRule
}

func newBackendRouter(cfg config.Config) *backendRouter {
	selectable := append([]string{cfg.Backend}, cfg.Routing.Backends...)
	slices.Sort(selectable)

	return &backendRouter{
		defaultBackend: cfg.Backend,
		selectable:     slices.Compact(selectable),
		rules:          cfg.Routing.Rules,
	}
}

// resolveBackend returns the name of the backend serving a request of tenant
// which requested the backend requested (the HeaderBackend value, empty when
// absent), and the source of that decision. allowed are the backends the request
// may use (the backends of the authenticated user), empty meaning any selectable
// backend. It's the single place where the backend of a request is decided, for
// reads and writes. The backend is:
//
//   - the requested backend, if any;
//   - else the backend of the first rule matching the tenant;
//   - else the default backend, or the first allowed backend when allowed is set
//     and doesn't contain the default backend.
//
// It returns an error wrapping types.ErrBackendNotSelectable if the requested
// backend isn't enabled, and an error wrapping types.ErrBackendNotAllowed if the
// requested backend or the backend of the matching rule isn't allowed.
func (r *backendRouter) resolveBackend(tenant, requested string, allowed []string) (string, string, error) {
	restricted := len(allowed) > 0

	if requested != "" {
		if !slices.Contains(r.selectable, requested) {
			// A restricted user is only told about the backends it may use.
			enabled := r.selectable
			if restricted {
				enabled = allowed
			}

			return "", "", fmt.Errorf(
				"%w: %q (from header %s), the enabled backends are: %s",
				types.ErrBackendNotSelectable, requested, types.HeaderBackend, strings.Join(enabled, ", "),
			)
		}

		if restricted && !slices.Contains(allowed, requested) {
			return "", "", fmt.Errorf(
				"%w: the user isn't allowed to use the backend %q (from header %s), its backends are: %s",
				types.ErrBackendNotAllowed, requested, types.HeaderBackend, strings.Join(allowed, ", "),
			)
		}

		return requested, routeSourceHeader, nil
	}

	if tenant != "" {
		for _, rule := range r.rules {
			if !ruleMatchTenant(rule, tenant) {
				continue
			}

			if restricted && !slices.Contains(allowed, rule.Backend) {
				return "", "", fmt.Errorf(
					"%w: the user isn't allowed to use the backend %q (routing rule of tenant %q), its backends are: %s",
					types.ErrBackendNotAllowed, rule.Backend, tenant, strings.Join(allowed, ", "),
				)
			}

			return rule.Backend, routeSourceRule, nil
		}
	}

	if restricted && !slices.Contains(allowed, r.defaultBackend) {
		return allowed[0], routeSourceUser, nil
	}

	return r.defaultBackend, routeSourceDefault, nil
}

// ruleMatchTenant tells whether one of the rule's patterns matches the tenant.
// The patterns are validated with the config, an invalid one never matches.
func ruleMatchTenant(rule config.RoutingRule, tenant string) bool {
	for _, pattern := range rule.Tenants {
		if ok, err := path.Match(pattern, tenant); err == nil && ok {
			return true
		}
	}

	return false
}

type routingMetrics struct {
	RequestsTotal *prometheus.CounterVec
}

func newRoutingMetrics(reg prometheus.Registerer) *routingMetrics {
	return &routingMetrics{
		RequestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "routing",
			Name:      "requests_total",
			Help: "Number of routed reads (queriers) and writes (appenders) by backend, operation " +
				"and source of the routing decision (header, rule, default or user)",
		}, []string{"backend", "operation", "source"}),
	}
}

// routingQueryable is the read path when routing is enabled: each querier is
// served by the backend resolved from its options (tenant, requested backend and
// allowed backends).
type routingQueryable struct {
	router  *backendRouter
	metrics *routingMetrics
	// targets are the queryables of the selectable backends, by name.
	targets map[string]types.QueryableWithOptions
}

func (q *routingQueryable) route(opts types.QuerierOptions) (types.QueryableWithOptions, types.QuerierOptions, error) {
	name, source, err := q.router.resolveBackend(opts.Tenant, opts.Backend, opts.AllowedBackends)
	if err != nil {
		return nil, opts, err
	}

	target, ok := q.targets[name]
	if !ok {
		return nil, opts, fmt.Errorf("%w: backend %q isn't started", types.ErrBackendNotSelectable, name)
	}

	q.metrics.RequestsTotal.WithLabelValues(name, routeOperationRead, source).Inc()

	opts.BackendOptions = backendOptionsFor(opts.BackendOptions, name)

	return target, opts, nil
}

func (q *routingQueryable) QuerierWithOptions(
	opts types.QuerierOptions,
	mint, maxt int64,
) (storage.Querier, error) {
	target, opts, err := q.route(opts)
	if err != nil {
		return nil, err
	}

	return target.QuerierWithOptions(opts, mint, maxt)
}

func (q *routingQueryable) ChunkQuerierWithOptions(
	opts types.QuerierOptions,
	mint, maxt int64,
) (storage.ChunkQuerier, error) {
	target, opts, err := q.route(opts)
	if err != nil {
		return nil, err
	}

	return target.ChunkQuerierWithOptions(opts, mint, maxt)
}

// routingAppendable is the write path when routing is enabled: each appender
// writes only to the backend resolved from its options. The default backend's
// appendable is the (possibly dual-write) primary write path; a routed backend
// is written alone, never dual-written.
type routingAppendable struct {
	router  *backendRouter
	metrics *routingMetrics
	// targets are the appendables of the selectable backends, by name.
	targets map[string]types.AppendableWithOptions
}

func (a *routingAppendable) AppenderWithOptions(
	ctx context.Context,
	opts types.AppenderOptions,
) storage.Appender {
	name, source, err := a.router.resolveBackend(opts.Tenant, opts.Backend, opts.AllowedBackends)
	if err != nil {
		return fixedappender.NewErrorAppender(err)
	}

	target, ok := a.targets[name]
	if !ok {
		return fixedappender.NewErrorAppender(
			fmt.Errorf("%w: backend %q isn't started", types.ErrBackendNotSelectable, name),
		)
	}

	a.metrics.RequestsTotal.WithLabelValues(name, routeOperationWrite, source).Inc()

	opts.BackendOptions = backendOptionsFor(opts.BackendOptions, name)

	return target.AppenderWithOptions(ctx, opts)
}

// routedBackendOptions are the per-request backend options (BackendOptions of
// the querier/appender options) when routing is enabled: the options parsed by
// each selectable backend, by name. The routing queryable/appendable hands to
// the backend serving the request the options it parsed itself.
type routedBackendOptions map[string]any

// backendOptionsFor returns the backend options of the backend name.
func backendOptionsFor(options any, name string) any {
	if routed, ok := options.(routedBackendOptions); ok {
		return routed[name]
	}

	return options
}

// routedOptionsParser parses the per-request backend options from the HTTP
// headers with the parser of every selectable backend which has one.
type routedOptionsParser struct {
	// parsers are the parsers of the selectable backends, by name.
	parsers map[string]types.HTTPOptionsParser
}

func (p routedOptionsParser) ParseHTTPOptions(h http.Header) (any, error) {
	result := make(routedBackendOptions, len(p.parsers))

	for name, parser := range p.parsers {
		options, err := parser.ParseHTTPOptions(h)
		if err != nil {
			return nil, err
		}

		result[name] = options
	}

	return result, nil
}

// routedRegisterer is the metric registerer of the routed backends. They share
// the registry of the primary backend, and some of their components may register
// the same metrics as the primary's (the backends tag their shared component
// metrics with a "backend" label, so this happens only between backends of the
// same type): such a duplicate registration is ignored instead of failing (a
// backend registers its metrics with promauto, which panics on error), so the
// duplicate metrics of a routed backend are not exported while its own metrics
// are. A registration conflicting with a different descriptor still fails.
type routedRegisterer struct {
	reg prometheus.Registerer

	l sync.Mutex
	// registered are the collectors this registerer actually registered, the
	// only ones it may unregister: unregistering an ignored duplicate would
	// unregister the primary's collector which has the same descriptors.
	registered []prometheus.Collector
}

func (r *routedRegisterer) Register(c prometheus.Collector) error {
	err := r.reg.Register(c)
	if err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			return nil
		}

		return err
	}

	r.l.Lock()
	defer r.l.Unlock()

	r.registered = append(r.registered, c)

	return nil
}

func (r *routedRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := r.Register(c); err != nil {
			panic(err)
		}
	}
}

func (r *routedRegisterer) Unregister(c prometheus.Collector) bool {
	r.l.Lock()
	defer r.l.Unlock()

	for i, registered := range r.registered {
		if sameCollector(registered, c) {
			r.registered = slices.Delete(r.registered, i, i+1)

			return r.reg.Unregister(c)
		}
	}

	return false
}

// sameCollector compares two collectors without panicking on a non-comparable
// collector type.
func sameCollector(a, b prometheus.Collector) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) || !reflect.TypeOf(a).Comparable() {
		return false
	}

	return a == b
}

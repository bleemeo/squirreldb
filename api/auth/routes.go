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
	"net/http"
	"strings"
)

// RouteClassifier returns the scope a request needs to be served.
type RouteClassifier func(r *http.Request) Scope

const apiPrefix = "/api/v1"

// readRoutes are the Prometheus API routes (relative to /api/v1) that only read data.
// They are registered by the Prometheus v1 API (see api.NewPrometheus).
var readRoutes = map[string]bool{ //nolint:gochecknoglobals
	"/query":            true,
	"/query_range":      true,
	"/query_exemplars":  true,
	"/format_query":     true,
	"/parse_query":      true,
	"/labels":           true,
	"/series":           true,
	"/metadata":         true,
	"/read":             true,
	"/scrape_pools":     true,
	"/targets":          true,
	"/targets/metadata": true,
	"/alertmanagers":    true,
	"/alerts":           true,
	"/rules":            true,
}

// writeRoutes are the Prometheus API routes (relative to /api/v1) that write data.
var writeRoutes = map[string]bool{ //nolint:gochecknoglobals
	"/write":           true,
	"/otlp/v1/metrics": true,
}

// ClassifyRoute classifies the routes of the main listener. Only the routes explicitly
// listed are read or write routes: every other route, including the admin endpoints
// and any route added in the future, requires the admin scope (fail closed).
// The paths are matched exactly, as the router does: a path the router would only
// redirect (e.g. with a trailing slash) is classified as admin.
func ClassifyRoute(r *http.Request) Scope {
	path := r.URL.Path

	switch path {
	case "/ready":
		return scopePublic
	case "/metrics":
		return scopeMetrics
	}

	route, isAPI := strings.CutPrefix(path, apiPrefix)
	if !isAPI {
		return ScopeAdmin
	}

	switch {
	case writeRoutes[route]:
		return ScopeWrite
	case r.Method == http.MethodDelete:
		// DELETE /api/v1/series drops series.
		return ScopeAdmin
	case readRoutes[route], isLabelValuesRoute(route), isStatusRoute(route):
		return ScopeRead
	default:
		return ScopeAdmin
	}
}

// ClassifyAdminRoute classifies the routes of the admin listener: everything requires
// the admin scope, except /ready and /metrics.
func ClassifyAdminRoute(r *http.Request) Scope {
	switch r.URL.Path {
	case "/ready":
		return scopePublic
	case "/metrics":
		return scopeMetrics
	default:
		return ScopeAdmin
	}
}

// isLabelValuesRoute matches /label/:name/values.
func isLabelValuesRoute(route string) bool {
	name, found := strings.CutPrefix(route, "/label/")
	if !found {
		return false
	}

	name, found = strings.CutSuffix(name, "/values")

	return found && name != "" && !strings.Contains(name, "/")
}

// isStatusRoute matches /status/*, the read-only server status (build info, TSDB stats, ...).
func isStatusRoute(route string) bool {
	rest, found := strings.CutPrefix(route, "/status/")

	return found && rest != ""
}

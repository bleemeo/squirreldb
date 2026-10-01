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
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/bleemeo/squirreldb/api/status"
)

const (
	statusPath = "/debug/status"
	// jsonCSP is the Content-Security-Policy of the responses that are not the page.
	jsonCSP = "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
)

// setStatusSecurityHeaders sets the headers of the admin status responses: they
// must not be cached (they show the live state and the configuration), framed,
// sniffed, nor leak their URL to another site.
func setStatusSecurityHeaders(w http.ResponseWriter, csp string) {
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Content-Security-Policy", csp)
}

// statusHandler serves the read-only status report: as JSON, or as the HTML page
// with ?format=html.
func (a *API) statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("format") == "html" {
		a.statusPageHandler(w, r, url.Values{"format": {"html"}})

		return
	}

	report := a.status.Collect(r.Context())

	setStatusSecurityHeaders(w, jsonCSP)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	if err := enc.Encode(report); err != nil {
		a.Logger.Debug().Err(err).Msg("Failed to write the status report")
	}
}

// statusPageHandler serves the status report as an HTML page. query are the
// parameters the auto-refresh links of the page must keep.
func (a *API) statusPageHandler(w http.ResponseWriter, r *http.Request, query url.Values) {
	report := a.status.Collect(r.Context())

	setStatusSecurityHeaders(w, a.statusPage.ContentSecurityPolicy())
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	links, docs := a.statusPageEndpoints()

	err := a.statusPage.Render(w, report, status.PageOptions{
		Path:     r.URL.Path,
		Query:    query,
		Refresh:  status.ParseRefresh(r.URL.Query()),
		JSONPath: statusPath,
		Links:    links,
		Docs:     docs,
	})
	if err != nil {
		a.Logger.Warn().Err(err).Msg("Failed to render the status page")

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		http.Error(w, "failed to render the status page, see the logs or "+statusPath, http.StatusInternalServerError)
	}
}

// statusPageEndpoints returns the admin endpoints listed on the status page: the
// read-only ones that are cheap and need no parameter are links, the others
// (that change state, are expensive or need parameters) are only documented.
func (a *API) statusPageEndpoints() (links []status.Endpoint, docs []status.Endpoint) {
	get := func(path, description string) status.Endpoint {
		return status.Endpoint{Method: http.MethodGet, Path: path, Description: description}
	}
	post := func(path, description string) status.Endpoint {
		return status.Endpoint{Method: http.MethodPost, Path: path, Description: description}
	}

	links = []status.Endpoint{
		get(statusPath, "this status as JSON"),
		get("/debug/", "this page, or the help of the debug endpoints with curl"),
		get("/debug/pprof/", "Go pprof profiles"),
		get("/metrics", "Prometheus metrics"),
		get("/ready", "readiness"),
	}

	if a.MutableLabelWriter != nil {
		links = append(links, get("/debug/mutable_dump", "CSV of the mutable labels"))
	}

	if a.DebugEndpoint != nil {
		links = append(links, get("/debug/index_info", "global information about the index"))

		docs = append(docs,
			get("/debug/index_verify", "verify the index consistency (expensive)"),
			get("/debug/index_dump", "CSV of every metric (expensive)"),
			post("/debug/index_verify?fix=1", "verify and fix the index"),
			post("/debug/flush", "flush the memory store to the TSDB"),
			post("/debug/preaggregate?from=&to=", "re-run the pre-aggregation"),
			post("/debug/index_block", "block the index writes"),
			post("/debug/index_unblock", "unblock the index writes"),
		)
	}

	docs = append(docs,
		post("/debug/toggle_debug_query", "toggle the query debug logs"),
		post("/debug/mutable_import", "replace the mutable labels by a CSV"),
		post("/mutable/names", "write mutable label names"),
		post("/mutable/values", "write mutable label values"),
	)

	for _, e := range a.backendDebugEndpoints {
		endpoint := status.Endpoint{Method: e.method, Path: e.path, Description: e.description}

		// The backend endpoints that change state are registered on POST only.
		if e.method == http.MethodGet {
			links = append(links, endpoint)
		} else {
			docs = append(docs, endpoint)
		}
	}

	return links, docs
}

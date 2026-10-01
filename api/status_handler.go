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
)

// setStatusSecurityHeaders sets the headers of the admin status responses: they
// must not be cached (they show the live state and the configuration), framed,
// sniffed, nor leak their URL to another site.
func setStatusSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
}

// statusHandler serves the read-only status report as JSON.
func (a *API) statusHandler(w http.ResponseWriter, r *http.Request) {
	report := a.status.Collect(r.Context())

	setStatusSecurityHeaders(w)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	if err := enc.Encode(report); err != nil {
		a.Logger.Debug().Err(err).Msg("Failed to write the status report")
	}
}

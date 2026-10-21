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

package status

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	"github.com/bleemeo/squirreldb/types"
)

func renderPage(t *testing.T, report *Report, opts PageOptions) string {
	t.Helper()

	page, err := NewPage()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := page.Render(&buf, report, opts); err != nil {
		t.Fatal(err)
	}

	return buf.String()
}

func TestPageRender(t *testing.T) {
	t.Parallel()

	const injection = `<script>alert("x")</script>`

	collector := newTestCollector(t, Sources{
		Backends: func() []types.BackendState {
			return []types.BackendState{{
				Name: "clickhouse", Role: types.BackendRolePrimary, Started: true,
				Backend: fakeStatus(types.ComponentStatus{
					Status: types.StatusDegraded,
					Reason: "1 of 2 hosts ready " + injection,
					Details: map[string]any{
						"hosts": []map[string]any{
							{"address": "ch1:9000", "state": "ready"},
							{"address": "ch2:9000", "state": "down", "cause": injection},
						},
					},
				}),
			}}
		},
		Config: SecretConfig(),
	})

	body := renderPage(t, collector.Collect(t.Context()), PageOptions{
		Path:     "/debug/",
		JSONPath: "/debug/status",
		Refresh:  30,
		Links:    []Endpoint{{Method: "GET", Path: "/debug/pprof/"}},
		Docs:     []Endpoint{{Method: "POST", Path: "/debug/flush"}},
	})

	if strings.Contains(body, injection) {
		t.Error("a value is not escaped")
	}

	for _, secret := range SecretStrings() {
		if strings.Contains(body, secret) {
			t.Errorf("the page contains the secret %q", secret)
		}
	}

	for _, want := range []string{
		`<meta http-equiv="refresh" content="30">`,
		`<title>Degraded - SquirrelDB`,
		`<td>ch2:9000</td>`,
		`<span class="badge down">down</span>`,
		`<a href="/debug/pprof/">`,
		`<code>POST /debug/flush</code>`,
		`<a href="/debug/?refresh=10">10s</a>`,
		`<a href="/debug/">off</a>`,
		`<details>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the page does not contain %q", want)
		}
	}

	if strings.Contains(body, `href="/debug/flush"`) {
		t.Error("an endpoint that changes state is a link")
	}
}

// TestPageStyleHash checks that the CSP hash matches the inline stylesheet, so
// browsers apply it.
func TestPageStyleHash(t *testing.T) {
	t.Parallel()

	page, err := NewPage()
	if err != nil {
		t.Fatal(err)
	}

	body := renderPage(t, newTestCollector(t, Sources{}).Collect(context.Background()), PageOptions{Path: "/debug/"})

	_, rest, _ := strings.Cut(body, "<style>")
	style, _, found := strings.Cut(rest, "</style>")

	if !found {
		t.Fatal("no inline stylesheet")
	}

	sum := sha256.Sum256([]byte(style))
	want := "style-src 'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'"

	if !strings.Contains(page.ContentSecurityPolicy(), want) {
		t.Errorf("Content-Security-Policy = %q, want %q", page.ContentSecurityPolicy(), want)
	}

	if strings.Contains(body, "http-equiv=\"refresh\"") {
		t.Error("the page auto-refreshes by default")
	}
}

func TestPrefersHTML(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"":                            false,
		"*/*":                         false,
		"text/plain":                  false,
		"application/json":            false,
		"text/html":                   true,
		"application/xhtml+xml":       true,
		"text/html;q=0":               false,
		"text/plain, text/html;q=0.5": false,
		"text/html, text/plain;q=0.5": true,
		"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8": true,
	}

	for accept, want := range cases {
		if got := PrefersHTML(accept); got != want {
			t.Errorf("PrefersHTML(%q) = %v, want %v", accept, got, want)
		}
	}
}

func TestParseRefresh(t *testing.T) {
	t.Parallel()

	cases := map[string]int{"": 0, "10": 10, "30": 30, "1": 0, "abc": 0, "-10": 0}

	for value, want := range cases {
		if got := ParseRefresh(url.Values{"refresh": {value}}); got != want {
			t.Errorf("ParseRefresh(%q) = %d, want %d", value, got, want)
		}
	}
}

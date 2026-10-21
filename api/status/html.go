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
	"crypto/sha256"
	_ "embed" // The page template and its stylesheet are embedded.
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"mime"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/types"
)

//go:embed status.html.tmpl
var pageTemplateText string

//go:embed status.css
var pageCSS string

// RefreshIntervals are the auto-refresh intervals offered by the page, in seconds.
var RefreshIntervals = []int{10, 30, 60} //nolint:gochecknoglobals

// Endpoint is an admin endpoint listed on the page.
type Endpoint struct {
	Method      string
	Path        string
	Description string
}

// PageOptions configures the rendering of the page.
type PageOptions struct {
	// Path is the path the page is served on, used by the auto-refresh links.
	// Query are the parameters these links keep (e.g. format=html).
	Path  string
	Query url.Values
	// Refresh is the auto-refresh interval in seconds, 0 to disable it.
	Refresh int
	// JSONPath is the path of the JSON report.
	JSONPath string
	// Links are read-only endpoints, rendered as links. Docs are the other
	// endpoints (that change state or need parameters), only rendered as text.
	Links []Endpoint
	Docs  []Endpoint
}

// Page renders the status report as an HTML page. It is safe for concurrent use.
type Page struct {
	tmpl *template.Template
	// styleHash is the CSP source of the inline stylesheet.
	styleHash string
}

// NewPage parses the page template.
func NewPage() (*Page, error) {
	tmpl, err := template.New("status").Funcs(template.FuncMap{
		"badge":       badgeClass,
		"statusText":  statusText,
		"formatTime":  formatTime,
		"join":        func(values []string) string { return strings.Join(values, ", ") },
		"sectionHead": func(title string, s Section) sectionHead { return sectionHead{Title: title, Section: s} },
	}).Parse(pageTemplateText)
	if err != nil {
		return nil, fmt.Errorf("parse status page template: %w", err)
	}

	sum := sha256.Sum256([]byte(pageCSS))

	return &Page{
		tmpl:      tmpl,
		styleHash: "'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'",
	}, nil
}

// ContentSecurityPolicy returns the Content-Security-Policy of the page: nothing
// may be loaded but its inline stylesheet (no script, image, font, frame, form).
func (p *Page) ContentSecurityPolicy() string {
	return "default-src 'none'; style-src " + p.styleHash +
		"; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
}

// Render writes the page of the report. Every value is escaped by html/template.
func (p *Page) Render(w io.Writer, report *Report, opts PageOptions) error {
	data := pageData{
		CSS:          template.CSS(pageCSS), //nolint:gosec // The embedded stylesheet is trusted.
		Report:       report,
		Refresh:      opts.Refresh,
		RefreshLinks: refreshLinks(opts),
		JSONHref:     opts.JSONPath,
		Components:   components(report),
		Links:        opts.Links,
		Docs:         opts.Docs,
	}

	if report.Config != nil && report.Config.Values != nil {
		config, err := json.MarshalIndent(report.Config.Values, "", "  ")
		if err != nil {
			return err
		}

		data.Config = string(config)
	}

	// Render to a buffer so a template error never sends a truncated page.
	var buf bytes.Buffer
	if err := p.tmpl.Execute(&buf, data); err != nil {
		return err
	}

	_, err := buf.WriteTo(w)

	return err
}

type pageData struct {
	CSS          template.CSS
	Report       *Report
	Refresh      int
	RefreshLinks []refreshLink
	JSONHref     string
	Components   []component
	Links        []Endpoint
	Docs         []Endpoint
	Config       string
}

type refreshLink struct {
	Label  string
	Href   string
	Active bool
}

type sectionHead struct {
	Title   string
	Section Section
}

// component is a section whose details are rendered generically.
type component struct {
	Title   string
	Section Section
	Details *node
}

func refreshLinks(opts PageOptions) []refreshLink {
	href := func(refresh int) string {
		query := url.Values{}

		for key, values := range opts.Query {
			if key != "refresh" {
				query[key] = values
			}
		}

		if refresh > 0 {
			query.Set("refresh", strconv.Itoa(refresh))
		}

		if len(query) == 0 {
			return opts.Path
		}

		return opts.Path + "?" + query.Encode()
	}

	links := make([]refreshLink, 0, 1+len(RefreshIntervals))
	links = append(links, refreshLink{Label: "off", Href: href(0), Active: opts.Refresh == 0})

	for _, interval := range RefreshIntervals {
		links = append(links, refreshLink{
			Label:  strconv.Itoa(interval) + "s",
			Href:   href(interval),
			Active: opts.Refresh == interval,
		})
	}

	return links
}

// ParseRefresh returns the auto-refresh interval of the refresh query parameter,
// 0 when it is missing or not one of RefreshIntervals.
func ParseRefresh(query url.Values) int {
	refresh, err := strconv.Atoi(query.Get("refresh"))
	if err != nil || !slices.Contains(RefreshIntervals, refresh) {
		return 0
	}

	return refresh
}

func components(report *Report) []component {
	var result []component

	for _, c := range []struct {
		title   string
		section *ComponentSection
	}{
		{"ClickHouse", report.ClickHouse},
		{"Cassandra", report.Cassandra},
		{"Dummy backend", report.Dummy},
	} {
		if c.section != nil {
			result = append(result, component{Title: c.title, Section: c.section.Section, Details: toNode(c.section.Details)})
		}
	}

	if cl := report.Cluster; cl != nil {
		details := map[string]any{"kind": cl.Kind, "size": cl.Size}
		if cl.Error != "" {
			details = nil
		} else if cl.Details != nil {
			details["details"] = cl.Details
		}

		result = append(result, component{Title: "Cluster", Section: cl.Section, Details: toNode(details)})
	}

	return result
}

// node is a JSON value prepared for the generic template: a table (list of
// objects), an object (key/value fields) or a text, optionally shown as a badge.
type node struct {
	Table  *table
	Fields []field
	Text   string
	Badge  string
}

type table struct {
	Columns []string
	Rows    [][]*node
}

type field struct {
	Key   string
	Value *node
}

// leadingColumns are shown first, in this order, in tables and objects.
var leadingColumns = []string{"address", "name", "state", "status"} //nolint:gochecknoglobals

// toNode converts a value to a node, through its JSON form so the page shows
// exactly what the JSON report contains. Nil when there is nothing to show.
func toNode(v any) *node {
	if v == nil {
		return nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		return &node{Text: "cannot render: " + err.Error()}
	}

	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return &node{Text: "cannot render: " + err.Error()}
	}

	return buildNode("", decoded)
}

func buildNode(key string, v any) *node {
	switch value := v.(type) {
	case map[string]any:
		keys := sortedKeys(value)
		fields := make([]field, 0, len(keys))

		for _, k := range keys {
			fields = append(fields, field{Key: k, Value: buildNode(k, value[k])})
		}

		if len(fields) == 0 {
			return &node{Text: "-"}
		}

		return &node{Fields: fields}
	case []any:
		return buildListNode(key, value)
	case nil:
		return &node{Text: "-"}
	case string:
		if value == "" {
			return &node{Text: "-"}
		}

		n := &node{Text: value}
		if key == "status" || key == "state" {
			n.Badge = badgeClass(value)
		}

		return n
	case float64:
		return &node{Text: strconv.FormatFloat(value, 'f', -1, 64)}
	default:
		return &node{Text: fmt.Sprint(value)}
	}
}

// buildListNode renders a list of objects as a table, any other list as text.
func buildListNode(key string, values []any) *node {
	if len(values) == 0 {
		return &node{Text: "-"}
	}

	columnSet := make(map[string]any)
	texts := make([]string, 0, len(values))

	for _, item := range values {
		object, ok := item.(map[string]any)
		if !ok {
			texts = append(texts, buildNode(key, item).Text)

			continue
		}

		for k := range object {
			columnSet[k] = nil
		}
	}

	if len(texts) > 0 {
		return &node{Text: strings.Join(texts, ", ")}
	}

	t := &table{Columns: sortedKeys(columnSet)}

	for _, item := range values {
		object, _ := item.(map[string]any)
		row := make([]*node, len(t.Columns))

		for i, column := range t.Columns {
			row[i] = buildNode(column, object[column])
		}

		t.Rows = append(t.Rows, row)
	}

	return &node{Table: t}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, func(a, b string) int {
		ia, ib := slices.Index(leadingColumns, a), slices.Index(leadingColumns, b)

		switch {
		case ia >= 0 && ib >= 0:
			return ia - ib
		case ia >= 0:
			return -1
		case ib >= 0:
			return 1
		default:
			return strings.Compare(a, b)
		}
	})

	return keys
}

// badgeClass returns the CSS class of a status (or a ClickHouse host state).
func badgeClass(status string) string {
	switch status {
	case types.StatusOK, "ready":
		return "ok"
	case types.StatusDegraded, "reachable":
		return "degraded"
	case types.StatusDown:
		return "down"
	default:
		return "unknown"
	}
}

func statusText(status string) string {
	switch status {
	case types.StatusOK:
		return "Healthy"
	case types.StatusDegraded:
		return "Degraded"
	case types.StatusDown:
		return "Down"
	default:
		return "Unknown"
	}
}

func formatTime(v any) string {
	switch t := v.(type) {
	case time.Time:
		return t.Format(time.RFC3339)
	case *time.Time:
		if t == nil {
			return "-"
		}

		return t.Format(time.RFC3339)
	default:
		return fmt.Sprint(v)
	}
}

// PrefersHTML returns whether an Accept header prefers an HTML page to plain
// text: text/html (or application/xhtml+xml) must be listed explicitly, with a
// quality at least that of text/plain. Browsers list it, curl sends "*/*".
func PrefersHTML(accept string) bool {
	htmlQ, plainQ := -1.0, -1.0

	for part := range strings.SplitSeq(accept, ",") {
		mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil {
			continue
		}

		q := 1.0

		if value, ok := params["q"]; ok {
			if parsed, err := strconv.ParseFloat(value, 64); err == nil {
				q = parsed
			}
		}

		switch mediaType {
		case "text/html", "application/xhtml+xml":
			htmlQ = max(htmlQ, q)
		case "text/plain":
			plainQ = max(plainQ, q)
		}
	}

	return htmlQ > 0 && htmlQ >= plainQ
}

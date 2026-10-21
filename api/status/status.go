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

// Package status builds the read-only status report of the admin listener
// (/debug/status): build, node, backends, cluster, security and the redacted
// configuration. It answers "is this node healthy, and if not why?".
//
// Each section is collected by its own collector, concurrently and with its own
// timeout: a slow or failing collector yields an error in its section, never a
// failed report. Collectors only read state that is already known (health probes,
// counters) or send requests no heavier than the health probes.
package status

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/core/version"
	"github.com/bleemeo/squirreldb/types"
)

// DefaultSectionTimeout bounds the time each section collector may take.
const DefaultSectionTimeout = 2 * time.Second

// certificateExpiryWarning is how long before its expiry the TLS certificate
// makes the security section degraded.
const certificateExpiryWarning = 14 * 24 * time.Hour

// Sources are the sources of the report that come from outside the API. Every
// field is optional: a missing source yields an unknown section.
type Sources struct {
	// Backends returns the state of the configured backends. It must not block.
	Backends func() []types.BackendState
	// Cluster returns the cluster, nil if it is not started. It must not block.
	Cluster func() types.Cluster
	// Telemetry returns the telemetry cluster and node IDs, empty when unknown.
	Telemetry func() (clusterID string, nodeID string)
	// Config is the effective configuration. It is redacted (see Redact) before
	// being added to the report, so it can be passed as-is.
	Config any
}

// Options configures a Collector.
type Options struct {
	Sources

	StartTime          time.Time
	ListenAddress      string
	AdminListenAddress string
	// Ready returns whether the API is ready to serve requests.
	Ready func() bool
	TLS   auth.TLSOptions
	// Auth is the authentication configuration. Only the user names, scopes,
	// tenants and whether they have a forced matcher are reported: never their
	// password or token hashes.
	Auth auth.Options
	// AuthFailures returns the authentication failures by reason.
	AuthFailures func() map[string]float64
	// SectionTimeout bounds each collector, DefaultSectionTimeout when zero.
	SectionTimeout time.Duration
}

// Collector builds status reports.
type Collector struct {
	opts Options
}

// New returns a Collector.
func New(opts Options) *Collector {
	if opts.SectionTimeout <= 0 {
		opts.SectionTimeout = DefaultSectionTimeout
	}

	return &Collector{opts: opts}
}

// Report is the status report. Its JSON form is documented in docs/admin-status.md:
// the fields are only ever added, never renamed nor removed.
type Report struct {
	// Status is the worst status of the node, backends, cluster and security sections.
	Status string `json:"status"`
	// Reasons explain a status other than ok, one per section that isn't ok.
	Reasons     []string         `json:"reasons,omitempty"`
	GeneratedAt time.Time        `json:"generated_at"`
	Build       *BuildSection    `json:"build"`
	Node        *NodeSection     `json:"node"`
	Backends    *BackendsSection `json:"backends"`
	// ClickHouse, Cassandra and Dummy are the details of the backends that are
	// configured (as primary or extra), nil otherwise.
	ClickHouse *ComponentSection `json:"clickhouse,omitempty"`
	Cassandra  *ComponentSection `json:"cassandra,omitempty"`
	Dummy      *ComponentSection `json:"dummy,omitempty"`
	Cluster    *ClusterSection   `json:"cluster"`
	Security   *SecuritySection  `json:"security"`
	Config     *ConfigSection    `json:"config"`
}

// Section is the common header of every section.
type Section struct {
	// Status is one of ok, degraded, down or unknown.
	Status string `json:"status"`
	// Reason is a short explanation of a status other than ok.
	Reason string `json:"reason,omitempty"`
	// Error is set when the collector of the section failed or timed out: the
	// other fields of the section are then missing.
	Error string `json:"error,omitempty"`
}

func (s *Section) header() *Section {
	return s
}

// BuildSection describes the SquirrelDB binary.
type BuildSection struct {
	Section

	Version       string    `json:"version"`
	Commit        string    `json:"commit"`
	Date          string    `json:"date"`
	GoVersion     string    `json:"go_version"`
	StartTime     time.Time `json:"start_time"`
	UptimeSeconds float64   `json:"uptime_seconds"`
	Uptime        string    `json:"uptime"`
}

// NodeSection describes this SquirrelDB.
type NodeSection struct {
	Section

	Hostname string `json:"hostname"`
	// TelemetryClusterID and TelemetryNodeID are empty until the telemetry
	// acquired them, and when the telemetry is disabled.
	TelemetryClusterID string `json:"telemetry_cluster_id,omitempty"`
	TelemetryNodeID    string `json:"telemetry_node_id,omitempty"`
	ListenAddress      string `json:"listen_address"`
	// AdminListenAddress is empty when the admin endpoints are on ListenAddress.
	AdminListenAddress string `json:"admin_listen_address,omitempty"`
	Ready              bool   `json:"ready"`
}

// BackendsSection summarizes the storage backends.
type BackendsSection struct {
	Section

	Primary *Backend `json:"primary"`
	// Extra is the best-effort dual-write target. Its Configured field is false
	// when there is none.
	Extra *Backend `json:"extra"`
}

// Backend summarizes one storage backend. Its details are in the section named
// after the backend (e.g. "clickhouse").
type Backend struct {
	Name       string `json:"name,omitempty"`
	Configured bool   `json:"configured"`
	// Started tells whether the backend is started; for the extra backend, whether
	// it is attached (receives the dual-writes).
	Started bool `json:"started"`
	// UnavailableSince is when the extra backend was first found unavailable at
	// startup. Points written since then up to its attachment were not dual-written.
	UnavailableSince *time.Time         `json:"unavailable_since,omitempty"`
	Info             *types.BackendInfo `json:"info,omitempty"`
	Status           string             `json:"status"`
	Reason           string             `json:"reason,omitempty"`
}

// ComponentSection is the health of a component reported by its StatusProvider.
type ComponentSection struct {
	Section

	// Details are component-specific, see docs/admin-status.md.
	Details any `json:"details,omitempty"`
}

// ClusterSection describes the SquirrelDB cluster.
type ClusterSection struct {
	Section

	// Kind is "redis" (nodes coordinate through Redis) or "local" (single node).
	Kind string `json:"kind"`
	// Size is the number of SquirrelDB nodes seen alive, including this one.
	Size    int `json:"size"`
	Details any `json:"details,omitempty"`
}

// SecuritySection describes the TLS and authentication of the API.
type SecuritySection struct {
	Section

	// Notes are informational remarks that don't change the status (e.g.
	// authentication disabled).
	Notes              []string  `json:"notes,omitempty"`
	AuthEnabled        bool      `json:"auth_enabled"`
	MetricsRequireAuth bool      `json:"metrics_require_auth"`
	TLS                TLSStatus `json:"tls"`
	Users              []User    `json:"users"`
	// AuthFailures are the requests rejected since the start, by reason. Nil when
	// the authentication is disabled.
	AuthFailures map[string]float64 `json:"auth_failures"`
}

// TLSStatus describes the TLS of the listeners.
type TLSStatus struct {
	Enabled        bool   `json:"enabled"`
	ClientAuthType string `json:"client_auth_type,omitempty"`
	ClientCA       bool   `json:"client_ca"`
	MinVersion     string `json:"min_version,omitempty"`
	// Certificate is the server certificate, read from cert_file.
	Certificate      *Certificate `json:"certificate,omitempty"`
	CertificateError string       `json:"certificate_error,omitempty"`
}

// Certificate describes a certificate, for expiry visibility.
type Certificate struct {
	Subject       string    `json:"subject"`
	DNSNames      []string  `json:"dns_names,omitempty"`
	NotBefore     time.Time `json:"not_before"`
	NotAfter      time.Time `json:"not_after"`
	ExpiresInDays float64   `json:"expires_in_days"`
}

// User is a configured user, without its credentials.
type User struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
	Tenant string   `json:"tenant,omitempty"`
	// Methods are the authentication methods of the user: basic and/or bearer.
	Methods          []string `json:"methods"`
	HasForcedMatcher bool     `json:"has_forced_matcher"`
}

// ConfigSection is the effective configuration with its secrets redacted.
type ConfigSection struct {
	Section

	Values any `json:"values"`
}

// Collect builds a report. It returns after at most about the section timeout,
// whatever the collectors do.
func (c *Collector) Collect(ctx context.Context) *Report {
	report := &Report{GeneratedAt: time.Now()}
	timeout := c.opts.SectionTimeout

	var wg sync.WaitGroup

	wg.Go(func() { report.Build = runSection(ctx, timeout, c.collectBuild) })
	wg.Go(func() { report.Node = runSection(ctx, timeout, c.collectNode) })
	wg.Go(func() { report.Cluster = runSection(ctx, timeout, c.collectCluster) })
	wg.Go(func() { report.Security = runSection(ctx, timeout, c.collectSecurity) })
	wg.Go(func() { report.Config = runSection(ctx, timeout, c.collectConfig) })
	wg.Go(func() { c.collectBackends(ctx, report) })

	wg.Wait()

	report.Status = types.StatusOK

	for _, s := range []struct {
		name    string
		section *Section
	}{
		{"node", &report.Node.Section},
		{"backends", &report.Backends.Section},
		{"cluster", &report.Cluster.Section},
		{"security", &report.Security.Section},
		// The build and the config are only reported when their collector failed.
		{"build", errorOnly(&report.Build.Section)},
		{"config", errorOnly(&report.Config.Section)},
	} {
		if s.section == nil || s.section.Status == types.StatusOK {
			continue
		}

		report.Status = types.WorstStatus(report.Status, s.section.Status)

		reason := s.section.Reason
		if s.section.Error != "" {
			reason = s.section.Error
		}

		report.Reasons = append(report.Reasons, s.name+": "+reason)
	}

	return report
}

func errorOnly(s *Section) *Section {
	if s.Error == "" {
		return nil
	}

	return s
}

// sectionPointer is a pointer to a section struct (which embeds Section).
type sectionPointer[T any] interface {
	*T
	header() *Section
}

// runSection runs a collector with a timeout. A collector that times out or
// panics yields a section with the unknown status and an error. A section
// without status is ok.
func runSection[T any, P sectionPointer[T]](
	ctx context.Context,
	timeout time.Duration,
	collect func(ctx context.Context) P,
) P {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Buffered so a collector returning after the timeout doesn't block forever.
	result := make(chan P, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				result <- failedSection[T, P](fmt.Sprintf("collector failed: %v", r))
			}
		}()

		result <- collect(ctx)
	}()

	select {
	case section := <-result:
		if section == nil {
			return failedSection[T, P]("collector returned no section")
		}

		if section.header().Status == "" {
			section.header().Status = types.StatusOK
		}

		return section
	case <-ctx.Done():
		return failedSection[T, P](fmt.Sprintf("collector did not answer within %s: %v", timeout, ctx.Err()))
	}
}

func failedSection[T any, P sectionPointer[T]](msg string) P {
	section := P(new(T))
	section.header().Status = types.StatusUnknown
	section.header().Error = msg

	return section
}

func (c *Collector) collectBuild(context.Context) *BuildSection {
	uptime := time.Since(c.opts.StartTime)

	return &BuildSection{
		Version:       version.Version,
		Commit:        version.Commit,
		Date:          version.Date,
		GoVersion:     runtime.Version(),
		StartTime:     c.opts.StartTime,
		UptimeSeconds: uptime.Seconds(),
		Uptime:        uptime.Truncate(time.Second).String(),
	}
}

func (c *Collector) collectNode(context.Context) *NodeSection {
	section := &NodeSection{
		ListenAddress:      c.opts.ListenAddress,
		AdminListenAddress: c.opts.AdminListenAddress,
	}

	if hostname, err := os.Hostname(); err == nil {
		section.Hostname = hostname
	}

	if c.opts.Telemetry != nil {
		section.TelemetryClusterID, section.TelemetryNodeID = c.opts.Telemetry()
	}

	section.Ready = c.opts.Ready != nil && c.opts.Ready()
	if !section.Ready {
		section.Status = types.StatusDown
		section.Reason = "the API is not ready"
	}

	return section
}

func (c *Collector) collectCluster(ctx context.Context) *ClusterSection {
	var cluster types.Cluster

	if c.opts.Cluster != nil {
		cluster = c.opts.Cluster()
	}

	if cluster == nil {
		return &ClusterSection{Status: types.StatusUnknown, Reason: "cluster not started"}
	}

	section := &ClusterSection{Kind: "local", Size: cluster.Size()}

	provider, ok := cluster.(types.StatusProvider)
	if !ok {
		return section
	}

	section.Kind = "redis"

	status := provider.Status(ctx)
	section.Status, section.Reason, section.Details = status.Status, status.Reason, status.Details

	return section
}

func (c *Collector) collectSecurity(context.Context) *SecuritySection {
	section := &SecuritySection{
		AuthEnabled:        c.opts.Auth.Enabled,
		MetricsRequireAuth: c.opts.Auth.MetricsRequireAuth,
		Users:              make([]User, 0, len(c.opts.Auth.Users)),
	}

	if c.opts.Auth.Enabled {
		for _, u := range c.opts.Auth.Users {
			section.Users = append(section.Users, publicUser(u))
		}

		if c.opts.AuthFailures != nil {
			section.AuthFailures = c.opts.AuthFailures()
		}
	} else {
		section.Notes = append(section.Notes, "authentication is disabled")
	}

	section.TLS = c.tlsStatus(time.Now(), &section.Section)

	if !section.TLS.Enabled {
		section.Notes = append(section.Notes, "TLS is disabled")
	}

	return section
}

// publicUser returns what can be shown of a user: never its password or token hash.
func publicUser(u auth.User) User {
	methods := make([]string, 0, 2)

	if u.PasswordHash != "" {
		methods = append(methods, "basic")
	}

	if u.TokenSHA256 != "" {
		methods = append(methods, "bearer")
	}

	return User{
		Name:             u.Name,
		Scopes:           u.Scopes,
		Tenant:           u.Tenant,
		Methods:          methods,
		HasForcedMatcher: u.ForcedMatcher != "",
	}
}

// tlsStatus describes the TLS configuration and degrades section when the server
// certificate is unreadable, expired or about to expire.
func (c *Collector) tlsStatus(now time.Time, section *Section) TLSStatus {
	opts := c.opts.TLS
	st := TLSStatus{
		Enabled:        opts.Enabled(),
		ClientAuthType: opts.ClientAuthType,
		ClientCA:       opts.ClientCAFile != "",
		MinVersion:     opts.MinVersion,
	}

	if !st.Enabled {
		return st
	}

	cert, err := opts.ServerCertificate()
	if err != nil {
		st.CertificateError = err.Error()
		section.Status = types.StatusDegraded
		section.Reason = "cannot read the TLS certificate: " + err.Error()

		return st
	}

	st.Certificate = &Certificate{
		Subject:       cert.Subject.String(),
		DNSNames:      cert.DNSNames,
		NotBefore:     cert.NotBefore,
		NotAfter:      cert.NotAfter,
		ExpiresInDays: cert.NotAfter.Sub(now).Hours() / 24,
	}

	switch {
	case now.After(cert.NotAfter):
		section.Status = types.StatusDown
		section.Reason = "the TLS certificate expired on " + cert.NotAfter.Format(time.RFC3339)
	case cert.NotAfter.Sub(now) < certificateExpiryWarning:
		section.Status = types.StatusDegraded
		section.Reason = "the TLS certificate expires on " + cert.NotAfter.Format(time.RFC3339)
	}

	return st
}

func (c *Collector) collectConfig(context.Context) *ConfigSection {
	if c.opts.Config == nil {
		return &ConfigSection{Status: types.StatusUnknown, Reason: "configuration not available"}
	}

	return &ConfigSection{Values: Redact(c.opts.Config)}
}

// collectBackends fills the backends section and the per-backend sections. Each
// backend's StatusProvider runs concurrently with its own timeout.
func (c *Collector) collectBackends(ctx context.Context, report *Report) {
	section := &BackendsSection{Extra: &Backend{Configured: false, Status: types.StatusOK}}
	report.Backends = section

	if c.opts.Backends == nil {
		section.Status = types.StatusUnknown
		section.Reason = "backends not available"

		return
	}

	states := c.opts.Backends()
	components := make([]*ComponentSection, len(states))

	var wg sync.WaitGroup

	for i, state := range states {
		wg.Go(func() {
			components[i] = runSection(ctx, c.opts.SectionTimeout, func(ctx context.Context) *ComponentSection {
				return backendComponent(ctx, state)
			})
		})
	}

	wg.Wait()

	for i, state := range states {
		component := components[i]
		summary := &Backend{
			Name:       state.Name,
			Configured: true,
			Started:    state.Started,
			Status:     component.Status,
			Reason:     component.Reason,
		}

		if component.Error != "" {
			summary.Reason = component.Error
		}

		if state.Started {
			info := state.Info
			summary.Info = &info
		}

		if !state.UnavailableSince.IsZero() {
			since := state.UnavailableSince
			summary.UnavailableSince = &since
		}

		switch state.Role {
		case types.BackendRoleExtra:
			section.Extra = summary
		default:
			section.Primary = summary
		}

		switch state.Name {
		case "clickhouse":
			report.ClickHouse = component
		case "cassandra":
			report.Cassandra = component
		case "dummy":
			report.Dummy = component
		}
	}

	section.Status, section.Reason = backendsHealth(section.Primary, section.Extra)
}

// backendComponent returns the health of a backend.
func backendComponent(ctx context.Context, state types.BackendState) *ComponentSection {
	if !state.Started {
		if state.Role == types.BackendRoleExtra {
			reason := "the dual-write target is not attached: points are written to the primary only"
			if !state.UnavailableSince.IsZero() {
				reason += ", repair from " + state.UnavailableSince.Format(time.RFC3339)
			}

			return &ComponentSection{Status: types.StatusDown, Reason: reason}
		}

		return &ComponentSection{Status: types.StatusDown, Reason: "backend not started"}
	}

	provider, ok := state.Backend.(types.StatusProvider)
	if !ok {
		return &ComponentSection{
			Status: types.StatusUnknown, Reason: "the backend does not report its status",
		}
	}

	status := provider.Status(ctx)

	return &ComponentSection{
		Status: status.Status, Reason: status.Reason,
		Details: status.Details,
	}
}

// backendsHealth returns the health of the backends: the primary one's, and at
// most degraded because of the extra backend (it is best-effort, a problem on it
// never affects the primary).
func backendsHealth(primary *Backend, extra *Backend) (string, string) {
	if primary == nil {
		return types.StatusUnknown, "no primary backend"
	}

	status, reason := primary.Status, primary.Reason
	if status != types.StatusOK {
		reason = "primary " + primary.Name + ": " + reason
	}

	if extra == nil || !extra.Configured || extra.Status == types.StatusOK {
		return status, reason
	}

	extraStatus := types.StatusDegraded
	if extra.Status == types.StatusUnknown {
		extraStatus = types.StatusUnknown
	}

	extraReason := "extra " + extra.Name + ": " + extra.Reason

	if status == types.StatusOK {
		return extraStatus, extraReason
	}

	return types.WorstStatus(status, extraStatus), reason + "; " + extraReason
}

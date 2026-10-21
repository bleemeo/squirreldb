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

package config

import (
	"testing"

	"github.com/bleemeo/squirreldb/api/auth/authtest"

	"golang.org/x/crypto/bcrypt"
)

func TestValidateConfigExtraWriteBackends(t *testing.T) {
	t.Parallel()

	base := DefaultConfig()

	cases := []validateConfigCase{
		{
			name:    "no extra_write_backend is fine",
			mutate:  func(*Config) {},
			wantErr: false,
		},
		{
			name: "dual write: clickhouse extra alongside cassandra",
			mutate: func(c *Config) {
				c.Backend = "cassandra"
				c.ExtraWriteBackend = "clickhouse"
			},
			wantErr: false,
		},
		{
			name: "extra equal to backend is rejected",
			mutate: func(c *Config) {
				c.Backend = "cassandra"
				c.ExtraWriteBackend = "cassandra"
			},
			wantErr: true,
		},
		{
			name: "sandbox backend is fine",
			mutate: func(c *Config) {
				c.Backend = BackendSandbox
			},
			wantErr: false,
		},
		{
			name: "sandbox can't be an extra backend",
			mutate: func(c *Config) {
				c.Backend = "cassandra"
				c.ExtraWriteBackend = BackendSandbox
			},
			wantErr: true,
		},
		{
			name: "unknown extra backend name",
			mutate: func(c *Config) {
				c.Backend = "cassandra"
				c.ExtraWriteBackend = "postgres"
			},
			wantErr: true,
		},
	}

	runValidateConfigCases(t, base, cases)
}

func TestValidateConfigAdminListenAddress(t *testing.T) {
	t.Parallel()

	base := DefaultConfig()

	cases := []validateConfigCase{
		{
			name:    "no admin_listen_address is fine",
			mutate:  func(*Config) {},
			wantErr: false,
		},
		{
			name: "admin_listen_address different from listen_address",
			mutate: func(c *Config) {
				c.ListenAddress = "localhost:9201"
				c.AdminListenAddress = "localhost:9202"
			},
			wantErr: false,
		},
		{
			name: "admin_listen_address equal to listen_address is rejected",
			mutate: func(c *Config) {
				c.ListenAddress = "localhost:9201"
				c.AdminListenAddress = "localhost:9201"
			},
			wantErr: true,
		},
	}

	runValidateConfigCases(t, base, cases)
}

type validateConfigCase struct {
	name    string
	mutate  func(*Config)
	wantErr bool
}

func runValidateConfigCases(t *testing.T, base Config, cases []validateConfigCase) {
	t.Helper()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := base
			tc.mutate(&cfg)

			err := validateConfig(cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateConfigTLS(t *testing.T) {
	t.Parallel()

	certs := authtest.WriteCertificates(t, t.TempDir())
	base := DefaultConfig()

	cases := []validateConfigCase{
		{
			name:    "TLS disabled by default",
			mutate:  func(*Config) {},
			wantErr: false,
		},
		{
			name: "valid certificate",
			mutate: func(c *Config) {
				c.TLS = TLS{CertFile: certs.ServerCertFile, KeyFile: certs.ServerKeyFile, MinVersion: "TLS13"}
			},
			wantErr: false,
		},
		{
			name: "valid mTLS",
			mutate: func(c *Config) {
				c.TLS = TLS{
					CertFile:       certs.ServerCertFile,
					KeyFile:        certs.ServerKeyFile,
					ClientCAFile:   certs.CAFile,
					ClientAuthType: "RequireAndVerifyClientCert",
				}
			},
			wantErr: false,
		},
		{
			name:    "missing certificate file",
			mutate:  func(c *Config) { c.TLS = TLS{CertFile: "/does/not/exist.pem", KeyFile: certs.ServerKeyFile} },
			wantErr: true,
		},
		{
			name:    "key without certificate",
			mutate:  func(c *Config) { c.TLS = TLS{KeyFile: certs.ServerKeyFile} },
			wantErr: true,
		},
		{
			name:    "key not matching the certificate",
			mutate:  func(c *Config) { c.TLS = TLS{CertFile: certs.ServerCertFile, KeyFile: certs.OtherKeyFile} },
			wantErr: true,
		},
		{
			name: "invalid min_version",
			mutate: func(c *Config) {
				c.TLS = TLS{CertFile: certs.ServerCertFile, KeyFile: certs.ServerKeyFile, MinVersion: "1.2"}
			},
			wantErr: true,
		},
	}

	runValidateConfigCases(t, base, cases)
}

func TestValidateConfigAuth(t *testing.T) {
	t.Parallel()

	hash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	reader := AuthUser{Name: "reader", PasswordHash: string(hash), Scopes: []string{"read"}}
	writer := AuthUser{
		Name:        "glouton-acme",
		TokenSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Tenant:      "acme",
		Scopes:      []string{"write"},
	}
	invalid := AuthUser{Name: "invalid", PasswordHash: "clear text", Scopes: []string{"read"}}

	base := DefaultConfig()

	cases := []validateConfigCase{
		{
			name:    "auth disabled by default",
			mutate:  func(*Config) {},
			wantErr: false,
		},
		{
			name:    "users are not checked when auth is disabled",
			mutate:  func(c *Config) { c.Auth.Users = []AuthUser{invalid} },
			wantErr: false,
		},
		{
			name:    "valid users",
			mutate:  func(c *Config) { c.Auth = Auth{Enabled: true, Users: []AuthUser{reader, writer}} },
			wantErr: false,
		},
		{
			name:    "enabled without user",
			mutate:  func(c *Config) { c.Auth = Auth{Enabled: true} },
			wantErr: true,
		},
		{
			name:    "invalid user",
			mutate:  func(c *Config) { c.Auth = Auth{Enabled: true, Users: []AuthUser{reader, invalid}} },
			wantErr: true,
		},
		{
			name: "tenant without tenant label",
			mutate: func(c *Config) {
				c.Auth = Auth{Enabled: true, Users: []AuthUser{writer}}
				c.TenantLabelName = ""
			},
			wantErr: true,
		},
	}

	runValidateConfigCases(t, base, cases)
}

func TestValidateConfigAuthUserBackends(t *testing.T) {
	t.Parallel()

	demo := func(backends ...string) AuthUser {
		return AuthUser{
			Name:        "demo",
			TokenSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Scopes:      []string{"read"},
			Backends:    backends,
		}
	}

	base := DefaultConfig()
	base.Backend = BackendClickHouse
	base.Routing = Routing{Backends: []string{BackendSandbox}}

	withUser := func(u AuthUser) func(*Config) {
		return func(c *Config) { c.Auth = Auth{Enabled: true, Users: []AuthUser{u}} }
	}

	cases := []validateConfigCase{
		{name: "no backends", mutate: withUser(demo()), wantErr: false},
		{name: "routed backend", mutate: withUser(demo(BackendSandbox)), wantErr: false},
		{name: "default backend", mutate: withUser(demo(BackendClickHouse)), wantErr: false},
		{name: "default and routed backends", mutate: withUser(demo(BackendSandbox, BackendClickHouse)), wantErr: false},
		{
			name: "default backend without routing",
			mutate: func(c *Config) {
				withUser(demo(BackendClickHouse))(c)
				c.Routing = Routing{}
			},
			wantErr: false,
		},
		{name: "unknown backend", mutate: withUser(demo("postgres")), wantErr: true},
		{name: "known but not selectable backend", mutate: withUser(demo(BackendCassandra)), wantErr: true},
		{
			name: "extra write backend is not selectable",
			mutate: func(c *Config) {
				withUser(demo(BackendCassandra))(c)
				c.ExtraWriteBackend = BackendCassandra
			},
			wantErr: true,
		},
		{name: "backend listed twice", mutate: withUser(demo(BackendSandbox, BackendSandbox)), wantErr: true},
		{
			name: "admin scope with backends",
			mutate: func(c *Config) {
				u := demo(BackendSandbox)
				u.Scopes = []string{"admin"}
				withUser(u)(c)
			},
			wantErr: true,
		},
		{
			name:    "not checked when auth is disabled",
			mutate:  func(c *Config) { c.Auth.Users = []AuthUser{demo("postgres")} },
			wantErr: false,
		},
	}

	runValidateConfigCases(t, base, cases)
}

func TestValidateConfigRouting(t *testing.T) {
	t.Parallel()

	base := DefaultConfig()
	base.Backend = BackendClickHouse

	cases := []struct {
		name    string
		routing Routing
		extra   string
		wantErr bool
	}{
		{
			name:    "empty routing is fine",
			routing: Routing{},
			wantErr: false,
		},
		{
			name: "routed backend and rules",
			routing: Routing{
				Backends: []string{BackendSandbox},
				Rules: []RoutingRule{
					{Tenants: []string{"demo-*", "1234"}, Backend: BackendSandbox},
					{Tenants: []string{"5678"}, Backend: BackendClickHouse},
				},
			},
			extra:   BackendCassandra,
			wantErr: false,
		},
		{
			name:    "rule to the default backend without routed backend",
			routing: Routing{Rules: []RoutingRule{{Tenants: []string{"*"}, Backend: BackendClickHouse}}},
			wantErr: false,
		},
		{
			name:    "unknown routed backend",
			routing: Routing{Backends: []string{"postgres"}},
			wantErr: true,
		},
		{
			name:    "default backend listed in routed backends",
			routing: Routing{Backends: []string{BackendClickHouse}},
			wantErr: true,
		},
		{
			name:    "extra write backend listed in routed backends",
			routing: Routing{Backends: []string{BackendCassandra}},
			extra:   BackendCassandra,
			wantErr: true,
		},
		{
			name:    "routed backend listed twice",
			routing: Routing{Backends: []string{BackendSandbox, BackendSandbox}},
			wantErr: true,
		},
		{
			name:    "rule backend not selectable",
			routing: Routing{Rules: []RoutingRule{{Tenants: []string{"1234"}, Backend: BackendSandbox}}},
			wantErr: true,
		},
		{
			name: "unknown rule backend",
			routing: Routing{
				Backends: []string{BackendSandbox},
				Rules:    []RoutingRule{{Tenants: []string{"1234"}, Backend: "postgres"}},
			},
			wantErr: true,
		},
		{
			name: "rule without tenant",
			routing: Routing{
				Backends: []string{BackendSandbox},
				Rules:    []RoutingRule{{Backend: BackendSandbox}},
			},
			wantErr: true,
		},
		{
			name: "rule with an invalid pattern",
			routing: Routing{
				Backends: []string{BackendSandbox},
				Rules:    []RoutingRule{{Tenants: []string{"demo-["}, Backend: BackendSandbox}},
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := base
			cfg.Routing = tc.routing
			cfg.ExtraWriteBackend = tc.extra

			err := validateConfig(cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

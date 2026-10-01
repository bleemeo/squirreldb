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

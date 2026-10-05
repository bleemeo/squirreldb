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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSecurityUnusedKeys checks that an unknown key is fatal in the security
// sections (auth, tls) and stays a warning elsewhere.
func TestSecurityUnusedKeys(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		env         map[string]string
		wantInError []string
		wantWarning string
	}{
		{
			name: "valid auth and tls",
			content: `
auth:
  enabled: true
  users:
    - name: reader
      token_sha256: "abc"
      tenant: acme
      scopes: [read]
tls:
  cert_file: /cert.pem
`,
		},
		{
			name: "typo in a user key",
			content: `
auth:
  enabled: true
  users:
    - name: ok
      scopes: [read]
    - name: reader
      tenat: acme
      scopes: [read]
`,
			wantInError: []string{"auth.users[1].tenat"},
		},
		{
			name: "typo with an invalid value outside the security sections",
			content: `
auth:
  users:
    - name: grafana-acme
      scopes: [read]
      tenat: acme
log:
  level: abc
`,
			wantInError: []string{"auth.users[0].tenat"},
		},
		{
			name: "invalid value in a security section",
			content: `
auth:
  enable: true
  users:
    - name: reader
      scopes: {read: true}
`,
			wantInError: []string{"auth.users[0].scopes"},
		},
		{
			name: "invalid value outside the security sections stays a warning",
			content: `
auth:
  enabled: true
log:
  level: abc
`,
			wantWarning: "log.level",
		},
		{
			name: "typo in an auth key",
			content: `
auth:
  enable: true
`,
			wantInError: []string{"auth.enable"},
		},
		{
			name: "typo in a tls key",
			content: `
tls:
  cert_file: /cert.pem
  client_auth_tpye: RequireAndVerifyClientCert
`,
			wantInError: []string{"tls.client_auth_tpye"},
		},
		{
			name: "several typos are all reported",
			content: `
auth:
  enable: true
  users:
    - name: reader
      scope: [read]
tls:
  certfile: /cert.pem
`,
			wantInError: []string{"auth.enable", "auth.users[0].scope", "tls.certfile"},
		},
		{
			name: "typo from an environment variable",
			env: map[string]string{
				"SQUIRRELDB_AUTH_ENABLE":   "true",
				"SQUIRRELDB_TLS_CERTFILE":  "/cert.pem",
				"SQUIRRELDB_AUTHENTICATED": "unrelated, not a security section",
			},
			content: `
auth:
  enabled: false
`,
			wantInError: []string{"SQUIRRELDB_AUTH_ENABLE", "SQUIRRELDB_TLS_CERTFILE"},
		},
		{
			name: "known environment variables of a security section",
			env: map[string]string{
				"SQUIRRELDB_AUTH_ENABLED":   "true",
				"SQUIRRELDB_TLS_CERT_FILE":  "/cert.pem",
				"SQUIRRELDB_CASSANDRA_FOOO": "unknown outside the security sections, ignored",
			},
		},
		{
			name: "unknown key outside the security sections is a warning",
			content: `
cassandra:
  keyspac: squirreldb
authentication:
  enabled: true
`,
			wantWarning: "invalid keys",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, value := range test.env {
				t.Setenv(key, value)
			}

			path := filepath.Join(t.TempDir(), "squirreldb.conf")
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}

			_, warnings, err := loadToStruct(false, nil, path)

			checkSecurityError(t, err, test.wantInError)

			if test.wantWarning != "" && !strings.Contains(errors.Join(warnings...).Error(), test.wantWarning) {
				t.Errorf("warnings %v don't contain %q", warnings, test.wantWarning)
			}
		})
	}
}

// checkSecurityError checks that err is nil when wantInError is empty, and otherwise
// an ErrInvalidValue mentioning every wanted key and no unrelated variable.
func checkSecurityError(t *testing.T, err error, wantInError []string) {
	t.Helper()

	if len(wantInError) == 0 {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		return
	}

	if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("error = %v, want an ErrInvalidValue", err)
	}

	for _, key := range wantInError {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q doesn't mention %q", err, key)
		}
	}

	if strings.Contains(err.Error(), "SQUIRRELDB_AUTHENTICATED") {
		t.Errorf("error %q mentions a variable outside the security sections", err)
	}
}

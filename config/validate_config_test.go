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

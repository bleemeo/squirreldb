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

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
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

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
	"fmt"
	"slices"

	"github.com/bleemeo/squirreldb/api/auth"
)

// TLSOptions converts the TLS configuration to the options of the API.
func (t TLS) TLSOptions() auth.TLSOptions {
	return auth.TLSOptions{
		CertFile:       t.CertFile,
		KeyFile:        t.KeyFile,
		ClientCAFile:   t.ClientCAFile,
		ClientAuthType: t.ClientAuthType,
		MinVersion:     t.MinVersion,
	}
}

// AuthUsers converts the users to the users of the API.
func (a Auth) AuthUsers() []auth.User {
	users := make([]auth.User, 0, len(a.Users))

	for _, u := range a.Users {
		users = append(users, auth.User{
			Name:          u.Name,
			PasswordHash:  u.PasswordHash,
			TokenSHA256:   u.TokenSHA256,
			Tenant:        u.Tenant,
			Scopes:        u.Scopes,
			ForcedMatcher: u.ForcedMatcher,
			Backends:      u.Backends,
		})
	}

	return users
}

// validateSecurity checks the tls and auth sections. The TLS files are read to
// check that they exist and that the certificate and key match.
func validateSecurity(cfg Config) []error {
	var errs []error

	if _, err := auth.NewTLSConfig(cfg.TLS.TLSOptions()); err != nil {
		errs = append(errs, fmt.Errorf("%w: 'tls': %w", ErrInvalidValue, err))
	}

	if !cfg.Auth.Enabled {
		return errs
	}

	if len(cfg.Auth.Users) == 0 {
		errs = append(errs, fmt.Errorf(
			"%w: 'auth.enabled' is true but 'auth.users' is empty, no request could be authenticated",
			ErrInvalidValue,
		))
	}

	if err := auth.ValidateUsers(cfg.Auth.AuthUsers()); err != nil {
		errs = append(errs, fmt.Errorf("%w: 'auth.users': %w", ErrInvalidValue, err))
	}

	for _, u := range cfg.Auth.Users {
		if u.Tenant != "" && cfg.TenantLabelName == "" {
			errs = append(errs, fmt.Errorf(
				"%w: user %q has a tenant but 'tenant_label_name' is empty, the tenant would have no effect",
				ErrInvalidValue, u.Name,
			))
		}

		errs = append(errs, validateUserBackends(cfg, u)...)
	}

	return errs
}

// validateUserBackends checks the backends of a user: each one must be selectable,
// i.e. the default backend or one of 'routing.backends'.
func validateUserBackends(cfg Config, u AuthUser) []error {
	var errs []error

	seen := make(map[string]bool, len(u.Backends))

	for _, name := range u.Backends {
		switch {
		case !isKnownBackend(name):
			errs = append(errs, fmt.Errorf(
				"%w: unknown backend %q in the backends of user %q", ErrInvalidValue, name, u.Name,
			))
		case name != cfg.Backend && !slices.Contains(cfg.Routing.Backends, name):
			errs = append(errs, fmt.Errorf(
				"%w: backend %q of user %q must be the default backend or listed in 'routing.backends'",
				ErrInvalidValue, name, u.Name,
			))
		case seen[name]:
			errs = append(errs, fmt.Errorf(
				"%w: backend %q is listed twice in the backends of user %q", ErrInvalidValue, name, u.Name,
			))
		}

		seen[name] = true
	}

	return errs
}

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

// Package auth implements the optional TLS and authentication of the SquirrelDB HTTP API:
// users authenticated by basic auth (bcrypt password) or bearer token (sha256), the scopes
// (read, write, admin) required by each route, and the per-user tenant and forced matchers
// applied to the request.
package auth

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/crypto/bcrypt"
)

// Scope is a permission granted to a user.
type Scope string

const (
	// ScopeRead allows the Prometheus read API (PromQL, series, labels, remote read, ...).
	ScopeRead Scope = "read"
	// ScopeWrite allows the remote write and OTLP write endpoints.
	ScopeWrite Scope = "write"
	// ScopeAdmin allows the admin endpoints (/debug/, /mutable/) and any route not
	// explicitly classified as read or write.
	ScopeAdmin Scope = "admin"

	// scopePublic is the requirement of routes that never need authentication.
	scopePublic Scope = ""
	// scopeMetrics is the requirement of /metrics: public unless MetricsRequireAuth
	// is set, any authenticated user otherwise.
	scopeMetrics Scope = "metrics"
)

var (
	ErrInvalidUser          = errors.New("invalid auth user")
	ErrInvalidForcedMatcher = errors.New("invalid forced matcher")
)

// User is a configured user.
type User struct {
	// Name is the basic auth username. It identifies the user in logs.
	Name string
	// PasswordHash is the bcrypt hash of the basic auth password. Empty disables basic auth for this user.
	PasswordHash string
	// TokenSHA256 is the hex encoded sha256 of the bearer token. Empty disables bearer auth for this user.
	TokenSHA256 string
	// Tenant, when set, binds the user to this tenant: its requests are processed as if
	// they had the tenant header with this value, and a different tenant header is rejected.
	Tenant string
	// Scopes are the permissions granted to the user.
	Scopes []string
	// ForcedMatcher restricts the series the user can read, combined (AND) with the
	// forced matcher header. See ParseForcedMatcher for the format.
	ForcedMatcher string
	// Backends, when set, are the only backends the user may use (see
	// docs/routing.md); the first one replaces the default backend when the
	// default backend isn't listed. Empty means every enabled backend.
	Backends []string
}

// Identity is an authenticated user, stored in the request context.
type Identity struct {
	Name   string
	Tenant string
	Scopes []Scope
	// ForcedMatchers are added (AND) to the matchers of every read request of this user.
	ForcedMatchers []*labels.Matcher
	// Backends are the only backends the user may use, empty meaning no restriction.
	Backends []string
}

// HasScope returns whether the identity was granted the scope.
func (id *Identity) HasScope(scope Scope) bool {
	return slices.Contains(id.Scopes, scope)
}

type contextKey struct{}

// ContextWithIdentity returns a context carrying the identity.
func ContextWithIdentity(ctx context.Context, identity *Identity) context.Context {
	return context.WithValue(ctx, contextKey{}, identity)
}

// IdentityFromContext returns the authenticated identity of the request, or nil when
// the request wasn't authenticated (authentication disabled or public route).
func IdentityFromContext(ctx context.Context) *Identity {
	identity, _ := ctx.Value(contextKey{}).(*Identity)

	return identity
}

// ParseForcedMatcher parses the forced matcher of a user. It accepts PromQL label
// matchers, with or without braces (`env="prod",team=~"a|b"`), or the format of the
// X-SquirrelDB-Forced-Matcher header (`env=prod`, a single equality matcher).
// An empty string means no matcher.
func ParseForcedMatcher(value string) ([]*labels.Matcher, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}

	selector := value
	if !strings.HasPrefix(selector, "{") {
		selector = "{" + selector + "}"
	}

	matchers, err := parser.NewParser(parser.Options{}).ParseMetricSelector(selector)
	if err == nil {
		return matchers, nil
	}

	// Fallback to the header format, whose value is not quoted.
	name, labelValue, found := strings.Cut(value, "=")
	if found && strings.TrimSpace(name) != "" && !strings.ContainsAny(value, "{}\"'`,~!") {
		matcher, matcherErr := labels.NewMatcher(labels.MatchEqual, strings.TrimSpace(name), labelValue)
		if matcherErr == nil {
			return []*labels.Matcher{matcher}, nil
		}
	}

	return nil, fmt.Errorf("%w %q: %w", ErrInvalidForcedMatcher, value, err)
}

// ValidateUsers checks that the users are valid. It doesn't check the bcrypt
// passwords themselves, only that the hashes are well formed.
func ValidateUsers(users []User) error {
	errs := make([]error, 0, len(users))
	seen := make(map[string]bool, len(users))

	for i, user := range users {
		errs = append(errs, validateUser(user, seen, i))
	}

	return errors.Join(errs...)
}

func validateUser(user User, seen map[string]bool, index int) error {
	if user.Name == "" {
		return fmt.Errorf("%w: user #%d has no name", ErrInvalidUser, index+1)
	}

	var errs []error

	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("%w %q: "+format, append([]any{ErrInvalidUser, user.Name}, args...)...))
	}

	if seen[user.Name] {
		fail("duplicate user name")
	}

	seen[user.Name] = true

	if user.PasswordHash == "" && user.TokenSHA256 == "" {
		fail("one of password_hash or token_sha256 must be set")
	}

	if user.PasswordHash != "" {
		if _, err := bcrypt.Cost([]byte(user.PasswordHash)); err != nil {
			fail("password_hash is not a valid bcrypt hash (generate one with 'squirreldb auth hash-password')")
		}
	}

	if user.TokenSHA256 != "" {
		if _, err := decodeTokenHash(user.TokenSHA256); err != nil {
			fail("token_sha256 must be 64 hexadecimal characters (generate one with 'squirreldb auth gen-token')")
		}
	}

	if len(user.Scopes) == 0 {
		fail("at least one scope must be set (read, write or admin)")
	}

	for _, scope := range user.Scopes {
		switch scope {
		case string(ScopeRead), string(ScopeWrite):
		case string(ScopeAdmin):
			// The admin endpoints are not tenant scoped (e.g. index dumps list every tenant)
			// nor routed (e.g. the debug endpoints of every backend), granting them to a
			// restricted user would bypass its restriction.
			if user.Tenant != "" || user.ForcedMatcher != "" || len(user.Backends) > 0 {
				fail("the admin scope can't be granted to a user with a tenant, a forced_matcher or backends")
			}
		default:
			fail("unknown scope %q (expected read, write or admin)", scope)
		}
	}

	if _, err := ParseForcedMatcher(user.ForcedMatcher); err != nil {
		fail("%v", err)
	}

	return errors.Join(errs...)
}

func decodeTokenHash(value string) ([]byte, error) {
	decoded, err := hex.DecodeString(strings.ToLower(value))
	if err != nil {
		return nil, err
	}

	if len(decoded) != 32 {
		return nil, fmt.Errorf("%w: token_sha256 has %d bytes, want 32", ErrInvalidUser, len(decoded))
	}

	return decoded, nil
}

func newIdentity(user User) (*Identity, error) {
	matchers, err := ParseForcedMatcher(user.ForcedMatcher)
	if err != nil {
		return nil, err
	}

	scopes := make([]Scope, 0, len(user.Scopes))
	for _, scope := range user.Scopes {
		scopes = append(scopes, Scope(scope))
	}

	return &Identity{
		Name:           user.Name,
		Tenant:         user.Tenant,
		Scopes:         scopes,
		ForcedMatchers: matchers,
		Backends:       slices.Clone(user.Backends),
	}, nil
}

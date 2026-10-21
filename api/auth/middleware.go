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

package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"
)

// Reasons of the authentication failures, used as label of the failures metric.
//
//nolint:gosec // These are metric label values, not credentials.
const (
	reasonMissingCredentials   = "missing_credentials"
	reasonMalformedCredentials = "malformed_credentials"
	reasonInvalidCredentials   = "invalid_credentials"
	reasonMissingScope         = "missing_scope"
	reasonTenantMismatch       = "tenant_mismatch"
)

const wwwAuthenticate = `Basic realm="SquirrelDB"`

var ErrNoUser = errors.New("authentication is enabled but no user is configured")

// Options configures the authentication.
type Options struct {
	// Enabled enables the authentication. When false, Middleware doesn't change the handler.
	Enabled bool
	// MetricsRequireAuth makes /metrics require an authenticated user (with any scope).
	MetricsRequireAuth bool
	Users              []User
	MetricRegistry     prometheus.Registerer
	Logger             zerolog.Logger
}

// Authenticator authenticates the requests and checks they have the scope required by their route.
type Authenticator struct {
	enabled            bool
	metricsRequireAuth bool
	users              []*user
	usersByName        map[string]*user
	// dummyHash is compared to the password of unknown users, so the response time
	// doesn't tell whether a user exists.
	dummyHash []byte
	cache     *verifyCache
	// compareHash is bcrypt.CompareHashAndPassword, replaced in tests.
	compareHash func(hash, password []byte) error
	failures    *prometheus.CounterVec
	logger      zerolog.Logger
}

type user struct {
	passwordHash []byte
	tokenHash    []byte
	identity     *Identity
}

// New returns an Authenticator. The users are validated (see ValidateUsers).
func New(opts Options) (*Authenticator, error) {
	a := &Authenticator{
		enabled:            opts.Enabled,
		metricsRequireAuth: opts.MetricsRequireAuth,
		usersByName:        make(map[string]*user, len(opts.Users)),
		cache:              newVerifyCache(),
		compareHash:        bcrypt.CompareHashAndPassword,
		logger:             opts.Logger,
	}

	if !opts.Enabled {
		return a, nil
	}

	if len(opts.Users) == 0 {
		return nil, ErrNoUser
	}

	if err := ValidateUsers(opts.Users); err != nil {
		return nil, err
	}

	dummyCost := bcrypt.DefaultCost

	for _, u := range opts.Users {
		identity, err := newIdentity(u)
		if err != nil {
			return nil, err
		}

		compiled := &user{identity: identity}

		if u.PasswordHash != "" {
			compiled.passwordHash = []byte(u.PasswordHash)

			if cost, err := bcrypt.Cost(compiled.passwordHash); err == nil {
				dummyCost = cost
			}
		}

		if u.TokenSHA256 != "" {
			compiled.tokenHash, _ = decodeTokenHash(u.TokenSHA256) // Already validated.
		}

		a.users = append(a.users, compiled)
		a.usersByName[u.Name] = compiled
	}

	dummyHash, err := bcrypt.GenerateFromPassword([]byte(rand.Text()), dummyCost)
	if err != nil {
		return nil, fmt.Errorf("generate dummy hash: %w", err)
	}

	a.dummyHash = dummyHash

	a.failures = promauto.With(opts.MetricRegistry).NewCounterVec(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "api",
		Name:      "auth_failures_total",
		Help:      "Total number of requests rejected by the authentication, by reason",
	}, []string{"reason"})

	for _, reason := range []string{
		reasonMissingCredentials,
		reasonMalformedCredentials,
		reasonInvalidCredentials,
		reasonMissingScope,
		reasonTenantMismatch,
	} {
		a.failures.WithLabelValues(reason)
	}

	return a, nil
}

// Enabled returns whether the authentication is enabled.
func (a *Authenticator) Enabled() bool {
	return a.enabled
}

// Middleware returns a handler that authenticates the requests, checks they have the
// scope classify returns for them, and adds the authenticated Identity to their context.
// When the authentication is disabled, next is returned unchanged.
func (a *Authenticator) Middleware(classify RouteClassifier, next http.Handler) http.Handler {
	if !a.enabled {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		required := classify(r)

		if required == scopePublic || (required == scopeMetrics && !a.metricsRequireAuth) {
			next.ServeHTTP(w, r)

			return
		}

		identity, reason := a.authenticate(r)
		if identity == nil {
			a.reject(w, r, http.StatusUnauthorized, reason, "")

			return
		}

		if required != scopeMetrics && !identity.HasScope(required) {
			a.reject(w, r, http.StatusForbidden, reasonMissingScope, identity.Name)

			return
		}

		if identity.Tenant != "" {
			// The tenant itself is applied by requestscoped from the identity.
			if tenant := r.Header.Get(types.HeaderTenant); tenant != "" && tenant != identity.Tenant {
				a.reject(w, r, http.StatusForbidden, reasonTenantMismatch, identity.Name)

				return
			}
		}

		next.ServeHTTP(w, r.WithContext(ContextWithIdentity(r.Context(), identity)))
	})
}

// authenticate returns the identity of the request, or nil and the failure reason.
func (a *Authenticator) authenticate(r *http.Request) (*Identity, string) {
	authorization := r.Header.Get("Authorization")
	if authorization == "" {
		return nil, reasonMissingCredentials
	}

	if username, password, ok := r.BasicAuth(); ok {
		return a.authenticateBasic(username, password)
	}

	scheme, token, found := strings.Cut(authorization, " ")
	if found && strings.EqualFold(scheme, "Bearer") {
		token = strings.TrimSpace(token)
		if token == "" {
			return nil, reasonMalformedCredentials
		}

		return a.authenticateToken(token)
	}

	return nil, reasonMalformedCredentials
}

func (a *Authenticator) authenticateBasic(username, password string) (*Identity, string) {
	u := a.usersByName[username]

	if u != nil && u.passwordHash != nil && a.cache.Has(username, password) {
		return u.identity, ""
	}

	if u == nil || u.passwordHash == nil {
		_ = a.compareHash(a.dummyHash, []byte(password))

		return nil, reasonInvalidCredentials
	}

	if err := a.compareHash(u.passwordHash, []byte(password)); err != nil {
		return nil, reasonInvalidCredentials
	}

	a.cache.Add(username, password)

	return u.identity, ""
}

func (a *Authenticator) authenticateToken(token string) (*Identity, string) {
	sum := sha256.Sum256([]byte(token))

	var found *Identity

	// Compare with every token (no early return) in constant time.
	for _, u := range a.users {
		if u.tokenHash != nil && subtle.ConstantTimeCompare(sum[:], u.tokenHash) == 1 {
			found = u.identity
		}
	}

	if found == nil {
		return nil, reasonInvalidCredentials
	}

	return found, ""
}

func (a *Authenticator) reject(w http.ResponseWriter, r *http.Request, status int, reason string, username string) {
	a.failures.WithLabelValues(reason).Inc()

	// Never log the credentials, only the user name once authenticated.
	a.logger.Debug().
		Str("reason", reason).
		Str("user", username).
		Str("remote_addr", r.RemoteAddr).
		Msgf("Rejected %s %s", r.Method, r.URL.Path)

	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", wwwAuthenticate)
	}

	http.Error(w, http.StatusText(status)+": "+strings.ReplaceAll(reason, "_", " "), status)
}

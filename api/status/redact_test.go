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
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/config"
)

// Secrets used by SecretConfig. They are distinct strings, so a test can check
// that none of them appears in an output.
const (
	secretRedisPassword      = "redis-secret-password"
	secretCassandraPassword  = "cassandra-secret-password"
	secretClickHousePassword = "clickhouse-secret-password"
	secretSentryKey          = "sentry-secret-key"
	secretPasswordHash       = "$2a$10$bcrypt-secret-hash"
	secretTokenHash          = "0123456789abcdef-secret-token-hash"
	secretURLPassword        = "url-secret-password"
)

// SecretStrings are the secrets of SecretConfig.
func SecretStrings() []string {
	return []string{
		secretRedisPassword,
		secretCassandraPassword,
		secretClickHousePassword,
		secretSentryKey,
		secretPasswordHash,
		secretTokenHash,
		secretURLPassword,
	}
}

// SecretConfig returns a configuration holding every kind of secret.
func SecretConfig() config.Config {
	cfg := config.DefaultConfig()
	cfg.Redis.Addresses = []string{"redis:6379"}
	cfg.Redis.Username = "redis-user"
	cfg.Redis.Password = secretRedisPassword
	cfg.Cassandra.Username = "cassandra-user"
	cfg.Cassandra.Password = secretCassandraPassword
	cfg.ClickHouse.Password = secretClickHousePassword
	cfg.Sentry.DSN = "https://" + secretSentryKey + "@o1.ingest.sentry.io/42"
	cfg.Telemetry.Address = "https://user:" + secretURLPassword + "@telemetry.example.com/v1?token=" + secretURLPassword
	cfg.Auth.Enabled = true
	cfg.Auth.Users = []config.AuthUser{
		{Name: "admin", PasswordHash: secretPasswordHash, Scopes: []string{"admin"}},
		{Name: "writer", TokenSHA256: secretTokenHash, Scopes: []string{"write"}, Tenant: "acme"},
	}

	return cfg
}

func TestRedactConfig(t *testing.T) {
	t.Parallel()

	redacted := Redact(SecretConfig())

	data, err := json.Marshal(redacted)
	if err != nil {
		t.Fatal(err)
	}

	for _, secret := range SecretStrings() {
		if strings.Contains(string(data), secret) {
			t.Errorf("the redacted config contains the secret %q: %s", secret, data)
		}
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	want := map[string]any{
		"redis.password":                        Redacted,
		"redis.username":                        "redis-user",
		"cassandra.password":                    Redacted,
		"clickhouse.password":                   Redacted,
		"sentry.dsn":                            Redacted,
		"telemetry.address":                     "https://" + Redacted + "@telemetry.example.com/v1?token=" + Redacted,
		"auth.enabled":                          true,
		"tls.cert_file":                         "",
		"listen_address":                        config.DefaultConfig().ListenAddress,
		"default_time_to_live":                  config.DefaultConfig().DefaultTimeToLive.String(),
		"clickhouse.wal_flush_delay":            "0s",
		"cassandra.aggregate.intended_duration": config.DefaultConfig().Cassandra.Aggregate.IntendedDuration.String(),
	}

	for path, wantValue := range want {
		if gotValue := lookup(t, got, path); !reflect.DeepEqual(gotValue, wantValue) {
			t.Errorf("%s = %#v, want %#v", path, gotValue, wantValue)
		}
	}

	users, _ := lookup(t, got, "auth.users").([]any)
	if len(users) != 2 {
		t.Fatalf("auth.users = %v, want 2 users", users)
	}

	wantUsers := []map[string]any{
		{"name": "admin", "password_hash": Redacted, "token_sha256": "", "tenant": ""},
		{"name": "writer", "password_hash": "", "token_sha256": Redacted, "tenant": "acme"},
	}

	for i, wantUser := range wantUsers {
		for key, wantValue := range wantUser {
			if gotValue := lookup(t, users[i], key); gotValue != wantValue {
				t.Errorf("auth.users[%d].%s = %#v, want %#v", i, key, gotValue, wantValue)
			}
		}
	}
}

// TestSensitiveFieldsAreTagged checks that every configuration field whose name
// looks like a secret has the sensitive tag: the tag is the source of truth, the
// name-based redaction is only a safety net.
func TestSensitiveFieldsAreTagged(t *testing.T) {
	t.Parallel()

	var check func(typ reflect.Type, path string)

	check = func(typ reflect.Type, path string) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}

		if typ.Kind() != reflect.Struct || typ == reflect.TypeFor[time.Time]() {
			return
		}

		for field := range typ.Fields() {
			name := path + fieldName(field)

			if IsSensitiveName(fieldName(field)) && field.Tag.Get(SensitiveTag) != "true" {
				t.Errorf("config field %s looks like a secret but has no `sensitive:\"true\"` tag", name)
			}

			check(field.Type, name+".")
		}
	}

	check(reflect.TypeFor[config.Config](), "")
}

func TestRedactValues(t *testing.T) {
	t.Parallel()

	type inner struct {
		Secret string `json:"value" sensitive:"true"`
		Plain  string `json:"plain"`
	}

	type sample struct {
		Inner      inner             `yaml:"inner"`
		Pointer    *inner            `yaml:"pointer"`
		Nil        *inner            `yaml:"nil"`
		APIToken   string            `yaml:"api_token"`
		Empty      string            `sensitive:"true" yaml:"empty"`
		Map        map[string]string `yaml:"map"`
		Func       func()            `yaml:"func"`
		Duration   time.Duration     `yaml:"duration"`
		Skipped    string            `yaml:"-"`
		unexported string
	}

	got := Redact(sample{
		Inner:      inner{Secret: "s1", Plain: "p1"},
		Pointer:    &inner{Secret: "s2", Plain: "p2"},
		APIToken:   "s3",
		Map:        map[string]string{"user_password": "s4", "host": "h"},
		Func:       func() {},
		Duration:   90 * time.Second,
		Skipped:    "s5",
		unexported: "s6",
	})

	want := map[string]any{
		"inner":     map[string]any{"value": Redacted, "plain": "p1"},
		"pointer":   map[string]any{"value": Redacted, "plain": "p2"},
		"nil":       nil,
		"api_token": Redacted,
		"empty":     "",
		"map":       map[string]any{"user_password": Redacted, "host": "h"},
		"func":      Redacted,
		"duration":  "1m30s",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Redact() = %#v, want %#v", got, want)
	}
}

func TestRedactString(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"localhost:9000":                         "localhost:9000",
		"admin@example.com":                      "admin@example.com",
		"user:pass@tcp(localhost:3306)/db":       Redacted + "@tcp(localhost:3306)/db",
		"redis://:pass@redis:6379/0":             "redis://" + Redacted + "@redis:6379/0",
		"https://key@sentry.io/1":                "https://" + Redacted + "@sentry.io/1",
		"https://host/path?a=1&password=x&b=2#f": "https://host/path?a=1&password=" + Redacted + "&b=2#f",
		"https://host/path/user@x":               "https://host/path/user@x",
		"":                                       "",
	}

	for input, want := range cases {
		if got := RedactString(input); got != want {
			t.Errorf("RedactString(%q) = %q, want %q", input, got, want)
		}
	}
}

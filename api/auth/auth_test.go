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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestParseForcedMatcher(t *testing.T) {
	t.Parallel()

	cases := []struct {
		value   string
		want    []string
		wantErr bool
	}{
		{value: "", want: nil},
		{value: `env="prod"`, want: []string{`env="prod"`}},
		{value: `{env="prod", team=~"a|b"}`, want: []string{`env="prod"`, `team=~"a|b"`}},
		{value: `env!="dev",team!~"x.*"`, want: []string{`env!="dev"`, `team!~"x.*"`}},
		// The format of the X-SquirrelDB-Forced-Matcher header.
		{value: "env=prod", want: []string{`env="prod"`}},
		{value: "env", wantErr: true},
		{value: `env="prod`, wantErr: true},
		{value: "=prod", wantErr: true},
	}

	for _, tc := range cases {
		matchers, err := ParseForcedMatcher(tc.value)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: expected an error, got %v", tc.value, matchers)
			}

			continue
		}

		if err != nil {
			t.Errorf("%q: %v", tc.value, err)

			continue
		}

		got := make([]string, 0, len(matchers))
		for _, m := range matchers {
			got = append(got, m.String())
		}

		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%q: got %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestValidateUsers(t *testing.T) {
	t.Parallel()

	valid := User{Name: "u", PasswordHash: mustHash(t, "p"), Scopes: []string{"read"}}

	cases := []struct {
		name    string
		mutate  func(u *User)
		wantErr bool
	}{
		{name: "valid", mutate: func(*User) {}},
		{name: "token only", mutate: func(u *User) { u.PasswordHash, u.TokenSHA256 = "", tokenHash("t") }},
		{name: "tenant and forced matcher", mutate: func(u *User) { u.Tenant, u.ForcedMatcher = "acme", "env=prod" }},
		{name: "no name", mutate: func(u *User) { u.Name = "" }, wantErr: true},
		{name: "no credentials", mutate: func(u *User) { u.PasswordHash = "" }, wantErr: true},
		{name: "clear text password", mutate: func(u *User) { u.PasswordHash = "secret" }, wantErr: true},
		{name: "short token hash", mutate: func(u *User) { u.TokenSHA256 = "abcd" }, wantErr: true},
		{name: "token not hashed", mutate: func(u *User) { u.TokenSHA256 = strings.Repeat("z", 64) }, wantErr: true},
		{name: "no scope", mutate: func(u *User) { u.Scopes = nil }, wantErr: true},
		{name: "unknown scope", mutate: func(u *User) { u.Scopes = []string{"read", "superuser"} }, wantErr: true},
		{name: "admin with tenant", mutate: func(u *User) { u.Scopes, u.Tenant = []string{"admin"}, "acme" }, wantErr: true},
		{
			name:    "admin with forced matcher",
			mutate:  func(u *User) { u.Scopes, u.ForcedMatcher = []string{"admin"}, "env=prod" },
			wantErr: true,
		},
		{name: "invalid forced matcher", mutate: func(u *User) { u.ForcedMatcher = `env="` }, wantErr: true},
	}

	for _, tc := range cases {
		user := valid
		tc.mutate(&user)

		err := ValidateUsers([]User{user})
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}

	if err := ValidateUsers([]User{valid, valid}); err == nil {
		t.Error("duplicate users: expected an error")
	}
}

func TestCommandHashPassword(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer

	code := runCommand([]string{"hash-password"}, strings.NewReader("my password\n"), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}

	hash := strings.TrimSpace(stdout.String())
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte("my password")); err != nil {
		t.Errorf("the printed hash doesn't match the password: %v", err)
	}

	if code := runCommand([]string{"hash-password"}, strings.NewReader("\n"), &stdout, &stderr); code == 0 {
		t.Error("an empty password was accepted")
	}

	if code := runCommand([]string{"unknown"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Errorf("unknown command: exit code = %d, want 2", code)
	}
}

func TestCommandGenToken(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer

	if code := runCommand([]string{"gen-token"}, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}

	values := map[string]string{}

	for line := range strings.Lines(stdout.String()) {
		key, value, _ := strings.Cut(strings.TrimSpace(line), ": ")
		values[key] = value
	}

	if len(values["token"]) < 32 {
		t.Fatalf("token %q is too short", values["token"])
	}

	sum := sha256.Sum256([]byte(values["token"]))
	if values["token_sha256"] != hex.EncodeToString(sum[:]) {
		t.Errorf("token_sha256 = %q, doesn't match the token", values["token_sha256"])
	}

	user := User{Name: "u", TokenSHA256: values["token_sha256"], Scopes: []string{"read"}}
	if err := ValidateUsers([]User{user}); err != nil {
		t.Errorf("the generated token hash is not valid: %v", err)
	}
}

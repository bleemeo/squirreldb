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
	"encoding"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
)

// Redacted replaces a secret value that is set. A secret that is not set is
// shown as empty, so the page tells whether it is configured.
const Redacted = "<redacted>"

// SensitiveTag is the struct tag that marks a field as a secret: `sensitive:"true"`.
const SensitiveTag = "sensitive"

// sensitiveNameParts are the parts of a field (or map key) name that make it
// redacted even without the sensitive tag. This is a safety net for a secret
// added without the tag: when in doubt, the value is redacted.
var sensitiveNameParts = []string{ //nolint:gochecknoglobals
	"password",
	"passwd",
	"secret",
	"token",
	"hash",
	"dsn",
	"credential",
	"private_key",
	"privatekey",
	"api_key",
	"apikey",
}

// IsSensitiveName returns whether a field or map key name looks like a secret.
func IsSensitiveName(name string) bool {
	name = strings.ToLower(name)

	for _, part := range sensitiveNameParts {
		if strings.Contains(name, part) {
			return true
		}
	}

	return false
}

// Redact returns a JSON-friendly copy of v (maps, slices and scalars) with its
// secrets redacted:
//   - struct fields tagged `sensitive:"true"`, and fields or map keys whose name
//     looks like a secret (see IsSensitiveName), are replaced by Redacted;
//   - credentials in URL-like strings (scheme://user:password@host, user:password@host)
//     and secret-looking query parameters are replaced by Redacted.
//
// Struct fields are named after their yaml tag (then their json tag, then their
// Go name). Values of an unsupported kind (functions, channels, ...) are redacted.
func Redact(v any) any {
	return redactValue(reflect.ValueOf(v))
}

func redactValue(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}

	if v.CanInterface() {
		if d, ok := reflect.TypeAssert[time.Duration](v); ok {
			return d.String()
		}

		if t, ok := reflect.TypeAssert[time.Time](v); ok {
			return t.Format(time.RFC3339Nano)
		}
	}

	switch v.Kind() { //nolint:exhaustive // The other kinds are redacted by default.
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}

		return redactValue(v.Elem())
	case reflect.Struct:
		return redactStruct(v)
	case reflect.Map:
		return redactMap(v)
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return []any{}
		}

		result := make([]any, v.Len())
		for i := range v.Len() {
			result[i] = redactValue(v.Index(i))
		}

		return result
	case reflect.String:
		return RedactString(v.String())
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	default:
		return Redacted
	}
}

func redactStruct(v reflect.Value) any {
	if v.CanInterface() {
		if m, ok := reflect.TypeAssert[encoding.TextMarshaler](v); ok {
			if text, err := m.MarshalText(); err == nil {
				return RedactString(string(text))
			}
		}
	}

	result := make(map[string]any, v.NumField())
	t := v.Type()

	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		name := fieldName(field)
		if name == "-" {
			continue
		}

		if field.Tag.Get(SensitiveTag) == "true" || IsSensitiveName(name) {
			result[name] = redactSecret(v.Field(i))

			continue
		}

		result[name] = redactValue(v.Field(i))
	}

	return result
}

func fieldName(field reflect.StructField) string {
	for _, key := range []string{"yaml", "json"} {
		if name, _, _ := strings.Cut(field.Tag.Get(key), ","); name != "" {
			return name
		}
	}

	return field.Name
}

func redactMap(v reflect.Value) any {
	if v.IsNil() {
		return map[string]any{}
	}

	result := make(map[string]any, v.Len())
	keys := v.MapKeys()

	// Sort the keys so a redacted key never depends on the iteration order.
	sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })

	for _, key := range keys {
		name := fmt.Sprint(redactValue(key))

		if IsSensitiveName(name) {
			result[name] = redactSecret(v.MapIndex(key))

			continue
		}

		result[name] = redactValue(v.MapIndex(key))
	}

	return result
}

// redactSecret returns Redacted for a secret that is set, and an empty value
// (of the same JSON kind when possible) for one that is not.
func redactSecret(v reflect.Value) any {
	if !v.IsValid() || v.IsZero() {
		switch v.Kind() { //nolint:exhaustive // Every other kind is shown as an empty string.
		case reflect.Slice, reflect.Array:
			return []any{}
		case reflect.Map:
			return map[string]any{}
		default:
			return ""
		}
	}

	if (v.Kind() == reflect.Slice || v.Kind() == reflect.Map) && v.Len() == 0 {
		return redactSecret(reflect.Value{})
	}

	return Redacted
}

// RedactString redacts the credentials of a URL-like string: the user info of
// "scheme://user:password@host" or "user:password@host" and the secret-looking
// query parameters. Other strings are returned unchanged.
func RedactString(s string) string {
	scheme, rest, hasScheme := strings.Cut(s, "://")
	if !hasScheme {
		scheme, rest = "", s
	}

	// The user info ends at the last "@" of the authority (before any path, query or fragment).
	authorityEnd := len(rest)
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authorityEnd = i
	}

	authority := rest[:authorityEnd]

	if at := strings.LastIndex(authority, "@"); at >= 0 {
		userInfo := authority[:at]

		// Without a scheme, "name@host" is likely an e-mail address: only a
		// "user:password@" user info is redacted. With a scheme, any user info is.
		if hasScheme || strings.Contains(userInfo, ":") {
			rest = Redacted + rest[at:]
		}
	}

	if hasScheme {
		s = scheme + "://" + rest
	} else {
		s = rest
	}

	return redactQuery(s)
}

// redactQuery redacts the values of the secret-looking query parameters.
func redactQuery(s string) string {
	base, query, found := strings.Cut(s, "?")
	if !found {
		return s
	}

	query, fragment, hasFragment := strings.Cut(query, "#")

	params := strings.Split(query, "&")
	for i, param := range params {
		name, _, hasValue := strings.Cut(param, "=")

		unescaped, err := url.QueryUnescape(name)
		if err != nil {
			unescaped = name
		}

		if hasValue && IsSensitiveName(unescaped) {
			params[i] = name + "=" + Redacted
		}
	}

	s = base + "?" + strings.Join(params, "&")
	if hasFragment {
		s += "#" + fragment
	}

	return s
}

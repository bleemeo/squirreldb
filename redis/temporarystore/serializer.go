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

package temporarystore

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

// Labels are serialized one per line (real '\n' separates labels), each as
// name=value. Label names AND values can be any UTF-8 string (Prometheus uses
// the UTF-8 validation scheme), so both are escaped: '\' -> '\\' and a real
// newline -> '\n'. Names additionally escape '=' -> '\=' so the first
// *unescaped* '=' always marks the name/value boundary. A real '\n' byte in the
// output is therefore always a label separator, never part of a name or value.
func serializeLabels(l labels.Labels) string {
	if l.Len() == 0 {
		return ""
	}

	var b strings.Builder

	b.Grow(l.Len() * 24) // estimate

	first := true

	l.Range(func(lbl labels.Label) {
		if !first {
			b.WriteByte('\n')
		}

		first = false

		writeEscaped(&b, lbl.Name, true)
		b.WriteByte('=')
		writeEscaped(&b, lbl.Value, false)
	})

	return b.String()
}

// writeEscaped escapes '\' and newlines; when escapeEq is set it also escapes
// '=' (used for label names, whose '=' must not be mistaken for the separator).
func writeEscaped(b *strings.Builder, s string, escapeEq bool) {
	// Fast path: most strings need no escaping.
	if !strings.ContainsAny(s, "\\\n") && !(escapeEq && strings.ContainsRune(s, '=')) { //nolint:staticcheck
		b.WriteString(s)

		return
	}

	for i := range len(s) {
		switch {
		case s[i] == '\\':
			b.WriteByte('\\')
			b.WriteByte('\\')
		case s[i] == '\n':
			b.WriteByte('\\')
			b.WriteByte('n')
		case escapeEq && s[i] == '=':
			b.WriteByte('\\')
			b.WriteByte('=')
		default:
			b.WriteByte(s[i])
		}
	}
}

func deserializeLabels(input string) labels.Labels {
	if input == "" {
		return labels.EmptyLabels()
	}

	result := make([]labels.Label, 0, strings.Count(input, "\n")+1)

	for input != "" {
		var line string

		if i := strings.IndexByte(input, '\n'); i >= 0 {
			line = input[:i]
			input = input[i+1:]
		} else {
			line = input
			input = ""
		}

		name, value, ok := splitNameValue(line)
		if !ok || name == "" {
			continue
		}

		result = append(result, labels.Label{
			Name:  unescape(name),
			Value: unescape(value),
		})
	}

	return labels.New(result...)
}

// splitNameValue splits a serialized line on its first *unescaped* '=' (the
// name/value separator), skipping escape pairs so an escaped '\=' inside the
// name is not mistaken for it. The returned parts are still escaped.
func splitNameValue(line string) (name, value string, ok bool) {
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++ // skip the escaped char
		case '=':
			return line[:i], line[i+1:], true
		}
	}

	return "", "", false
}

func unescape(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}

	b := make([]byte, 0, len(s))

	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b = append(b, s[i])

			continue
		}

		i++

		switch s[i] {
		case '\\':
			b = append(b, '\\')
		case 'n':
			b = append(b, '\n')
		case '=':
			b = append(b, '=')
		default:
			b = append(b, '\\', s[i])
		}
	}

	return string(b)
}

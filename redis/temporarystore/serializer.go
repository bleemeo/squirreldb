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

		b.WriteString(lbl.Name)
		b.WriteByte('=')
		writeEscaped(&b, lbl.Value)
	})

	return b.String()
}

func writeEscaped(b *strings.Builder, s string) {
	// Fast path: most value won't need escaping.
	if !strings.ContainsAny(s, "\\\n") {
		b.WriteString(s)

		return
	}

	for i := range len(s) {
		switch s[i] {
		case '\\':
			b.WriteByte('\\')
			b.WriteByte('\\')
		case '\n':
			b.WriteByte('\\')
			b.WriteByte('n')
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

		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			continue
		}

		result = append(result, labels.Label{
			Name:  line[:eq],
			Value: unescapeValue(line[eq+1:]),
		})
	}

	return labels.New(result...)
}

func unescapeValue(s string) string {
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
		default:
			b = append(b, '\\', s[i])
		}
	}

	return string(b)
}

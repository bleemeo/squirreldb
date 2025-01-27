// Copyright 2015-2025 Bleemeo
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

package logger

import (
	"io"
	"testing"
)

type testWriter struct {
	*testing.T
}

// TestWriter returns an [io.Writer] that writes using the given [testing.T]'s Log() method.
// This makes it easier to understand from which subtest a log message comes from,
// compared to a message that has been written directly on [os.Stdout] or [os.Stderr].
func TestWriter(t *testing.T) io.Writer {
	t.Helper()

	return &testWriter{T: t}
}

func (t testWriter) Write(p []byte) (n int, err error) {
	t.Log(string(p))

	return len(p), nil
}

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
	"fmt"
	"io"
	"sync"

	zlogsentry "github.com/archdx/zerolog-sentry"
	"github.com/rs/zerolog"
)

// SentrySafeWriter is a thread safe wrapper for a sentry writer.
type SentrySafeWriter struct {
	sentryWriter io.WriteCloser
	minLevel     zerolog.Level
	l            sync.Mutex
}

// Write to the child writer.
func (w *SentrySafeWriter) Write(p []byte) (n int, err error) {
	// The sentry SDK is not thread safe.
	w.l.Lock()
	defer w.l.Unlock()

	n, err = w.sentryWriter.Write(p)
	if err != nil {
		return n, fmt.Errorf("write: %w", err)
	}

	return n, nil
}

// WriteLevel writes an event at a level.
func (w *SentrySafeWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	if level < w.minLevel {
		return len(p), nil
	}

	// The sentry SDK is not thread safe.
	w.l.Lock()
	defer w.l.Unlock()

	n, err = w.sentryWriter.Write(p)
	if err != nil {
		return n, fmt.Errorf("write: %w", err)
	}

	return n, nil
}

// Close flushes the events to sentry.
func (w *SentrySafeWriter) Close() error {
	if err := w.sentryWriter.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	return nil
}

// InternalSetWriter sets a new writer. Used in tests.
func (w *SentrySafeWriter) InternalSetWriter(newWriter io.WriteCloser) {
	w.sentryWriter = newWriter
}

// NewSentryWriter returns a thread safe writer that logs events to sentry.
func NewSentryWriter(dsn string, level zerolog.Level, opts ...zlogsentry.WriterOption) (io.WriteCloser, error) {
	sentryWriter, err := zlogsentry.New(dsn, opts...)
	if err != nil {
		return nil, fmt.Errorf("create sentry writer: %w", err)
	}

	safeWriter := &SentrySafeWriter{
		sentryWriter: sentryWriter,
		minLevel:     level,
	}

	return safeWriter, nil
}

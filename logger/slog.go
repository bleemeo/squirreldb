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
	"context"
	"log/slog"

	"github.com/rs/zerolog"
)

func slogLevelToZerologLevel(level slog.Level) zerolog.Level {
	switch level {
	case slog.LevelDebug:
		return zerolog.DebugLevel
	case slog.LevelInfo:
		return zerolog.InfoLevel
	case slog.LevelWarn:
		return zerolog.WarnLevel
	case slog.LevelError:
		return zerolog.ErrorLevel
	default:
		return zerolog.NoLevel
	}
}

type slogToZerologAdapter struct {
	zl    zerolog.Logger
	group string
}

func (s2z slogToZerologAdapter) fmtKey(key string) string {
	if s2z.group != "" {
		return s2z.group + "." + key
	}

	return key
}

func (s2z slogToZerologAdapter) Enabled(_ context.Context, level slog.Level) bool {
	currentLevel := s2z.zl.GetLevel()

	switch level {
	case slog.LevelDebug:
		return currentLevel <= zerolog.DebugLevel
	case slog.LevelInfo:
		return currentLevel <= zerolog.InfoLevel
	case slog.LevelWarn:
		return currentLevel <= zerolog.WarnLevel
	case slog.LevelError:
		return currentLevel <= zerolog.ErrorLevel
	default:
		return false
	}
}

func (s2z slogToZerologAdapter) Handle(ctx context.Context, record slog.Record) error {
	evt := s2z.zl.WithLevel(slogLevelToZerologLevel(record.Level)).Ctx(ctx) //nolint:zerologlint

	if !record.Time.IsZero() {
		evt = evt.Time(zerolog.TimestampFieldName, record.Time)
	}

	record.Attrs(func(attr slog.Attr) bool {
		evt = s2z.eventWith(evt, s2z.fmtKey(attr.Key), attr.Value)

		return true
	})

	evt.Msg(record.Message)

	return nil
}

func (s2z slogToZerologAdapter) eventWith(
	evt *zerolog.Event,
	key string,
	val slog.Value,
) *zerolog.Event { //nolint: dupl
	switch val.Kind() {
	case slog.KindAny:
		return evt.Any(key, val.Any())
	case slog.KindBool:
		return evt.Bool(key, val.Bool())
	case slog.KindDuration:
		return evt.Dur(key, val.Duration())
	case slog.KindFloat64:
		return evt.Float64(key, val.Float64())
	case slog.KindInt64:
		return evt.Int64(key, val.Int64())
	case slog.KindString:
		return evt.Str(key, val.String())
	case slog.KindTime:
		return evt.Time(key, val.Time())
	case slog.KindUint64:
		return evt.Uint64(key, val.Uint64())
	case slog.KindGroup:
		for _, attr := range val.Group() {
			evt = s2z.eventWith(evt, key+"."+attr.Key, attr.Value)
		}

		return evt
	case slog.KindLogValuer:
		return s2z.eventWith(evt, key, val.Resolve())
	default:
		// We can't use the value when its kind is unknown: slog will panic.
		s2z.zl.Warn().Msgf("Unknown slog attribute kind %[1]q (%[1]d) (key: %q)", val.Kind(), key)
	}

	return evt
}

func (s2z slogToZerologAdapter) WithAttrs(attrs []slog.Attr) slog.Handler {
	zlogCtx := s2z.zl.With()

	for _, attr := range attrs {
		zlogCtx = s2z.contextWith(zlogCtx, s2z.fmtKey(attr.Key), attr.Value)
	}

	return slogToZerologAdapter{zl: zlogCtx.Logger()}
}

func (s2z slogToZerologAdapter) contextWith(
	zlogCtx zerolog.Context,
	key string,
	val slog.Value,
) zerolog.Context { //nolint: dupl
	switch val.Kind() {
	case slog.KindAny:
		return zlogCtx.Any(key, val.Any())
	case slog.KindBool:
		return zlogCtx.Bool(key, val.Bool())
	case slog.KindDuration:
		return zlogCtx.Dur(key, val.Duration())
	case slog.KindFloat64:
		return zlogCtx.Float64(key, val.Float64())
	case slog.KindInt64:
		return zlogCtx.Int64(key, val.Int64())
	case slog.KindString:
		return zlogCtx.Str(key, val.String())
	case slog.KindTime:
		return zlogCtx.Time(key, val.Time())
	case slog.KindUint64:
		return zlogCtx.Uint64(key, val.Uint64())
	case slog.KindGroup:
		for _, attr := range val.Group() {
			zlogCtx = s2z.contextWith(zlogCtx, key+"."+attr.Key, attr.Value)
		}

		return zlogCtx
	case slog.KindLogValuer:
		return s2z.contextWith(zlogCtx, key, val.Resolve())
	default:
		// We can't use the value when its kind is unknown: slog will panic.
		s2z.zl.Warn().Msgf("Unknown slog attribute kind %[1]q (%[1]d) (key: %q)", val.Kind(), key)
	}

	return zlogCtx
}

func (s2z slogToZerologAdapter) WithGroup(name string) slog.Handler {
	if name == "" {
		return s2z
	}

	return slogToZerologAdapter{zl: s2z.zl, group: name}
}

// NewSLogger returns a slog.Logger that sends events to the given zerolog.Logger.
func NewSLogger(logger zerolog.Logger) *slog.Logger {
	return slog.New(slogToZerologAdapter{zl: logger})
}

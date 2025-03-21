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

	"github.com/go-kit/log"
	"github.com/rs/zerolog"
)

type zerologLogger struct {
	Logger *zerolog.Logger
}

//nolint: zerologlint,gofmt,gofumpt,goimports
func (l zerologLogger) Log(kv ...any) error {
	fields := make(map[string]any)

	for i := 0; i < len(kv); i += 2 {
		if i+1 < len(kv) {
			fields[fmt.Sprint(kv[i])] = kv[i+1]
		} else {
			fields[fmt.Sprint(kv[i])] = "(MISSING)"
		}
	}

	var e *zerolog.Event

	switch fmt.Sprint(fields["level"]) {
	case "debug":
		e = l.Logger.Debug()
	case "info":
		e = l.Logger.Info()
	case "warn":
		e = l.Logger.Warn()
	case "error":
		e = l.Logger.Error()
	default:
		e = l.Logger.Debug()
	}

	msg, exists := fields["msg"]
	if !exists {
		msg = ""
	}

	delete(fields, "level") // level key will be added by zerolog
	delete(fields, "msg")
	e.Fields(fields).Msg(fmt.Sprint(msg))

	return nil
}

// NewKitLogger returns a Go kit log.Logger that sends
// log events to a zerolog.Logger.
func NewKitLogger(logger *zerolog.Logger) log.Logger {
	zlog := zerologLogger{
		Logger: logger,
	}

	return zlog
}

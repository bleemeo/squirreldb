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
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// NewConsoleWriter returns a zerolog writer with colored console output.
func NewConsoleWriter(disableColor bool) io.Writer {
	writer := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "2006-01-02 15:04:05.000",
		NoColor:    disableColor,
	}

	return writer
}

// NewLogger returns a zerolog logger with timestamp and level.
func NewLogger(w io.Writer, level zerolog.Level) zerolog.Logger {
	return log.Output(w).With().Timestamp().Logger().Level(level)
}

// NewTestLogger returns a zerolog logger ready to use in tests.
func NewTestLogger(disableColor bool) zerolog.Logger {
	return NewLogger(NewConsoleWriter(disableColor), zerolog.TraceLevel)
}

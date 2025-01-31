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
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/rs/zerolog"
)

type dummyValuer struct {
	val any
}

func (dv dummyValuer) LogValue() slog.Value {
	return slog.AnyValue(dv.val)
}

func TestSlog2Zerolog(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string

		loggerLevel zerolog.Level

		msgLevel slog.Level
		fields   []any // key-value pairs (keys must be strings)
		msg      string

		expectedOutput string
	}{
		{
			name:           "classic",
			loggerLevel:    zerolog.InfoLevel,
			msgLevel:       slog.LevelWarn,
			fields:         []any{"component", "worker"},
			msg:            "Some notice",
			expectedOutput: "2024-12-31 17:57:37 WRN Some notice component=worker\n",
		},
		{
			name:           "under level",
			loggerLevel:    zerolog.InfoLevel,
			msgLevel:       slog.LevelDebug,
			msg:            "Some non significant info",
			expectedOutput: "",
		},
		{
			name:           "variable field",
			loggerLevel:    zerolog.DebugLevel,
			msgLevel:       slog.LevelDebug,
			fields:         []any{"pi", dummyValuer{3.14}},
			msg:            "Some variable value",
			expectedOutput: "2024-12-31 17:57:37 DBG Some variable value pi=3.14\n",
		},
	}

	now := time.Date(2024, 12, 31, 17, 57, 37, 0, time.Local)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := new(bytes.Buffer)
			zLogger := zerolog.New(zerolog.NewConsoleWriter(
				func(w *zerolog.ConsoleWriter) {
					w.Out = buf
					w.NoColor = true
					w.TimeFormat = time.DateTime
				},
			)).Level(tc.loggerLevel)

			// We override the time field to force it to have a fixed value.
			fields := append(tc.fields, zerolog.TimestampFieldName, now) //nolint: gocritic

			NewSLogger(zLogger).Log(context.Background(), tc.msgLevel, tc.msg, fields...)

			if diff := cmp.Diff(tc.expectedOutput, buf.String()); diff != "" {
				t.Fatalf("Unexpected output (-want +got):\n%s", diff)
			}
		})
	}
}

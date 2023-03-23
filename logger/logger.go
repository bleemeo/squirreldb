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
		Out:        os.Stdout,
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

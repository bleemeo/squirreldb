package logger

import (
	"github.com/rs/zerolog/log"
)

// ProcessPanic logs panics to Sentry.
// It should be defered at the beginning of every new goroutines.
func ProcessPanic() {
	if res := recover(); res != nil {
		// If the panic was caused by an error, add it with the "error" field,
		// this makes the logger send a stacktrace to Sentry.
		if err, ok := res.(error); ok {
			log.Panic().Err(err).Msg("Panic")
		}

		log.Panic().Msgf("%s", res)
	}
}

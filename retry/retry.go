package retry

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"
)

// NewExponentialBackOff creates a new ExponentialBackOff object.
func NewExponentialBackOff(ctx context.Context, maxInterval time.Duration) backoff.BackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()

	exponentialBackOff.MaxInterval = maxInterval

	return backoff.WithContext(exponentialBackOff, ctx)
}

// Print displays if an error message has occurred, the time before the next attempt and a resolution message.
func Print(o backoff.Operation, b backoff.BackOff, action string) error {
	tried := false

	err := backoff.RetryNotify(o, b, func(err error, duration time.Duration) {
		if err != nil {
			tried = true
			log.Err(err).Msgf("Error during %s", action)
			log.Info().Msgf("|__ Retry in %v", duration)
		}
	})

	if tried && err == nil {
		log.Info().Msgf("Resolved %s", action)
	}

	return err //nolint:wrapcheck
}

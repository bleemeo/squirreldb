package retry

import (
	"github.com/cenkalti/backoff"
	"log"
	"time"
)

// NewExponentialBackOff creates a new ExponentialBackOff object
func NewExponentialBackOff(maxInterval time.Duration) backoff.BackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()

	exponentialBackOff.MaxInterval = maxInterval

	return exponentialBackOff
}

// Print displays if an error message has occurred, the time before the next attempt and a resolution message
func Print(o backoff.Operation, b backoff.BackOff, logger *log.Logger, errorMsg, resolveMsg string) {
	tried := false

	_ = backoff.RetryNotify(o, b, func(err error, duration time.Duration) {
		if err != nil {
			tried = true
			logger.Printf("%s (%v)", errorMsg, err)
			logger.Printf("|__ Retry in %v", duration)
		}
	})

	if tried {
		logger.Printf("%s", resolveMsg)
	}
}

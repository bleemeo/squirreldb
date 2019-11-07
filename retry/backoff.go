package retry

import (
	"github.com/cenkalti/backoff"
	"log"
	"time"
)

// Do is a backoff.Retry wrapper that adds useful logs
func Do(fun func() error, logger *log.Logger, errorMsg, resolveMsg string, b backoff.BackOff) {
	tried := false

	_ = backoff.RetryNotify(fun, b, func(err error, duration time.Duration) {
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

// NewBackOff create a new ExponentialBackOff with specified settings
func NewBackOff(maxInterval time.Duration) backoff.BackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()

	exponentialBackOff.MaxInterval = maxInterval

	return exponentialBackOff
}

package retry

import (
	"github.com/cenkalti/backoff"
	"time"
)

// NewBackOff create a new ExponentialBackOff with specified settings
func NewBackOff(maxInterval time.Duration) backoff.BackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()

	exponentialBackOff.MaxInterval = maxInterval

	return exponentialBackOff
}

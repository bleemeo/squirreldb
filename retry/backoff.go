package retry

import (
	"github.com/cenkalti/backoff"
	"time"
)

// TODO: Comment
func NewBackOff(maxInterval time.Duration) backoff.BackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()

	exponentialBackOff.MaxInterval = maxInterval

	return exponentialBackOff
}

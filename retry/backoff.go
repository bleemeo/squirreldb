package retry

import (
	"github.com/cenkalti/backoff"
	"log"
	"os"
	"time"
)

var logger = log.New(os.Stdout, "[retry] ", log.LstdFlags)

// Do is a backoff.Retry wrapper that adds useful logs
func Do(fun func() error, moduleName, funcName, errorMsg, resolveMsg string, b backoff.BackOff) {
	tried := false

	_ = backoff.RetryNotify(func() error {
		err := fun()

		if err != nil {
			logger.Printf("[%s] %s: %s ( %v )"+"\n", moduleName, funcName, errorMsg, err)
			tried = true
		} else if tried {
			logger.Printf("[%s] %s: %s"+"\n", moduleName, funcName, resolveMsg)
		}

		return err
	}, b, func(err error, duration time.Duration) {
		if err != nil {
			logger.Printf("|__ Retry in %v"+"\n", duration)
		}
	})
}

// NewBackOff create a new ExponentialBackOff with specified settings
func NewBackOff(maxInterval time.Duration) backoff.BackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()

	exponentialBackOff.MaxInterval = maxInterval

	return exponentialBackOff
}

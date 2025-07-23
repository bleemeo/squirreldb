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

package retry

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog"
)

// NewExponentialBackOff creates a new ExponentialBackOff object.
func NewExponentialBackOff(ctx context.Context, maxInterval time.Duration) backoff.BackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()

	exponentialBackOff.MaxInterval = maxInterval

	return backoff.WithContext(exponentialBackOff, ctx)
}

// Print displays if an error message has occurred, the time before the next attempt and a resolution message.
func Print(o backoff.Operation, b backoff.BackOff, logger zerolog.Logger, action string) error {
	tried := false

	err := backoff.RetryNotify(o, b, func(err error, duration time.Duration) {
		if err != nil {
			tried = true

			logger.Err(err).Msgf("Error during %s", action)
			logger.Info().Msgf("|__ Retry in %v", duration)
		}
	})
	if tried && err == nil {
		logger.Info().Msgf("Resolved %s", action)
	}

	return err //nolint:wrapcheck
}

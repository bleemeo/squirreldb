// Copyright 2015-2026 Bleemeo
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

// Package processsetup contains utility function to initialize & setup a SquirrelDB process
package processsetup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	zlogsentry "github.com/archdx/zerolog-sentry"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/core/version"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// SetupSentryLogger sets the default zerolog logger to a Sentry logger, or a simple
// console logger if Sentry is disabled. The closer returned should be closed at the
// end of the program to flush the events to Sentry.
func SetupSentryLogger(cfg config.Config) io.Closer {
	consoleWriter := logger.NewConsoleWriter(cfg.Log.DisableColor)
	logLevel := zerolog.Level(cfg.Log.Level) //nolint:gosec
	release := zlogsentry.WithRelease(fmt.Sprintf("squirreldb@%s-%s", version.Version, version.Commit))

	if cfg.Sentry.DSN == "" {
		log.Logger = logger.NewLogger(consoleWriter, logLevel)

		return nil
	}

	sentryWriter, err := logger.NewSentryWriter(cfg.Sentry.DSN, zerolog.ErrorLevel, release)
	if err != nil {
		log.Err(err).Msg("Failed to initialize sentry")

		log.Logger = logger.NewLogger(consoleWriter, logLevel)

		return nil
	}

	// Set up logger with level filter.
	multiWriter := zerolog.MultiLevelWriter(consoleWriter, sentryWriter)
	log.Logger = logger.NewLogger(multiWriter, logLevel)

	return sentryWriter
}

// RunWithSignalHandler runs given function with two context:
//   - The first context is canceled on kill or ctrl+c
//   - The second context is canceled on a second kill or ctrl+c
//
// The function is allowed to perform closing action once first context expire
// (but it should be "quick") and the function should exit immediately when second
// context expire.
//
// In any case, on a third kill or ctrl+c, RunWithSignalHandler exit in any case.
func RunWithSignalHandler(f func(context.Context, context.Context) error) error {
	signalChan := make(chan os.Signal, 1)
	runTerminated := make(chan error, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	largeCtx, largeCancel := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancel(largeCtx)

	defer func() {
		signal.Stop(signalChan)
		close(signalChan)
		cancel()
		largeCancel()
	}()

	go func() {
		defer logger.ProcessPanic()

		runTerminated <- f(ctx, largeCtx)
	}()

	stopCount := 0

	for {
		select {
		case err := <-runTerminated:
			cancel()
			largeCancel()

			return err
		case <-signalChan:
			switch stopCount {
			case 0:
				log.Info().Msg("Received stop signal, start graceful shutdown")

				cancel()

				stopCount++
			case 1:
				log.Warn().Msg("Received 2 stop signals, request immediate shutdown")

				largeCancel()

				stopCount++
			default:
				return errors.New("forced shutdown")
			}
		}
	}
}

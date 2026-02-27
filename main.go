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

package main

import (
	"fmt"
	"io"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/logger"

	zlogsentry "github.com/archdx/zerolog-sentry"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// variable set by GoReleaser
//
//nolint:gochecknoglobals
var (
	version string
	commit  string
	date    string
)

// setupSentryLogger sets the default zerolog logger to a Sentry logger, or a simple
// console logger if Sentry is disabled. The closer returned should be closed at the
// end of the program to flush the events to Sentry.
func setupSentryLogger(cfg config.Config) io.Closer {
	consoleWriter := logger.NewConsoleWriter(cfg.Log.DisableColor)
	logLevel := zerolog.Level(cfg.Log.Level) //nolint:gosec
	release := zlogsentry.WithRelease(fmt.Sprintf("squirreldb@%s-%s", version, commit))

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

func main() {
	daemon.Version = version
	daemon.Commit = commit
	daemon.Date = date

	// Change the default time format of zerolog to allow millisecond precision.
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs

	// Initialize the logger temporarily before loading the config.
	log.Logger = logger.NewLogger(logger.NewConsoleWriter(false), zerolog.TraceLevel)

	cfg, warnings, err := daemon.Config()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read config")
	}

	if warnings != nil {
		log.Warn().Err(warnings).Msg("Got warnings while loading config")
	}

	if sentryCloser := setupSentryLogger(cfg); sentryCloser != nil {
		defer sentryCloser.Close()
	}

	defer logger.ProcessPanic()

	squirreldb := &daemon.SquirrelDB{
		Config: cfg,
		Logger: log.With().Str("component", "daemon").Logger(),
	}

	log.Info().Msgf("Starting SquirrelDB %s (commit %s)", version, commit)

	err = daemon.RunWithSignalHandler(squirreldb.Run)
	if err != nil {
		log.Err(err).Msg("Failed to run SquirrelDB")
	}

	log.Debug().Msg("SquirrelDB is stopped")
}

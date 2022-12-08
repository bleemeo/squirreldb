package main

import (
	"fmt"
	"io"
	"math/rand"
	"squirreldb/config2"
	"squirreldb/daemon"
	"squirreldb/logger"
	"time"

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
func setupSentryLogger(cfg config2.Config) io.Closer {
	consoleWriter := logger.NewConsoleWriter(cfg.Log.DisableColor)
	logLevel := zerolog.Level(cfg.Log.Level)
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
	rand.Seed(time.Now().UnixNano())

	daemon.Version = version
	daemon.Commit = commit
	daemon.Date = date

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

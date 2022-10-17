package main

import (
	"fmt"
	"io"
	"math/rand"
	"squirreldb/config"
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
func setupSentryLogger(cfg *config.Config) io.Closer {
	consoleWriter := logger.NewConsoleWriter(cfg.Bool("log.disable_color"))
	logLevel := zerolog.Level(cfg.Int("log.level"))
	release := zlogsentry.WithRelease(fmt.Sprintf("squirreldb@%s-%s", version, commit))

	sentryDSN := cfg.String("sentry.dsn")
	if sentryDSN == "" {
		log.Logger = logger.NewLogger(consoleWriter, logLevel)

		return nil
	}

	sentryWriter, err := logger.NewSentryWriter(sentryDSN, zerolog.ErrorLevel, release)
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

	cfg, err := daemon.Config()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read config")
	}

	if sentryCloser := setupSentryLogger(cfg); sentryCloser != nil {
		defer sentryCloser.Close()
	}

	defer logger.ProcessPanic()

	squirreldb := &daemon.SquirrelDB{
		Config:                     cfg,
		Logger:                     log.With().Str("component", "daemon").Logger(),
		DebugDisableBackgroundTask: cfg.Bool("debug-disable-background-task"),
	}

	log.Info().Msgf("Starting SquirrelDB %s (commit %s)", version, commit)

	err = daemon.RunWithSignalHandler(squirreldb.Run)
	if err != nil {
		log.Err(err).Msg("Failed to run SquirrelDB")
	}

	log.Debug().Msg("SquirrelDB is stopped")
}

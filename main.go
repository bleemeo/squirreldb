package main

import (
	"fmt"
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
//nolint:gochecknoglobals
var (
	version string
	commit  string
	date    string
)

func setupSentryLogger(cfg *config.Config) bool {
	consoleWriter := logger.NewConsoleWriter(cfg.Bool("log.disable_color"))
	logLevel := zerolog.Level(cfg.Int("log.level"))
	release := zlogsentry.WithRelease(fmt.Sprintf("squirreldb@%s-%s", version, commit))

	sentryDSN := cfg.String("sentry.dsn")
	if sentryDSN == "" {
		log.Logger = logger.NewLogger(consoleWriter, logLevel)

		return false
	}

	sentryWriter, err := logger.NewSentryWriter(sentryDSN, zerolog.ErrorLevel, release)
	if err != nil {
		log.Err(err).Msg("Failed to initialize sentry")

		log.Logger = logger.NewLogger(consoleWriter, logLevel)

		return false
	}

	// The writer must be closed to flush events to Sentry.
	defer sentryWriter.Close()

	// Set up logger with level filter.
	multiWriter := zerolog.MultiLevelWriter(consoleWriter, sentryWriter)
	log.Logger = logger.NewLogger(multiWriter, logLevel)

	return true
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

	setupSentryLogger(cfg)

	defer logger.ProcessPanic()

	squirreldb := &daemon.SquirrelDB{
		Config: cfg,
	}

	log.Info().Msgf("Starting SquirrelDB %s (commit %s)", version, commit)

	err = daemon.RunWithSignalHandler(squirreldb.Run)
	if err != nil {
		log.Err(err).Msg("Failed to run SquirrelDB")
	}

	log.Debug().Msg("SquirrelDB is stopped")
}

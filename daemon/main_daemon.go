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

package daemon

import (
	"errors"
	"os"
	"runtime/pprof"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/core/processsetup"
	"github.com/bleemeo/squirreldb/core/version"
	"github.com/bleemeo/squirreldb/logger"

	"github.com/rs/zerolog/log"
)

// daemonCommandFlags are the command-line flags of the daemon entry point (the
// default `squirreldb` / `squirreldb daemon`). Beyond the shared config flags
// that every entry point exposes, the daemon also exposes the daemon-only config
// flags (listen_address): it runs the server they configure, unlike `migrate`.
func daemonCommandFlags() []config.Flag {
	return config.DaemonConfigFlags()
}

// RunDaemon is the default `squirreldb` / `squirreldb daemon` entry point: it
// loads the configuration from args and runs SquirrelDB until a stop signal. It
// returns a process exit code.
func RunDaemon(args []string) int {
	_, cfg, warnings, err := config.LoadConfigFromFlags("daemon", daemonCommandFlags(), args)
	if errors.Is(err, config.ErrNormalExit) {
		return 0
	}

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse flags or load configuration")
	}

	if warnings != nil {
		log.Warn().Err(warnings).Msg("Got warnings while loading config")
	}

	if sentryCloser := processsetup.SetupSentryLogger(cfg); sentryCloser != nil {
		defer sentryCloser.Close()
	}

	defer logger.ProcessPanic()

	squirreldb := &SquirrelDB{
		Config: cfg,
		Logger: log.With().Str("component", "daemon").Logger(),
	}

	log.Info().Msgf("Starting SquirrelDB %s (commit %s)", version.Version, version.Commit)

	defer func() {
		log.Debug().Msg("SquirrelDB is stopped")

		if cfg.Internal.ShowGoroutingOnShutdown {
			_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		}
	}()

	if err := processsetup.RunWithSignalHandler(squirreldb.Run); err != nil {
		log.Err(err).Msg("Failed to run SquirrelDB")

		return 1
	}

	return 0
}

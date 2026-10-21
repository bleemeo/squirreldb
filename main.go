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

package main

import (
	"os"
	"strings"

	"github.com/bleemeo/squirreldb/api/auth"
	versionPkg "github.com/bleemeo/squirreldb/core/version"
	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/migration"

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

func main() {
	versionPkg.Version = version
	versionPkg.Commit = commit
	versionPkg.Date = date

	// Change the default time format of zerolog to allow millisecond precision.
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs

	// Initialize the logger temporarily before loading the config.
	log.Logger = logger.NewLogger(logger.NewConsoleWriter(false), zerolog.TraceLevel)

	// Subcommand dispatch: `squirreldb daemon`, `squirreldb migrate` and
	// `squirreldb auth` are handled identically — each gets the args that follow
	// it. A bare `squirreldb` (or a first argument that is a flag) is treated as `daemon`.
	args := os.Args[1:]
	subcommand := "daemon"

	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		subcommand, args = args[0], args[1:]
	}

	switch subcommand {
	case "daemon":
		os.Exit(daemon.RunDaemon(args))
	case "migrate":
		os.Exit(migration.RunMigrate(args))
	case "auth":
		os.Exit(auth.RunCommand(args))
	default:
		log.Error().Msgf("Unknown subcommand %q (expected \"daemon\", \"migrate\" or \"auth\")", subcommand)
		os.Exit(2)
	}
}

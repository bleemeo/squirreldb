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

package config

import (
	"errors"
	goflag "flag"
	"fmt"
	"path"
	"runtime"

	"github.com/bleemeo/squirreldb/core/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
)

// ErrNormalExit is returned when process should exit normal (return code 0).
var ErrNormalExit = errors.New("normal exit")

// ErrUnexpectedArgument is returned when the command line contains a positional argument.
var ErrUnexpectedArgument = errors.New("unexpected argument")

// LoadConfigFromFlags will parse flags and load the configuration.
// commandFlags is the sub-command specific flags (daemon, migrate, ...) and will
// be available in the returned FlagSet. Some common flags (see CommonFlags) are
// always added and processed; some of them stop the process normally (e.g.
// --version, --help, ...), in which case ErrNormalExit is returned.
// The two errors are warnings (from configuration loading) and error.
//
//nolint:forbidigo // This function is allowed to use fmt.Print* for CLI output.
func LoadConfigFromFlags(
	subcommandName string,
	commandFlags []Flag,
	args []string,
) (*pflag.FlagSet, Config, error, error) {
	allFlags := append(CommonFlags(), commandFlags...)
	// Only the config flags shared by every entry point are exposed here; a
	// command that needs the daemon-only ones (listen_address, admin_listen_address)
	// adds them via its commandFlags. The full binding still comes from
	// ConfigFlags() in Load.
	allFlags = append(allFlags, SharedConfigFlags()...)
	flagSet := NewFlagSet("squirreldb "+subcommandName, allFlags)

	// Add "Go" flags that are used in tests.
	flagSet.AddGoFlagSet(goflag.CommandLine)

	if err := flagSet.Parse(args); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return flagSet, Config{}, nil, ErrNormalExit
		}

		return flagSet, Config{}, nil, err
	}

	// Process common flags
	if showHelp, _ := flagSet.GetBool("help"); showHelp {
		flagSet.PrintDefaults()

		return flagSet, Config{}, nil, ErrNormalExit
	}

	if showVersion, _ := flagSet.GetBool("version"); showVersion {
		fmt.Printf("Version %s\n", version.Version)

		return flagSet, Config{}, nil, ErrNormalExit
	}

	if showBuildInfo, _ := flagSet.GetBool("build-info"); showBuildInfo {
		fmt.Printf("Built at %s using %s from commit %s\n", version.Date, runtime.Version(), version.Commit)
		fmt.Printf("Version %s\n", version.Version)

		return flagSet, Config{}, nil, ErrNormalExit
	}

	// The subcommand name must come first (see main.go); any other positional
	// argument, e.g. a subcommand placed after a flag, would otherwise be
	// silently ignored and the default subcommand would run instead.
	if flagSet.NArg() > 0 {
		return flagSet, Config{}, nil, fmt.Errorf(
			"%w %q (a subcommand must be the first argument)", ErrUnexpectedArgument, flagSet.Arg(0),
		)
	}

	configFiles, _ := flagSet.GetStringSlice("config")

	cfg, warnings, err := LoadConfig(flagSet, configFiles)

	return flagSet, cfg, errors.Join(warnings...), err
}

// LoadConfig loads the configuration from the given files (plus environment and
// the config flags set in the already parsed flagSet) and validates it. It is
// shared by the daemon and the migrate subcommand so both load config identically.
func LoadConfig(flagSet *pflag.FlagSet, configFiles []string) (Config, Warnings, error) {
	cfg, warnings, err := Load(flagSet, configFiles...)
	if err != nil {
		return Config{}, nil, fmt.Errorf("can't load config: %w", err)
	}

	if err := validateConfig(cfg); err != nil {
		return cfg, nil, err
	}

	return cfg, warnings, nil
}

// validateConfig checks if the configuration is valid and consistent.
func validateConfig(cfg Config) error {
	var warnings prometheus.MultiError

	if cfg.Cassandra.Keyspace == "" {
		warnings.Append(fmt.Errorf("%w: 'cassandra.keyspace' must be set", ErrInvalidValue))
	}

	if cfg.Cassandra.ReplicationFactor <= 0 {
		err := fmt.Errorf("%w: 'cassandra.replication_factor' must be strictly greater than 0", ErrInvalidValue)
		warnings.Append(err)
	}

	if cfg.Batch.Size <= 0 {
		warnings.Append(fmt.Errorf("%w: 'batch.size' must be strictly greater than 0", ErrInvalidValue))
	}

	if cfg.Cassandra.Aggregate.IntendedDuration <= 0 {
		err := fmt.Errorf(
			"%w: 'cassandra.aggregate.intended_duration' must be strictly greater than 0",
			ErrInvalidValue,
		)
		warnings.Append(err)
	}

	if cfg.AdminListenAddress != "" && cfg.AdminListenAddress == cfg.ListenAddress {
		warnings.Append(fmt.Errorf(
			"%w: 'admin_listen_address' (%q) must differ from 'listen_address' "+
				"(leave it empty to serve the admin endpoints on 'listen_address')",
			ErrInvalidValue, cfg.AdminListenAddress,
		))
	}

	for _, err := range validateSecurity(cfg) {
		warnings.Append(err)
	}

	if cfg.ExtraWriteBackend != "" {
		switch {
		case cfg.ExtraWriteBackend == BackendSandbox:
			warnings.Append(fmt.Errorf(
				"%w: backend %q can't be used as 'extra_write_backend': it discards every write",
				ErrInvalidValue, cfg.ExtraWriteBackend,
			))
		case !isKnownBackend(cfg.ExtraWriteBackend):
			warnings.Append(fmt.Errorf(
				"%w: unknown backend %q in 'extra_write_backend'", ErrInvalidValue, cfg.ExtraWriteBackend,
			))
		}

		if cfg.ExtraWriteBackend == cfg.Backend {
			warnings.Append(fmt.Errorf(
				"%w: 'extra_write_backend' (%q) must differ from 'backend' "+
					"(which is already the primary, must-succeed write target)",
				ErrInvalidValue, cfg.ExtraWriteBackend,
			))
		}
	}

	for _, err := range validateRouting(cfg) {
		warnings.Append(err)
	}

	return warnings.MaybeUnwrap()
}

// isKnownBackend tells whether name is the name of a backend SquirrelDB can build.
func isKnownBackend(name string) bool {
	switch name {
	case BackendCassandra, BackendClickHouse, BackendDummy, BackendSandbox:
		return true
	default:
		return false
	}
}

// validateRouting checks the "routing" section. The default backend is always
// selectable, so listing it in 'routing.backends' is rejected as a mistake. The
// extra write backend is rejected too: a backend instance is either the
// dual-write target of the default route or a routed backend, never both.
func validateRouting(cfg Config) []error {
	var errs []error

	selectable := map[string]bool{cfg.Backend: true}

	for _, name := range cfg.Routing.Backends {
		switch {
		case !isKnownBackend(name):
			errs = append(errs, fmt.Errorf("%w: unknown backend %q in 'routing.backends'", ErrInvalidValue, name))
		case name == cfg.Backend:
			errs = append(errs, fmt.Errorf(
				"%w: 'routing.backends' must not contain the default backend %q (it is always selectable)",
				ErrInvalidValue, name,
			))
		case name == cfg.ExtraWriteBackend:
			errs = append(errs, fmt.Errorf(
				"%w: 'routing.backends' must not contain the 'extra_write_backend' %q",
				ErrInvalidValue, name,
			))
		case selectable[name]:
			errs = append(errs, fmt.Errorf("%w: backend %q is listed twice in 'routing.backends'", ErrInvalidValue, name))
		}

		selectable[name] = true
	}

	for i, rule := range cfg.Routing.Rules {
		if len(rule.Tenants) == 0 {
			errs = append(errs, fmt.Errorf("%w: 'routing.rules[%d].tenants' must not be empty", ErrInvalidValue, i))
		}

		for _, pattern := range rule.Tenants {
			if _, err := path.Match(pattern, ""); err != nil {
				errs = append(errs, fmt.Errorf(
					"%w: invalid pattern %q in 'routing.rules[%d].tenants': %w", ErrInvalidValue, pattern, i, err,
				))
			}
		}

		switch {
		case !isKnownBackend(rule.Backend):
			errs = append(errs, fmt.Errorf(
				"%w: unknown backend %q in 'routing.rules[%d].backend'", ErrInvalidValue, rule.Backend, i,
			))
		case !selectable[rule.Backend]:
			errs = append(errs, fmt.Errorf(
				"%w: 'routing.rules[%d].backend' %q must be the default backend or listed in 'routing.backends'",
				ErrInvalidValue, i, rule.Backend,
			))
		}
	}

	return errs
}

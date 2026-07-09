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
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
)

// Flag describes one command-line flag. Command flags (help, version, config
// files, subcommand-specific options) are defined by each entry point (the
// daemon, the migrate subcommand, the test tools); the config flags below are
// shared. Both are turned into a *pflag.FlagSet with NewFlagSet.
type Flag struct {
	Name   string
	Short  string
	Value  any
	Usage  string
	Hidden bool
}

// ConfigFlags returns the flags that override configuration keys on the command
// line. They are shared by every entry point and applied on top of files and
// environment by Load.
func ConfigFlags() []Flag {
	defaultCfg := DefaultConfig()

	return []Flag{
		{
			Name:  "cassandra.addresses",
			Value: defaultCfg.Cassandra.Addresses,
			Usage: "Set the Cassandra cluster addresses",
		},
		{
			Name:  "redis.addresses",
			Value: defaultCfg.Redis.Addresses,
			Usage: "Set the Redis addresses",
		},
		{
			Name:  "listen_address",
			Value: defaultCfg.ListenAddress,
			Usage: "Set the Prometheus API listen address",
		},
	}
}

// NewFlagSet builds a pflag.FlagSet (ContinueOnError) from the given flags.
func NewFlagSet(name string, flags []Flag) *pflag.FlagSet {
	flagSet := pflag.NewFlagSet(name, pflag.ContinueOnError)

	for _, f := range flags {
		switch value := f.Value.(type) {
		case bool:
			flagSet.BoolP(f.Name, f.Short, value, f.Usage)
		case float64:
			flagSet.Float64P(f.Name, f.Short, value, f.Usage)
		case int:
			flagSet.IntP(f.Name, f.Short, value, f.Usage)
		case string:
			flagSet.StringP(f.Name, f.Short, value, f.Usage)
		case []string:
			flagSet.StringSliceP(f.Name, f.Short, value, f.Usage)
		default:
			log.Fatal().Msgf(`Flag "%s" has unsupported type %T`, f.Name, value)
		}

		if f.Hidden {
			_ = flagSet.MarkHidden(f.Name)
		}
	}

	return flagSet
}

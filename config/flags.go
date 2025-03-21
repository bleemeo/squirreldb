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

package config

import (
	goflag "flag"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
)

type flag struct {
	name   string
	short  string
	value  any
	usage  string
	hidden bool
}

func commandFlags() []flag {
	return []flag{
		{
			name:  "help",
			short: "h",
			value: false,
			usage: "Display help",
		},
		{
			name:  "version",
			short: "v",
			value: false,
			usage: "Show version and exit",
		},
		{
			name:  "build-info",
			short: "",
			value: false,
			usage: "Show build-info and exit",
		},
		{
			name:  "config",
			short: "c",
			value: defaultPaths(),
			usage: "Files and folders from which to load the configuration",
		},
	}
}

func configFlags() []flag {
	defaultCfg := DefaultConfig()

	return []flag{
		{
			name:  "cassandra.addresses",
			short: "",
			value: defaultCfg.Cassandra.Addresses,
			usage: "Set the Cassandra cluster addresses",
		},
		{
			name:  "redis.addresses",
			short: "",
			value: defaultCfg.Redis.Addresses,
			usage: "Set the Redis addresses",
		},
		{
			name:  "listen_address",
			short: "",
			value: defaultCfg.ListenAddress,
			usage: "Set the Prometheus API listen address",
		},
	}
}

// ParseFlags returns the parsed flags.
func ParseFlags() (*pflag.FlagSet, error) {
	flagSet := flagSetFromFlags(append(commandFlags(), configFlags()...))

	// Add flags used in tests.
	flagSet.AddGoFlagSet(goflag.CommandLine)

	if err := flagSet.Parse(os.Args); err != nil {
		return nil, fmt.Errorf("parse command line arguments: %w", err)
	}

	return flagSet, nil
}

// Returns a flag set generated from a flag list.
func flagSetFromFlags(flags []flag) *pflag.FlagSet {
	flagSet := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

	for _, flag := range flags {
		switch value := flag.value.(type) {
		case bool:
			flagSet.BoolP(flag.name, flag.short, value, flag.usage)
		case float64:
			flagSet.Float64P(flag.name, flag.short, value, flag.usage)
		case int:
			flagSet.IntP(flag.name, flag.short, value, flag.usage)
		case string:
			flagSet.StringP(flag.name, flag.short, value, flag.usage)
		case []string:
			flagSet.StringSliceP(flag.name, flag.short, value, flag.usage)
		default:
			log.Fatal().Msgf(`Flag "%s" has unsupported type %T`, flag.name, value)
		}

		if flag.hidden {
			_ = flagSet.MarkHidden(flag.name)
		}
	}

	return flagSet
}

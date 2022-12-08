package config2

import (
	goflag "flag"
	"fmt"
	"os"

	"github.com/spf13/pflag"
)

type flag struct {
	name   string
	short  string
	value  interface{}
	usage  string
	hidden bool
}

//nolint:gochecknoglobals
func flags(defaultCfg Config) []flag {
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
			name:  "internal.disable_background_task",
			short: "",
			value: false,
			usage: "Debug option that disable (some) background tasks like index metric expiration",
		},
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
	flagSet := flagSetFromFlags(flags(DefaultConfig()))

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
		}

		if flag.hidden {
			_ = flagSet.MarkHidden(flag.name)
		}
	}

	flagSet.AddGoFlagSet(goflag.CommandLine)

	return flagSet
}

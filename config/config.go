package config

import (
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/spf13/pflag"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type flag struct {
	name   string
	short  string
	value  interface{}
	usage  string
	hidden bool
}

type Config struct {
	*koanf.Koanf
	FlagSet *pflag.FlagSet
}

// New creates a new Config object
func New() (*Config, error) {
	config := Config{
		Koanf: koanf.New(configDelimiter),
	}

	configPaths, err := findConfigPaths(configFolderRoot, configFileExtensions)

	if err != nil {
		return nil, err
	}

	for _, path := range configPaths {
		err = config.Load(file.Provider(path), yaml.Parser())

		if err != nil {
			return nil, err
		}
	}

	// Initializes environment support
	err = config.Load(env.Provider(configEnvPrefix, configDelimiter, func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, configEnvPrefix)), "_", configDelimiter)
	}), nil)

	if err != nil {
		return nil, err
	}

	// Initializes flags support
	config.FlagSet = flagsToFlagSet(configFlags)

	if err := config.FlagSet.Parse(os.Args); err != nil {
		return nil, err
	}

	err = config.Load(posflag.Provider(config.FlagSet, configDelimiter, config.Koanf), nil)

	if err != nil {
		return nil, err
	}

	return &config, nil
}

// Returns every config files in the root folder matching with the specified extensions
func findConfigPaths(root string, extensions []string) ([]string, error) {
	var configPaths []string

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		for _, extension := range extensions {
			if filepath.Ext(path) == extension {
				configPaths = append(configPaths, path)
			}
		}

		return err
	})

	if err != nil {
		return nil, err
	}

	sort.Strings(configPaths)

	return configPaths, nil
}

// Convert flags to spf13 FlagSet
func flagsToFlagSet(flags []flag) *pflag.FlagSet {
	flagSet := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

	for _, flag := range flags {
		switch flag.value.(type) {
		case bool:
			value, _ := flag.value.(bool)
			flagSet.BoolP(flag.name, flag.short, value, flag.usage)
		case float64:
			value, _ := flag.value.(float64)
			flagSet.Float64P(flag.name, flag.short, value, flag.usage)
		case int:
			value, _ := flag.value.(int)
			flagSet.IntP(flag.name, flag.short, value, flag.usage)
		case string:
			value, _ := flag.value.(string)
			flagSet.StringP(flag.name, flag.short, value, flag.usage)
		case []string:
			value, _ := flag.value.([]string)
			flagSet.StringSliceP(flag.name, flag.short, value, flag.usage)
		}
		if flag.hidden {
			_ = flagSet.MarkHidden(flag.name)
		}
	}

	return flagSet
}

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

var C *Config

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

func NewConfig() *Config {
	return &Config{
		Koanf: koanf.New(configDelimiter),
	}
}

func (c *Config) Setup() error {
	configPaths, err := findConfigPaths(configFolderRoot)

	if err != nil {
		return nil
	}

	c.FlagSet = toFlagSet(flags)

	if err := c.FlagSet.Parse(os.Args); err != nil {
		return err
	}

	for _, path := range configPaths {
		err = c.Load(file.Provider(path), yaml.Parser())

		if err != nil {
			return err
		}
	}

	err = c.Load(env.Provider(configEnvPrefix, configDelimiter, func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, configEnvPrefix)), "_", configDelimiter)
	}), nil)

	if err != nil {
		return err
	}

	err = c.Load(posflag.Provider(c.FlagSet, configDelimiter, c.Koanf), nil)

	if err != nil {
		return err
	}

	return nil
}

func toFlagSet(flags []flag) *pflag.FlagSet {
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

func findConfigPaths(root string) ([]string, error) {
	var configPaths []string

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		for _, fileExtension := range configFileExtensions {
			if filepath.Ext(path) == fileExtension {
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

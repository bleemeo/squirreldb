package config

import (
	"github.com/knadh/koanf/providers/posflag"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/spf13/pflag"
)

const (
	configRoot = "./"
	envPrefix  = "SQUIRRELDB_"
	delimiter  = "."
)

var C *Config

var flags = []value{
	{
		name:  "help",
		short: "h",
		value: false,
		usage: "Display help",
	},
	{
		name:  "prometheus.address",
		short: "",
		value: "localhost:1800",
		usage: "Set the Prometheus remote storage server address",
	},
	{
		name:  "cassandra.addresses",
		short: "",
		value: []string{"localhost:9042"},
		usage: "Set the Cassandra cluster addresses",
	},
	{
		name:   "test",
		short:  "",
		value:  false,
		usage:  "",
		hidden: true,
	},
}

type value struct {
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
		Koanf: koanf.New(delimiter),
	}
}

func (c *Config) Setup() error {
	var err error
	c.FlagSet, err = initFlags()

	if err != nil {
		return err
	}

	configPaths, err := getConfigPaths(configRoot)

	if err != nil {
		return nil
	}

	for _, path := range configPaths {
		if err := c.Load(file.Provider(path), yaml.Parser()); err != nil {
			return err
		}
	}

	err = c.Load(env.Provider(envPrefix, delimiter, func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, envPrefix)), "_", delimiter)
	}), nil)

	if err != nil {
		return err
	}

	err = c.Load(posflag.Provider(c.FlagSet, delimiter, c.Koanf), nil)

	if err != nil {
		return err
	}

	return nil
}

func initFlags() (*pflag.FlagSet, error) {
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

	if err := flagSet.Parse(os.Args); err != nil {
		return nil, err
	}

	return flagSet, nil
}

func getConfigPaths(root string) ([]string, error) {
	var configPaths []string

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".conf" {
			configPaths = append(configPaths, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	sort.Strings(configPaths)

	return configPaths, nil
}

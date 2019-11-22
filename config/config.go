package config

import (
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/spf13/pflag"

	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	delimiter  = "."
	envPrefix  = "SQUIRRELDB_"
	folderRoot = "."
)

//nolint: gochecknoglobals
var fileExtensions = []string{".conf", ".yaml", ".yml"}

type Config struct {
	*koanf.Koanf
	FlagSet *pflag.FlagSet
}

// New creates a new Config object
func New() (*Config, error) {
	instance := koanf.New(delimiter)

	err := instance.Load(confmap.Provider(defaults, delimiter), nil)

	if err != nil {
		return nil, err
	}

	filePaths, err := filePaths(folderRoot, fileExtensions)

	if err != nil {
		return nil, err
	}

	for _, filePath := range filePaths {
		err = instance.Load(file.Provider(filePath), yaml.Parser())

		if err != nil {
			return nil, err
		}
	}

	err = instance.Load(env.Provider(envPrefix, delimiter, func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, envPrefix)), "_", delimiter)
	}), nil)

	if err != nil {
		return nil, err
	}

	flagSet := flagSetFromFlags(flags)

	if err := flagSet.Parse(os.Args); err != nil {
		return nil, err
	}

	err = instance.Load(posflag.Provider(flagSet, delimiter, instance), nil)

	if err != nil {
		return nil, err
	}

	config := &Config{
		Koanf:   instance,
		FlagSet: flagSet,
	}

	return config, nil
}

// Return file paths
func filePaths(root string, fileExtensions []string) ([]string, error) {
	filesInfo, err := ioutil.ReadDir(root)

	if err != nil {
		return nil, err
	}

	var filePaths []string

filesLoop:
	for _, fileInfo := range filesInfo {
		path := filepath.Join(root, fileInfo.Name())

		for _, fileExtension := range fileExtensions {
			if filepath.Ext(path) == fileExtension {
				filePaths = append(filePaths, path)

				continue filesLoop
			}
		}
	}

	sort.Strings(filePaths)

	return filePaths, nil
}

// Returns a flag set generated from a flag list
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

	return flagSet
}

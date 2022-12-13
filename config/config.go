package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/imdario/mergo"
	"github.com/knadh/koanf"
	yamlParser "github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/providers/structs"
	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

const (
	envPrefix = "SQUIRRELDB_"
	delimiter = "."
)

var (
	errWrongMapFormat = errors.New("could not parse map from string")
	ErrInvalidValue   = errors.New("invalid config value")
)

type Warnings error

// Load loads the configuration from files and directories to a struct.
// Returns the config, warnings and an error.
func Load(withDefaultAndFlags bool, paths ...string) (Config, Warnings, error) {
	// If no config was given with flags or env variables, fallback on the default files.
	if len(paths) == 0 || len(paths) == 1 && paths[0] == "" {
		paths = DefaultPaths()
	}

	cfg, warnings, err := loadToStruct(withDefaultAndFlags, os.Args, paths...)

	return cfg, warnings.MaybeUnwrap(), err
}

func loadToStruct(withDefaultAndFlags bool, args []string, paths ...string) (Config, prometheus.MultiError, error) {
	k, warnings, err := load(withDefaultAndFlags, args, paths...)

	var config Config

	unmarshalConf := koanf.UnmarshalConf{
		DecoderConfig: &mapstructure.DecoderConfig{
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToSliceHookFunc(","),
				mapstructure.TextUnmarshallerHookFunc(),
				mapstructure.StringToTimeDurationHookFunc(),
				intToTimeDurationHookFunc(),
				stringToMapHookFunc(),
			),
			Metadata:         nil,
			ErrorUnused:      true,
			Result:           &config,
			WeaklyTypedInput: true,
		},
		Tag: "yaml",
	}

	warning := k.UnmarshalWithConf("", &config, unmarshalConf)
	warnings.Append(warning)

	return config, unwrapErrors(warnings), err
}

// load the configuration from files and directories.
func load(withDefaultAndFlags bool, args []string, paths ...string) (*koanf.Koanf, prometheus.MultiError, error) {
	fileEnvKoanf, warnings, errors := loadPaths(paths)

	// Load config from environment variables.
	// The warnings are filled only after k.Load is called.
	envToKey, envWarnings := envToKeyFunc()

	// Environment variable overwrite basic types (string, int), arrays, and maps.
	envMergeFunc := mergeFunc(mergo.WithOverride)

	warning := fileEnvKoanf.Load(env.Provider(envPrefix, delimiter, envToKey), nil, envMergeFunc)
	warnings.Append(warning)

	if len(*envWarnings) > 0 {
		warnings = append(warnings, *envWarnings...)
	}

	// Load default values.
	k := koanf.New(delimiter)

	if withDefaultAndFlags {
		warning = k.Load(structsProvider(DefaultConfig(), "yaml"), nil)
		warnings.Append(warning)
	}

	// Merge defaults and config from files and environment.
	// The config overwrites the defaults for basic types (string, int) and arrays, and merges maps.
	warning = k.Load(confmap.Provider(fileEnvKoanf.All(), delimiter), nil, mergeFunc(mergo.WithOverride))
	warnings.Append(warning)

	if withDefaultAndFlags {
		// Parse the config flags without the command flags because the command flags would
		// create warnings about invalid keys when the config is converted to a struct.
		flagSet := flagSetFromFlags(configFlags())

		// The error can be safely ignored as the flags were already parsed in the daemon.
		_ = flagSet.Parse(args)

		// Overwrite the config with the command line args set explicitly.
		warning = k.Load(posflag.Provider(flagSet, delimiter, k), nil)
		warnings.Append(warning)
	}

	return k, warnings, errors.MaybeUnwrap()
}

// envToKeyFunc returns a function that converts an environment variable to a configuration key
// and a pointer to Warnings, the warnings are filled only after koanf.Load has been called.
// Panics if two config keys correspond to the same environment variable.
func envToKeyFunc() (func(string) string, *prometheus.MultiError) {
	// Get all config keys from an empty config.
	k := koanf.New(delimiter)
	_ = k.Load(structs.Provider(Config{}, "yaml"), nil)
	allKeys := k.All()

	// Build a map of the environment variables with their corresponding config keys.
	envToKey := make(map[string]string, len(allKeys))

	for key := range allKeys {
		envKey := toEnvKey(key)

		if oldKey, exists := envToKey[envKey]; exists {
			err := fmt.Sprintf(
				"Conflict between config keys, %s and %s both corresponds to the variable %s",
				oldKey, key, envKey,
			)
			panic(err)
		}

		envToKey[envKey] = key
	}

	warnings := make(prometheus.MultiError, 0)
	envFunc := func(s string) string {
		return envToKey[s]
	}

	return envFunc, &warnings
}

// mergeFunc return a merge function to use with koanf.
func mergeFunc(opts ...func(*mergo.Config)) koanf.Option {
	merge := func(src, dest map[string]interface{}) error {
		err := mergo.Merge(&dest, src, opts...)
		if err != nil {
			log.Err(err).Msg("Failed to merge config")
		}

		return err
	}

	return koanf.WithMergeFunc(merge)
}

// toEnvKey returns the environment variable corresponding to a configuration key.
// For instance: toEnvKey("web.enable") -> GLOUTON_WEB_ENABLE.
func toEnvKey(key string) string {
	envKey := strings.ToUpper(key)
	envKey = envPrefix + strings.ReplaceAll(envKey, ".", "_")

	return envKey
}

// loadPaths returns the config loaded from the given paths, warnings and errors.
func loadPaths(paths []string) (*koanf.Koanf, prometheus.MultiError, prometheus.MultiError) {
	var warnings, errors prometheus.MultiError

	k := koanf.New(delimiter)

	for _, path := range paths {
		stat, err := os.Stat(path)
		if err != nil && os.IsNotExist(err) {
			// Ignore missing config files.
			continue
		}

		if err != nil {
			errors.Append(err)

			continue
		}

		if stat.IsDir() {
			moreWarnings, err := loadDirectory(k, path)
			errors.Append(err)

			if moreWarnings != nil {
				warnings = append(warnings, moreWarnings...)
			}
		} else {
			warning := loadFile(k, path)
			warnings.Append(warning)
		}
	}

	return k, warnings, errors
}

func loadDirectory(k *koanf.Koanf, dirPath string) (prometheus.MultiError, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var warnings prometheus.MultiError

	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".conf") {
			continue
		}

		path := filepath.Join(dirPath, f.Name())

		warning := loadFile(k, path)
		warnings.Append(warning)
	}

	return warnings, nil
}

func loadFile(k *koanf.Koanf, path string) error {
	// Merge this file with the previous config.
	// Overwrite values, merge maps and append slices.
	err := k.Load(file.Provider(path), yamlParser.Parser(), mergeFunc(mergo.WithOverride, mergo.WithAppendSlice))
	if err != nil {
		return fmt.Errorf("failed to load '%s': %w", path, err)
	}

	return nil
}

// unwrapErrors unwrap all errors in the list than contain multiple errors.
func unwrapErrors(errs prometheus.MultiError) prometheus.MultiError {
	if len(errs) == 0 {
		return nil
	}

	unwrapped := make(prometheus.MultiError, 0, len(errs))

	for _, err := range errs {
		var (
			mapErr  *mapstructure.Error
			yamlErr *yaml.TypeError
		)

		switch {
		case errors.As(err, &mapErr):
			unwrapped = append(unwrapped, mapErr.WrappedErrors()...)
		case errors.As(err, &yamlErr):
			for _, wrappedErr := range yamlErr.Errors {
				unwrapped.Append(errors.New(wrappedErr)) //nolint:goerr113
			}
		default:
			unwrapped.Append(err)
		}
	}

	return unwrapped
}

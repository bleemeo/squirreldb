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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"dario.cat/mergo"
	"github.com/go-viper/mapstructure/v2"
	yamlParser "github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
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
	ErrZeroTTL        = errors.New(
		"a TTL of zero or less drops points immediately; use a large value like " +
			"876000h (~100 years) for near-unlimited retention",
	)
)

// deprecatedKeys maps a deprecated configuration key to the key that replaces it.
// A value set under a deprecated key is moved to the new key (see migrateDeprecatedKeys),
// so old configurations keep working while emitting a warning.
var deprecatedKeys = map[string]string{ //nolint:gochecknoglobals
	"cassandra.default_time_to_live": "default_time_to_live",
}

type Warnings []error

// Load loads the configuration from files and directories to a struct.
// Returns the config, warnings and an error.
func Load(paths ...string) (Config, Warnings, error) {
	cfg, warnings, err := loadToStruct(true, os.Args, paths...)
	if err != nil {
		return cfg, warnings, err
	}

	if err := sanityCheck(cfg); err != nil {
		return cfg, warnings, err
	}

	return cfg, warnings, nil
}

func loadToStruct(withDefaultAndFlags bool, args []string, paths ...string) (Config, Warnings, error) {
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

	if err != nil {
		return config, unwrapErrors(warnings), err
	}

	if err := checkSecuritySections(k, unmarshalConf); err != nil {
		return config, unwrapErrors(warnings), err
	}

	return config, unwrapErrors(warnings), nil
}

// checkSecuritySections decodes the security sections alone, into a throwaway
// struct, and returns an error if they contain an unknown key or an invalid value.
//
// The decoder doesn't report the unused keys in Metadata when ErrorUnused fails,
// hence this second decode with ErrorUnused disabled. It is limited to the security
// sections because the decoder also skips Metadata for every struct holding a
// decode error: an invalid value elsewhere (e.g. "log.level: abc") must not hide
// a typo in "auth". An invalid value inside a security section is fatal too, as
// the decoder then can't list its unknown keys, and a value that can't be decoded
// is left to its zero value, which can disable a restriction as well.
func checkSecuritySections(k *koanf.Koanf, unmarshalConf koanf.UnmarshalConf) error {
	raw := k.Raw()
	sections := make(map[string]any, len(securitySections))

	for _, section := range securitySections {
		if value, ok := raw[section]; ok {
			sections[section] = value
		}
	}

	if len(sections) == 0 {
		return nil
	}

	var (
		discarded Config
		metadata  mapstructure.Metadata
	)

	decoderConf := *unmarshalConf.DecoderConfig
	decoderConf.TagName = unmarshalConf.Tag
	decoderConf.ErrorUnused = false
	decoderConf.Metadata = &metadata
	decoderConf.Result = &discarded

	decoder, err := mapstructure.NewDecoder(&decoderConf)
	if err != nil {
		return err
	}

	if err := decoder.Decode(sections); err != nil {
		return fmt.Errorf(
			"%w: invalid value in a security section (%s): %w",
			ErrInvalidValue, strings.Join(securitySections, ", "), err,
		)
	}

	return checkSecurityUnusedKeys(metadata.Unused)
}

// securitySections are the config sections where an unknown key is an error
// instead of a warning: a typo there (e.g. "tenat" for a user's "tenant", or
// "enable" for "enabled") silently weakens the access restrictions.
var securitySections = []string{"auth", "tls"} //nolint:gochecknoglobals

// checkSecurityUnusedKeys returns an error listing the unknown keys found in the
// security sections. unused holds the full key paths reported by the decoder,
// e.g. "auth.users[0].tenat".
func checkSecurityUnusedKeys(unused []string) error {
	var invalid []string

	for _, key := range unused {
		for _, section := range securitySections {
			if key == section || strings.HasPrefix(key, section+".") || strings.HasPrefix(key, section+"[") {
				invalid = append(invalid, key)

				break
			}
		}
	}

	if len(invalid) == 0 {
		return nil
	}

	slices.Sort(invalid)

	return fmt.Errorf(
		"%w: unknown keys in a security section (%s): %s",
		ErrInvalidValue, strings.Join(securitySections, ", "), strings.Join(invalid, ", "),
	)
}

// load the configuration from files and directories.
func load(withDefaultAndFlags bool, args []string, paths ...string) (*koanf.Koanf, prometheus.MultiError, error) {
	fileEnvKoanf, warnings, errors := loadPaths(paths)

	// Load config from environment variables.
	// The warnings are filled only after k.Load is called.
	envToKey, envWarnings, unknownSecurityEnv := envToKeyFunc()

	// Environment variable overwrite basic types (string, int), arrays, and maps.
	envMergeFunc := mergeFunc(mergo.WithOverride)

	warning := fileEnvKoanf.Load(env.Provider(envPrefix, delimiter, envToKey), nil, envMergeFunc)
	warnings.Append(warning)

	if len(*envWarnings) > 0 {
		warnings = append(warnings, *envWarnings...)
	}

	// Unknown environment variables are ignored, except in the security sections
	// where a typo would silently weaken the access restrictions.
	if len(*unknownSecurityEnv) > 0 {
		slices.Sort(*unknownSecurityEnv)

		errors.Append(fmt.Errorf(
			"%w: unknown environment variables for a security section (%s): %s",
			ErrInvalidValue, strings.Join(securitySections, ", "), strings.Join(*unknownSecurityEnv, ", "),
		))
	}

	// Migrate deprecated keys before the defaults are merged. At this point fileEnvKoanf
	// only contains values explicitly set by the user (files and environment), so we can
	// tell whether the replacement key was set by the user or not.
	warnings = append(warnings, migrateDeprecatedKeys(fileEnvKoanf)...)

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
		flagSet := NewFlagSet("config", ConfigFlags())

		// The error can be safely ignored as the flags were already parsed in the daemon.
		_ = flagSet.Parse(args)

		// Overwrite the config with the command line args set explicitly.
		warning = k.Load(posflag.Provider(flagSet, delimiter, k), nil)
		warnings.Append(warning)
	}

	return k, warnings, errors.MaybeUnwrap()
}

// migrateDeprecatedKeys moves values set under a deprecated key to its replacement key and
// returns a deprecation warning for each migrated key. This must run before the deprecated key
// would otherwise be rejected by ErrorUnused when unmarshalling into the Config struct.
// If both the deprecated and the replacement keys are set, the replacement wins and the
// deprecated value is dropped.
func migrateDeprecatedKeys(k *koanf.Koanf) prometheus.MultiError {
	var warnings prometheus.MultiError

	for oldKey, newKey := range deprecatedKeys {
		if !k.Exists(oldKey) {
			continue
		}

		if k.Exists(newKey) {
			warnings.Append(fmt.Errorf( //nolint:err113
				"config key %q is deprecated and ignored because %q is also set, please remove %q",
				oldKey, newKey, oldKey,
			))
		} else {
			warnings.Append(fmt.Errorf( //nolint:err113
				"config key %q is deprecated, please use %q instead",
				oldKey, newKey,
			))

			if err := k.Set(newKey, k.Get(oldKey)); err != nil {
				warnings.Append(err)
			}
		}

		k.Delete(oldKey)
	}

	return warnings
}

// envToKeyFunc returns a function that converts an environment variable to a configuration key
// and a pointer to Warnings, the warnings are filled only after koanf.Load has been called.
// Panics if two config keys correspond to the same environment variable.
// The returned slice is filled, when the environment is loaded, with the
// variables of a security section that match no config key.
func envToKeyFunc() (func(string) string, *prometheus.MultiError, *[]string) {
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

	// Map deprecated keys too, so their environment variables are still loaded and can then
	// be migrated to their replacement key by migrateDeprecatedKeys.
	for oldKey := range deprecatedKeys {
		envToKey[toEnvKey(oldKey)] = oldKey
	}

	warnings := make(prometheus.MultiError, 0)
	unknownSecurity := make([]string, 0)
	envFunc := func(s string) string {
		key, ok := envToKey[s]
		if !ok && isSecurityEnvKey(s) {
			unknownSecurity = append(unknownSecurity, s)
		}

		return key
	}

	return envFunc, &warnings, &unknownSecurity
}

// isSecurityEnvKey reports whether an environment variable belongs to one of the
// securitySections, e.g. SQUIRRELDB_AUTH_ENABLED.
func isSecurityEnvKey(envKey string) bool {
	for _, section := range securitySections {
		if strings.HasPrefix(envKey, toEnvKey(section)+"_") {
			return true
		}
	}

	return false
}

// mergeFunc return a merge function to use with koanf.
func mergeFunc(opts ...func(*mergo.Config)) koanf.Option {
	merge := func(src, dest map[string]any) error {
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
func unwrapErrors(errs prometheus.MultiError) Warnings {
	if len(errs) == 0 {
		return nil
	}

	unwrapped := make(Warnings, 0, len(errs))

	for _, err := range errs {
		for _, subErr := range unwrapRecurse(err) {
			if yamlErr, ok := errors.AsType[*yaml.TypeError](subErr); ok {
				for _, wrappedErr := range yamlErr.Errors {
					unwrapped = append(unwrapped, errors.New(wrappedErr)) //nolint:err113
				}
			} else {
				unwrapped = append(unwrapped, subErr)
			}
		}
	}

	return unwrapped
}

func unwrapRecurse(err error) []error {
	wrapError, isWrapErr := err.(interface{ Unwrap() error })
	if isWrapErr {
		unwrappedErr := wrapError.Unwrap()
		_, unwrap := unwrappedErr.(interface{ Unwrap() error })
		_, multiUnwrap := unwrappedErr.(interface{ Unwrap() []error })
		// If err is just an fmt.wrapError without more depth, return it as-is.
		if unwrap || multiUnwrap {
			return unwrapRecurse(unwrappedErr)
		}

		return []error{err}
	}

	joinError, isJoinErr := err.(interface{ Unwrap() []error })
	if isJoinErr {
		var subErrs []error

		for _, subErr := range joinError.Unwrap() {
			subErrs = append(subErrs, unwrapRecurse(subErr)...)
		}

		return subErrs
	}

	return []error{err}
}

func sanityCheck(config Config) error {
	if config.DefaultTimeToLive <= 0 {
		return ErrZeroTTL
	}

	return nil
}

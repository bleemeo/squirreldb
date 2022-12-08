package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
)

const (
	delimiter  = "."
	envPrefix  = "SQUIRRELDB_"
	configFile = "squirreldb.conf"
)

type Config struct {
	*koanf.Koanf
	FlagSet *pflag.FlagSet
	logger  zerolog.Logger
}

// New creates a new Config object.
func New(logger zerolog.Logger) (*Config, error) {
	instance := koanf.New(delimiter)

	err := instance.Load(ConfMapProvider(defaults, delimiter), nil)
	if err != nil {
		return nil, fmt.Errorf("koanf: %w", err)
	}

	if _, err := os.Stat(configFile); err == nil {
		err = instance.Load(file.Provider(configFile), yaml.Parser())
		if err != nil {
			return nil, fmt.Errorf("read option from file %s: %w", configFile, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("fail to read %s: %w", configFile, err)
	}

	err = instance.Load(newEnvProvider(), nil)
	if err != nil {
		return nil, fmt.Errorf("read option from environment: %w", err)
	}

	config := &Config{
		Koanf:  instance,
		logger: logger,
	}

	return config, nil
}

// Duration returns the time.Duration value of a given key path. It take
// care of converting string to a Duration and if it's a numetric value convert
// it from a number of seconds.
func (c *Config) Duration(key string) time.Duration {
	value := c.Koanf.String(key)

	return string2Duration(value)
}

// string2Duration convert a string to a time.Duration
// In addition to time.ParseDuration, it also allow simple number and assume
// it value to be second.
func string2Duration(value string) time.Duration {
	result, err := time.ParseDuration(value)
	if err == nil {
		return result
	}

	resultInt, err := strconv.ParseInt(value, 10, 0)

	if err == nil {
		return time.Duration(resultInt) * time.Second
	}

	return 0
}

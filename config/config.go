package config

import (
	goflag "flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/spf13/pflag"
)

const (
	delimiter  = "."
	envPrefix  = "SQUIRRELDB_"
	folderRoot = "."
)

//nolint: gochecknoglobals
var fileExtensions = []string{".conf", ".yaml", ".yml"}

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[config] ", log.LstdFlags)

type Config struct {
	*koanf.Koanf
	FlagSet *pflag.FlagSet
}

// New creates a new Config object.
func New() (*Config, error) {
	instance := koanf.New(delimiter)

	err := instance.Load(ConfMapProvider(defaults, delimiter), nil)
	if err != nil {
		return nil, fmt.Errorf("koanf: %w", err)
	}

	filePaths, err := filePaths(folderRoot, fileExtensions)
	if err != nil {
		return nil, err
	}

	for _, filePath := range filePaths {
		err = instance.Load(file.Provider(filePath), yaml.Parser())
		if err != nil {
			return nil, fmt.Errorf("read option from file %s: %w", filePath, err)
		}
	}

	err = instance.Load(newEnvProvider(), nil)
	if err != nil {
		return nil, fmt.Errorf("read option from environment: %w", err)
	}

	flagSet := flagSetFromFlags(flags)

	if err := flagSet.Parse(os.Args); err != nil {
		return nil, fmt.Errorf("parse command line arguments: %w", err)
	}

	err = instance.Load(posflag.Provider(flagSet, delimiter, instance), nil)
	if err != nil {
		return nil, fmt.Errorf("parse command line arguments: %w", err)
	}

	config := &Config{
		Koanf:   instance,
		FlagSet: flagSet,
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

// Validate checks if the configuration is valid and consistent.
func (c *Config) Validate() bool {
	keyspace := c.String("cassandra.keyspace")
	replicationFactor := c.Int("cassandra.replication_factor")
	batchSize := c.Duration("batch.size")
	aggregateIntendedDuration := c.Duration("cassandra.aggregate.intended_duration")
	valid := true

	if keyspace == "" {
		valid = false

		logger.Println("Error: 'cassandra.keyspace' must be set")
	}

	if replicationFactor <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.replication_factor' must be strictly greater than 0")
	}

	if batchSize <= 0 {
		valid = false

		logger.Println("Error: 'batch.size' must be strictly greater than 0")
	}

	if aggregateIntendedDuration <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.aggregate.intended_duration' must be strictly greater than 0")
	}

	return valid
}

// Return file paths.
func filePaths(root string, fileExtensions []string) ([]string, error) {
	filesInfo, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("list dir %s: %w", root, err)
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

	return filePaths, nil
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

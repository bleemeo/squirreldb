package config

import (
	"github.com/gocql/gocql"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/spf13/pflag"

	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
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

	err = instance.Load(newEnvProvider(), nil)

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

// Validate checks if the configuration is valid and consistent
func (c *Config) Validate() bool {
	keyspace := c.String("cassandra.keyspace")
	replicationFactor := c.Int("cassandra.replication_factor")
	batchSize := c.Int64("batch.size")
	rawPartitionSize := c.Int64("cassandra.partition_size.raw")
	aggregateResolution := c.Int64("cassandra.aggregate.resolution")
	aggregateSize := c.Int64("cassandra.aggregate.size")
	aggregatePartitionSize := c.Int64("cassandra.partition_size.aggregate")
	aggregateIntendedDuration := c.Int64("cassandra.aggregate.intended_duration")
	valid := true

	if keyspace == "" {
		valid = false

		logger.Println("Error: 'cassandra.keyspace' is invalid (must be filled)")
	}

	if replicationFactor <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.replication_factor' is invalid (must be strictly greater than 0)")
	}

	if batchSize <= 0 {
		valid = false

		logger.Println("Error: 'batch.size' is invalid (must be strictly greater than 0)")
	}

	if rawPartitionSize <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.partition_size.raw' (must be strictly greater than 0)")
	}

	if aggregateResolution <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.aggregate.resolution' is invalid (must be strictly greater than 0)")
	}

	if aggregateSize <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.aggregate.size' is invalid (must be strictly greater than 0)")
	}

	if aggregatePartitionSize <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.partition_size.aggregate' is invalid (must be strictly greater than 0)")
	}

	if aggregateIntendedDuration <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.aggregate.intended_duration' is invalid (must be strictly greater than 0)")
	}

	if batchSize > rawPartitionSize {
		valid = false

		logger.Println("Error: 'batch.size' is invalid (must be greater than 'cassandra.partition_size.raw')")
	}

	if aggregateResolution > aggregateSize {
		valid = false

		logger.Println("Error: 'cassandra.aggregate.resolution' is invalid (must be greater than 'cassandra.aggregate.size')")
	}

	if aggregateSize > aggregatePartitionSize {
		valid = false

		logger.Println("Error: 'cassandra.aggregate.size' is invalid (must be greater than 'cassandra.partition_size.aggregate')")
	}

	return valid
}

// ValidateRemote checks if the local configuration is consistent with the remote configuration
func (c *Config) ValidateRemote(state types.State) (bool, bool) {
	names := []string{"batch.size", "cassandra.partition_size.raw", "cassandra.aggregate.resolution",
		"cassandra.aggregate.size", "cassandra.partition_size.aggregate"}
	valid := true
	exists := false

	for _, name := range names {
		local := c.String(name)

		var (
			remote string
			err    error
		)

		retry.Print(func() error {
			err = state.Read(name, &remote) // nolint: scopelint

			if err == gocql.ErrNotFound {
				return nil
			}

			exists = true
			return err
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"read config state "+name,
		)

		if exists && (local != remote) {
			valid = false

			logger.Printf("Error: '%s' current value (%s) and previous value (%s) doesn't match", name, local, remote)
		}
	}

	return valid, exists
}

// WriteRemote writes the remote constant values if they do not exist
func (c *Config) WriteRemote(state types.State, overwrite bool) {
	names := []string{"batch.size", "cassandra.partition_size.raw", "cassandra.aggregate.resolution",
		"cassandra.aggregate.size", "cassandra.partition_size.aggregate"}

	for _, name := range names {
		value := c.String(name)

		retry.Print(func() error {
			if overwrite {
				return state.Update(name, value) // nolint: scopelint
			}

			return state.Write(name, value) // nolint: scopelint
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"write config state "+name,
		)
	}
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

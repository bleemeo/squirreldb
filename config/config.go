package config

import (
	"context"
	goflag "flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"squirreldb/retry"
	"squirreldb/types"
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
	rawPartitionSize := c.Duration("cassandra.partition_size.raw")
	aggregateResolution := c.Duration("cassandra.aggregate.resolution")
	aggregateSize := c.Duration("cassandra.aggregate.size")
	aggregatePartitionSize := c.Duration("cassandra.partition_size.aggregate")
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

	if aggregateResolution <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.aggregate.resolution' must be strictly greater than 0")
	}

	if aggregateIntendedDuration <= 0 {
		valid = false

		logger.Println("Error: 'cassandra.aggregate.intended_duration' must be strictly greater than 0")
	}

	if rawPartitionSize < batchSize {
		valid = false

		logger.Printf("Error: 'cassandra.rawPartitionSize' (current value %v) must be greater than 'batch.size' (current value %v)", rawPartitionSize, batchSize)
	}

	// This is due to Cassandra storing the offset as millisecond in a int32
	// It's also due to Gorilla TSZ storing time delta as uint32
	if rawPartitionSize > 24*24*time.Hour {
		valid = false

		logger.Printf("Error: cassandra.rawPartitionSize' (current value %v) must be less than 24 days", rawPartitionSize)
	}

	if aggregateSize < aggregateResolution {
		valid = false

		logger.Printf("Error: 'cassandra.aggregate.size' (current value %v) must be greater than 'cassandra.aggregate.resolution' (current value %v)", aggregateSize, aggregateResolution)
	}

	if aggregatePartitionSize < aggregateSize {
		valid = false

		logger.Printf("Error: 'cassandra.partition_size.aggregate' (current value %v) must be greater than 'cassandra.aggregate.size' (current value %v)", aggregatePartitionSize, aggregateSize)
	}

	return valid
}

// ValidateRemote checks if the local configuration is consistent with the remote configuration.
func (c *Config) ValidateRemote(state types.State) bool {
	names := []string{
		"batch.size", "cassandra.partition_size.raw", "cassandra.aggregate.resolution",
		"cassandra.aggregate.size", "cassandra.partition_size.aggregate",
	}
	valid := true
	exists := false
	force := c.Bool("overwite-previous-config")

	for _, name := range names {
		local := c.Duration(name)

		var (
			remoteStr string
			err       error
		)

		retry.Print(func() error {
			exists, err = state.Read(name, &remoteStr) // nolint: scopelint

			return err // nolint: wrapcheck
		}, retry.NewExponentialBackOff(context.Background(), 30*time.Second), logger,
			"read config state "+name,
		)

		remote := string2Duration(remoteStr)

		if exists && (local != remote) {
			valid = false
			level := "Error"

			if force {
				level = "Warning"

				c.writeRemote(state, name, local.String())
			}

			logger.Printf("%s: option '%s' changed from \"%s\" to \"%s\" since last SquirrelDB start", level, name, remote, local)
		} else if !exists {
			c.writeRemote(state, name, local.String())
		}
	}

	if !valid {
		logger.Printf("Changing thoses values is not (yet) supported and could result in lost of previously ingested data")

		if force {
			logger.Printf("Since --overwite-previous-config is set, ignoring this warning and updating previous config")
			logger.Printf("This SquirrelDB will now use those values (if using multiple SquirrelDB instance restart the other with new config also)")

			valid = true
		} else {
			logger.Printf("If you want to ignore this error and continue with new option (potentially losing all data already ingested) use the option --overwite-previous-config")
		}
	}

	return valid
}

// writeRemote writes the remote constant value.
func (c *Config) writeRemote(state types.State, name string, value string) {
	retry.Print(func() error {
		return state.Write(name, value)
	}, retry.NewExponentialBackOff(context.Background(), 30*time.Second), logger,
		"write config state "+name,
	)
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

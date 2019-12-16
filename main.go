package main

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"

	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"squirreldb/batch"
	"squirreldb/cassandra/index"
	"squirreldb/cassandra/locks"
	"squirreldb/cassandra/session"
	"squirreldb/cassandra/states"
	"squirreldb/cassandra/tsdb"
	"squirreldb/config"
	"squirreldb/debug"
	"squirreldb/redis"
	"squirreldb/remotestorage"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"syscall"
	"time"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

func main() {
	rand.Seed(time.Now().UnixNano())

	squirrelConfig, err := config.New()

	if err != nil {
		logger.Fatalf("Error: Can't create config (%v)", err)
	}

	if !squirrelConfig.Validate() {
		logger.Fatalf("Error: Invalid configuration")
	}

	if squirrelConfig.Bool("help") {
		squirrelConfig.FlagSet.PrintDefaults()
		return
	}

	debug.Level = squirrelConfig.Int("debug.level")

	keyspace := squirrelConfig.String("cassandra.keyspace")
	squirrelSession := createSquirrelSession(keyspace, squirrelConfig)
	squirrelIndex := createSquirrelIndex(squirrelSession, keyspace)
	instance := createInstance()
	squirrelLocks := createSquirrelLocks(squirrelSession, keyspace, instance)
	squirrelStates := createSquirrelStates(squirrelSession, keyspace)
	squirrelTSDB := createSquirrelTSDB(squirrelSession, keyspace, squirrelConfig, squirrelIndex, squirrelLocks, squirrelStates)
	squirrelRedis := createSquirrelRedis(squirrelConfig)
	squirrelBatch := createSquirrelBatch(squirrelConfig, squirrelRedis, squirrelTSDB, squirrelTSDB)
	squirrelRemoteStorage := createSquirrelRemoteStorage(squirrelConfig, squirrelIndex, squirrelBatch, squirrelBatch)

	if valid, exists := squirrelConfig.ValidateRemote(squirrelStates); !valid {
		if squirrelConfig.Bool("ignore-config") {
			logger.Println("Warning: The current configuration constant values are not the same as the previous configuration constant values" + "\n" +
				"\t" + "SquirrelDB uses the current configuration")
		} else if squirrelConfig.Bool("overwrite-config") {
			squirrelConfig.WriteRemote(squirrelStates)

			logger.Println("Info: The current configuration has overwritten the previous configuration")
		} else {
			logger.Fatalln("Error: The current configuration constant values are not the same as the previous configuration constant values" + "\n" +
				"\t" + "Run SquirrelDB with the flag --ignore-config to ignore this error" + "\n" +
				"\t" + "Run SquirrelDB with the flag --overwrite-config to overwrite the previous configuration with the current configuration")
		}
	} else if !exists {
		squirrelConfig.WriteRemote(squirrelStates)
	}

	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	runSquirrelIndex := func() {
		defer wg.Done()
		squirrelIndex.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelIndex()

	runSquirrelTSDB := func() {
		defer wg.Done()
		squirrelTSDB.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelTSDB()

	runSquirrelBatch := func() {
		defer wg.Done()
		squirrelBatch.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelBatch()

	runSquirrelRemoteStorage := func() {
		defer wg.Done()
		squirrelRemoteStorage.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelRemoteStorage()

	logger.Println("SquirrelDB is ready")
	logger.Println("Instance UUID:", instance.UUID)

	<-signalChan

	logger.Println("Stopping...")

	cancel()

	waitChan := make(chan bool)

	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		logger.Println("All services have been successfully stopped")
	case <-signalChan:
		logger.Println("Force stop")
	}

	squirrelSession.Close()
	signal.Stop(signalChan)
	close(signalChan)

	logger.Println("SquirrelDB is stopped")
}

func createInstance() types.Instance {
	hostname, _ := os.Hostname()
	uuid, _ := gouuid.NewV4()

	instance := types.Instance{
		Hostname: hostname,
		UUID:     uuid.String(),
	}

	return instance
}

func createSquirrelSession(keyspace string, config *config.Config) *gocql.Session {
	options := session.Options{
		Addresses:         config.Strings("cassandra.addresses"),
		ReplicationFactor: config.Int("cassandra.replication_factor"),
		Keyspace:          keyspace,
	}

	var squirrelSession *gocql.Session

	retry.Print(func() error {
		var err error
		squirrelSession, err = session.New(options)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't create the session",
		"Resolved: Create the session")

	return squirrelSession
}

func createSquirrelIndex(session *gocql.Session, keyspace string) *index.CassandraIndex {
	var squirrelIndex *index.CassandraIndex

	retry.Print(func() error {
		var err error
		squirrelIndex, err = index.New(session, keyspace)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't create Cassandra index",
		"Resolved: Create Cassandra index")

	return squirrelIndex
}

func createSquirrelLocks(session *gocql.Session, keyspace string, instance types.Instance) *locks.CassandraLocks {
	var squirrelLocks *locks.CassandraLocks

	retry.Print(func() error {
		var err error
		squirrelLocks, err = locks.New(session, keyspace, instance)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't create Cassandra locks",
		"Resolved: Create Cassandra locks")

	return squirrelLocks
}

func createSquirrelStates(session *gocql.Session, keyspace string) *states.CassandraStates {
	var squirrelStates *states.CassandraStates

	retry.Print(func() error {
		var err error
		squirrelStates, err = states.New(session, keyspace)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't create Cassandra states",
		"Resolved: Create Cassandra states")

	return squirrelStates
}

func createSquirrelTSDB(session *gocql.Session, keyspace string, config *config.Config, indexer types.Indexer, locker types.Locker, stater types.Stater) *tsdb.CassandraTSDB {
	options := tsdb.Options{
		DefaultTimeToLive:         config.Int64("cassandra.default_time_to_live"),
		BatchSize:                 config.Int64("batch.size"),
		RawPartitionSize:          config.Int64("cassandra.partition_size.raw"),
		AggregatePartitionSize:    config.Int64("cassandra.partition_size.aggregate"),
		AggregateResolution:       config.Int64("cassandra.aggregate.resolution"),
		AggregateSize:             config.Int64("cassandra.aggregate.size"),
		AggregateIntendedDuration: config.Int64("cassandra.aggregate.intended_duration"),
	}

	var squirrelTSDB *tsdb.CassandraTSDB

	retry.Print(func() error {
		var err error
		squirrelTSDB, err = tsdb.New(session, keyspace, options, indexer, locker, stater)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't create Cassandra TSDB",
		"Resolved: Create Cassandra TSDB")

	return squirrelTSDB
}

func createSquirrelRedis(config *config.Config) *redis.Redis {
	options := redis.Options{
		Address: config.String("redis.address"),
	}

	squirrelRedis := redis.New(options)

	return squirrelRedis
}

func createSquirrelBatch(config *config.Config, storer batch.Storer, reader types.MetricReader, writer types.MetricWriter) *batch.Batch {
	squirrelBatchSize := config.Int64("batch.size")

	squirrelBatch := batch.New(squirrelBatchSize, storer, reader, writer)

	return squirrelBatch
}

func createSquirrelRemoteStorage(config *config.Config, indexer types.Indexer, reader types.MetricReader, writer types.MetricWriter) *remotestorage.RemoteStorage {
	options := remotestorage.Options{
		ListenAddress: config.String("remote_storage.listen_address"),
		WithUUID:      config.Bool("remote_storage.with_uuid"),
	}

	squirrelRemoteStorage := remotestorage.New(options, indexer, reader, writer)

	return squirrelRemoteStorage
}

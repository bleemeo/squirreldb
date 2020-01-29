package main

import (
	"github.com/gocql/gocql"

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
	"squirreldb/memorystore"
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
		logger.Fatalf("Error: Can't load config: %v", err)
	}

	if squirrelConfig.Bool("help") {
		squirrelConfig.FlagSet.PrintDefaults()
		return
	}

	if !squirrelConfig.Validate() {
		os.Exit(1)
	}

	debug.Level = squirrelConfig.Int("log.level")

	keyspace := squirrelConfig.String("cassandra.keyspace")
	squirrelSession := createSquirrelSession(keyspace, squirrelConfig)
	squirrelStates := createSquirrelStates(squirrelSession)

	if !squirrelConfig.ValidateRemote(squirrelStates) {
		os.Exit(1)
	}

	var squirrelStore batch.TemporaryStore

	redisAddress := squirrelConfig.String("redis.address")

	if redisAddress != "" {
		squirrelStore = createSquirrelRedis(redisAddress)
	} else {
		squirrelStore = memorystore.New()
	}

	squirrelIndex := createSquirrelIndex(squirrelSession, squirrelConfig)
	squirrelLocks := createSquirrelLocks(squirrelSession)
	squirrelTSDB := createSquirrelTSDB(squirrelSession, squirrelConfig, squirrelIndex, squirrelLocks, squirrelStates)
	squirrelBatch := createSquirrelBatch(squirrelConfig, squirrelStore, squirrelTSDB, squirrelTSDB)
	squirrelRemoteStorage := createSquirrelRemoteStorage(squirrelConfig, squirrelIndex, squirrelBatch)

	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	tasks := []func(context.Context){
		squirrelIndex.Run,
		squirrelTSDB.Run,
		squirrelBatch.Run,
		squirrelRemoteStorage.Run,
	}

	if redisAddress == "" {
		tasks = append(tasks, squirrelStore.(*memorystore.Store).Run)
	}

	wg.Add(len(tasks))

	for _, runner := range tasks {
		runner := runner

		go func() {
			defer wg.Done()
			runner(ctx)
		}()
	}

	logger.Println("SquirrelDB is ready")

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
		debug.Print(2, logger, "All services have been successfully stopped")
	case <-signalChan:
		logger.Println("Force stop")
	}

	squirrelSession.Close()
	signal.Stop(signalChan)
	close(signalChan)

	debug.Print(1, logger, "SquirrelDB is stopped")
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
		"create Cassandra session",
	)

	return squirrelSession
}

func createSquirrelIndex(session *gocql.Session, config *config.Config) *index.CassandraIndex {
	var squirrelIndex *index.CassandraIndex

	options := index.Options{
		DefaultTimeToLive: config.Int64("cassandra.default_time_to_live"),
		IncludeUUID:       config.Bool("index.include_uuid"),
	}

	retry.Print(func() error {
		var err error
		squirrelIndex, err = index.New(session, options)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"create Cassandra index",
	)

	return squirrelIndex
}

func createSquirrelLocks(session *gocql.Session) *locks.CassandraLocks {
	var squirrelLocks *locks.CassandraLocks

	retry.Print(func() error {
		var err error
		squirrelLocks, err = locks.New(session)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"create Cassandra locks",
	)

	return squirrelLocks
}

func createSquirrelStates(session *gocql.Session) *states.CassandraStates {
	var squirrelStates *states.CassandraStates

	retry.Print(func() error {
		var err error
		squirrelStates, err = states.New(session)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"create Cassandra state",
	)

	return squirrelStates
}

func createSquirrelTSDB(session *gocql.Session, config *config.Config, index types.Index, lockFactory *locks.CassandraLocks, state types.State) *tsdb.CassandraTSDB {
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
		squirrelTSDB, err = tsdb.New(session, options, index, lockFactory, state)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"create Cassandra TSDB",
	)

	return squirrelTSDB
}

func createSquirrelRedis(address string) *redis.Redis {
	options := redis.Options{
		Address: address,
	}

	squirrelRedis := redis.New(options)

	return squirrelRedis
}

func createSquirrelBatch(config *config.Config, store batch.TemporaryStore, reader types.MetricReader, writer types.MetricWriter) *batch.Batch {
	squirrelBatchSize := config.Int64("batch.size")

	squirrelBatch := batch.New(squirrelBatchSize, store, reader, writer)

	return squirrelBatch
}

func createSquirrelRemoteStorage(config *config.Config, index types.Index, batch *batch.Batch) *remotestorage.RemoteStorage {
	options := remotestorage.Options{
		ListenAddress: config.String("remote_storage.listen_address"),
		FlushCallback: batch.Flush,
	}

	squirrelRemoteStorage := remotestorage.New(options, index, batch, batch)

	return squirrelRemoteStorage
}

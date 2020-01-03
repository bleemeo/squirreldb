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

	var squirrelStore batch.Store

	redisEnable := squirrelConfig.Bool("redis.enable")

	if redisEnable {
		squirrelStore = createSquirrelRedis(squirrelConfig)
	} else {
		squirrelStore = memorystore.New()
	}

	keyspace := squirrelConfig.String("cassandra.keyspace")
	squirrelSession := createSquirrelSession(keyspace, squirrelConfig)
	squirrelIndex := createSquirrelIndex(squirrelSession, keyspace, squirrelConfig)
	instance := createInstance()
	squirrelLocks := createSquirrelLocks(squirrelSession, keyspace, instance)
	squirrelStates := createSquirrelStates(squirrelSession, keyspace)
	squirrelTSDB := createSquirrelTSDB(squirrelSession, keyspace, squirrelConfig, squirrelIndex, squirrelLocks, squirrelStates)
	squirrelBatch := createSquirrelBatch(squirrelConfig, squirrelStore, squirrelTSDB, squirrelTSDB)
	squirrelRemoteStorage := createSquirrelRemoteStorage(squirrelConfig, squirrelIndex, squirrelBatch, squirrelBatch)

	if valid, exists := squirrelConfig.ValidateRemote(squirrelStates); !valid {
		switch {
		case squirrelConfig.Bool("ignore-config"):
			logger.Println("Warning: The current configuration constant values are not the same as the previous configuration constant values" + "\n" +
				"\t" + "SquirrelDB uses the current configuration")
		case squirrelConfig.Bool("overwrite-config"):
			squirrelConfig.WriteRemote(squirrelStates, true)

			logger.Println("Info: The current configuration has overwritten the previous configuration")
		default:
			logger.Fatalln("Error: The current configuration constant values are not the same as the previous configuration constant values" + "\n" +
				"\t" + "Run SquirrelDB with the flag --ignore-config to ignore this error" + "\n" +
				"\t" + "Run SquirrelDB with the flag --overwrite-config to overwrite the previous configuration with the current configuration")
		}
	} else if !exists {
		squirrelConfig.WriteRemote(squirrelStates, false)
	}

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

	if !redisEnable {
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
		"create Cassandra session",
	)

	return squirrelSession
}

func createSquirrelIndex(session *gocql.Session, keyspace string, config *config.Config) *index.CassandraIndex {
	var squirrelIndex *index.CassandraIndex

	options := index.Options{
		IncludeUUID: config.Bool("index.include_uuid"),
	}

	retry.Print(func() error {
		var err error
		squirrelIndex, err = index.New(session, keyspace, options)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"create Cassandra index",
	)

	return squirrelIndex
}

func createSquirrelLocks(session *gocql.Session, keyspace string, instance types.Instance) *locks.CassandraLocks {
	var squirrelLocks *locks.CassandraLocks

	retry.Print(func() error {
		var err error
		squirrelLocks, err = locks.New(session, keyspace, instance)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"create Cassandra locks",
	)

	return squirrelLocks
}

func createSquirrelStates(session *gocql.Session, keyspace string) *states.CassandraStates {
	var squirrelStates *states.CassandraStates

	retry.Print(func() error {
		var err error
		squirrelStates, err = states.New(session, keyspace)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"create Cassandra state",
	)

	return squirrelStates
}

func createSquirrelTSDB(session *gocql.Session, keyspace string, config *config.Config, index types.Index, locker types.Locker, state types.State) *tsdb.CassandraTSDB {
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
		squirrelTSDB, err = tsdb.New(session, keyspace, options, index, locker, state)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"create Cassandra TSDB",
	)

	return squirrelTSDB
}

func createSquirrelRedis(config *config.Config) *redis.Redis {
	options := redis.Options{
		Address: config.String("redis.address"),
	}

	squirrelRedis := redis.New(options)

	return squirrelRedis
}

func createSquirrelBatch(config *config.Config, store batch.Store, reader types.MetricReader, writer types.MetricWriter) *batch.Batch {
	squirrelBatchSize := config.Int64("batch.size")

	squirrelBatch := batch.New(squirrelBatchSize, store, reader, writer)

	return squirrelBatch
}

func createSquirrelRemoteStorage(config *config.Config, index types.Index, reader types.MetricReader, writer types.MetricWriter) *remotestorage.RemoteStorage {
	options := remotestorage.Options{
		ListenAddress: config.String("remote_storage.listen_address"),
	}

	squirrelRemoteStorage := remotestorage.New(options, index, reader, writer)

	return squirrelRemoteStorage
}

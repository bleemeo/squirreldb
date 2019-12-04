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
	squirrelBatchSize := squirrelConfig.Int64("batch.size")
	squirrelBatch := batch.New(squirrelBatchSize, squirrelRedis, squirrelTSDB, squirrelTSDB)
	listenAddress := squirrelConfig.String("remote_storage.listen_address")
	squirrelRemoteStorage := remotestorage.New(listenAddress, squirrelIndex, squirrelBatch, squirrelBatch)

	squirrelConfig.WriteRemote(squirrelStates)

	if !squirrelConfig.ValidateRemote(squirrelStates) {
		if squirrelConfig.Bool("bypass-validate") {
			logger.Println("Warning: Configuration local constant values are not the same as remote constant values")
		} else {
			logger.Fatalln("Error: Configuration local constant values are not the same as remote constant values")
		}
	}

	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	runSquirrelCassandra := func() {
		defer wg.Done()
		squirrelTSDB.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelCassandra()

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

func createSquirrelTSDB(session *gocql.Session, keyspace string, config *config.Config, index *index.CassandraIndex, locks *locks.CassandraLocks, states *states.CassandraStates) *tsdb.CassandraTSDB {
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
		squirrelTSDB, err = tsdb.New(session, keyspace, options, index, locks, states)

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

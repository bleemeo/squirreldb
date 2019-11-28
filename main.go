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
	"squirreldb/prometheus"
	"squirreldb/redis"
	"squirreldb/retry"
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

	if squirrelConfig.Bool("help") {
		squirrelConfig.FlagSet.PrintDefaults()
		return
	}

	debug.Level = squirrelConfig.Int("debug.level")

	keyspace := squirrelConfig.String("cassandra.keyspace")

	squirrelSession := createSquirrelSession(keyspace, squirrelConfig)
	squirrelIndex := createSquirrelIndex(squirrelSession, keyspace)
	squirrelLocks := createSquirrelLocks(squirrelSession, keyspace)
	squirrelStates := createSquirrelStates(squirrelSession, keyspace)
	squirrelTSDB := createSquirrelTSDB(squirrelSession, keyspace, squirrelConfig, squirrelIndex, squirrelLocks, squirrelStates)
	squirrelRedis := createSquirrelRedis(squirrelConfig)
	squirrelBatchSize := squirrelConfig.Int64("batch.size")
	squirrelBatch := batch.New(squirrelBatchSize, squirrelRedis, squirrelTSDB, squirrelTSDB)
	squirrelPrometheusListenAddress := squirrelConfig.String("prometheus.listen_address")
	squirrelPrometheus := prometheus.New(squirrelPrometheusListenAddress, squirrelIndex, squirrelBatch, squirrelBatch)

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

	runSquirrelPrometheus := func() {
		defer wg.Done()
		squirrelPrometheus.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelPrometheus()

	logger.Println("SquirrelDB is ready")

	<-signalChan

	logger.Println("Stopping...")

	cancel()

	waitChan := make(chan bool)

	go func() {
		wg.Wait()
		waitChan <- true
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

func createSquirrelLocks(session *gocql.Session, keyspace string) *locks.CassandraLocks {
	var squirrelLocks *locks.CassandraLocks

	retry.Print(func() error {
		var err error
		squirrelLocks, err = locks.New(session, keyspace)

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
		DefaultTimeToLive:      config.Int64("cassandra.default_time_to_live"),
		BatchSize:              config.Int64("batch.size"),
		RawPartitionSize:       config.Int64("cassandra.partition_size.raw"),
		AggregateResolution:    config.Int64("cassandra.aggregate.resolution"),
		AggregateSize:          config.Int64("cassandra.aggregate.size"),
		AggregatePartitionSize: config.Int64("cassandra.partition_size.aggregate"),
	}
	debugOptions := tsdb.DebugOptions{
		AggregateForce: config.Bool("debug.aggregate.force"),
		AggregateSize:  config.Int64("debug.aggregate.size"),
	}

	var squirrelTSDB *tsdb.CassandraTSDB

	retry.Print(func() error {
		var err error
		squirrelTSDB, err = tsdb.New(session, keyspace, options, debugOptions, index, locks, states)

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

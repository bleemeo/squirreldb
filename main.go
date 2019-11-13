package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"squirreldb/batch"
	"squirreldb/cassandra/index"
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

var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

func main() {
	squirrelConfig, err := config.New()

	if err != nil {
		logger.Fatalf("Can't create the config (%v)", err)
	}

	// Flags handle
	if squirrelConfig.Bool("help") {
		squirrelConfig.FlagSet.PrintDefaults()
		return
	} else if squirrelConfig.Bool("test") {
		for _, key := range squirrelConfig.Keys() {
			fmt.Printf("%s:%v"+"\n", key, squirrelConfig.Get(key))
		}
		return
	}

	debug.Level = squirrelConfig.Int("debug.level")

	// Create Cassandra services
	cassandraOptions := session.Options{
		Addresses:         squirrelConfig.Strings("cassandra.addresses"),
		ReplicationFactor: squirrelConfig.Int("cassandra.replication_factor"),
		Keyspace:          squirrelConfig.String("cassandra.keyspace"),
	}

	var squirrelCassandra *session.Cassandra

	retry.Print(func() error {
		var err error
		squirrelCassandra, err = session.New(cassandraOptions)

		return err
	}, retry.NewBackOff(30*time.Second), logger,
		"Error: Can't create the session",
		"Resolved: Create the session")

	var squirrelIndex *index.CassandraIndex

	retry.Print(func() error {
		var err error
		squirrelIndex, err = index.New(squirrelCassandra.Session, cassandraOptions.Keyspace)

		return err
	}, retry.NewBackOff(30*time.Second), logger,
		"Error: Can't create the index",
		"Resolved: Create the index")

	var squirrelStates *states.CassandraStates

	retry.Print(func() error {
		var err error
		squirrelStates, err = states.New(squirrelCassandra.Session, cassandraOptions.Keyspace)

		return err
	}, retry.NewBackOff(30*time.Second), logger,
		"Error: Can't create the states",
		"Resolved: Create the states")

	tsdbOptions := tsdb.Options{
		DefaultTimeToLive:      squirrelConfig.Int64("cassandra.default_time_to_live"),
		BatchSize:              squirrelConfig.Int64("batch.size"),
		RawPartitionSize:       squirrelConfig.Int64("cassandra.partition_size.raw"),
		AggregateResolution:    squirrelConfig.Int64("cassandra.aggregate.resolution"),
		AggregateSize:          squirrelConfig.Int64("cassandra.aggregate.size"),
		AggregateStartOffset:   squirrelConfig.Int64("cassandra.aggregate.start_offset"),
		AggregatePartitionSize: squirrelConfig.Int64("cassandra.partition_size.aggregate"),
	}

	tsdbDebug := tsdb.Debug{
		AggregateForce: squirrelConfig.Bool("debug.aggregate.force"),
		AggregateSize:  squirrelConfig.Int64("debug.aggregate.size"),
	}

	var squirrelTSDB *tsdb.CassandraTSDB

	retry.Print(func() error {
		var err error
		squirrelTSDB, err = tsdb.New(squirrelCassandra.Session, cassandraOptions.Keyspace, tsdbOptions,
			tsdbDebug, squirrelIndex, squirrelStates)

		return err
	}, retry.NewBackOff(30*time.Second), logger,
		"Error: Can't create the TSDB",
		"Resolved: create the TSDB")

	redisOptions := redis.Options{
		Address: squirrelConfig.String("redis.address"),
	}

	// Create store, batch and Prometheus services
	squirrelRedis := redis.New(redisOptions)
	squirrelBatch := batch.New(tsdbOptions.BatchSize, squirrelRedis, squirrelTSDB, squirrelTSDB)
	squirrelPrometheus := prometheus.New(squirrelIndex, squirrelBatch, squirrelBatch)

	// Handle signals
	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	// Run services
	runSquirrelCassandra := func() {
		defer wg.Done()
		squirrelTSDB.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelCassandra()

	prometheusListenAddress := squirrelConfig.String("prometheus.listen_address")

	runSquirrelPrometheus := func() {
		defer wg.Done()
		squirrelPrometheus.Run(ctx, prometheusListenAddress)
	}

	wg.Add(1)

	go runSquirrelPrometheus()

	runSquirrelBatch := func() {
		defer wg.Done()
		squirrelBatch.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelBatch()

	logger.Println("SquirrelDB is ready")
	logger.Println("Listening on", prometheusListenAddress)

	// Wait to receive a stop signal
	<-signalChan

	// Stop services
	logger.Println("Stopping...")

	cancel()

	// Wait for all services
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

	// Close session and signal
	squirrelCassandra.Close()

	signal.Stop(signalChan)
	close(signalChan)

	logger.Println("SquirrelDB is stopped")
}

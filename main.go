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
	"squirreldb/prometheus"
	"squirreldb/retry"
	"squirreldb/store"
	"sync"
	"syscall"
	"time"
)

var (
	logger = log.New(os.Stdout, "[main] ", log.LstdFlags)
)

func main() {
	squirrelConfig, err := config.New()

	if err != nil {
		logger.Fatalln("config: Init: Can't initialize config (", err, ")")
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

	// Create services instance
	prometheusListenAddress := squirrelConfig.String("prometheus.listen_address")

	cassandraOptions := session.Options{
		Addresses:         squirrelConfig.Strings("cassandra.addresses"),
		ReplicationFactor: squirrelConfig.Int("cassandra.replication_factor"),
		Keyspace:          squirrelConfig.String("cassandra.keyspace"),
	}

	var squirrelCassandra *session.Cassandra

	retry.Do(func() error {
		var err error
		squirrelCassandra, err = session.NewCassandra(cassandraOptions)

		return err
	}, logger,
		"Error: Can't initialize the session",
		"Resolved: Initialized the session",
		retry.NewBackOff(30*time.Second))

	tsdbOptions := tsdb.Options{
		BatchSize:              squirrelConfig.Int64("batch.size"),
		RawPartitionSize:       squirrelConfig.Int64("cassandra.partition_size.raw"),
		AggregateResolution:    squirrelConfig.Int64("cassandra.aggregate.resolution"),
		AggregateSize:          squirrelConfig.Int64("cassandra.aggregate.size"),
		AggregateStartOffset:   squirrelConfig.Int64("cassandra.aggregate.start_offset"),
		AggregatePartitionSize: squirrelConfig.Int64("cassandra.partition_size.aggregate"),
	}

	var squirrelIndex *index.CassandraIndex

	retry.Do(func() error {
		var err error
		squirrelIndex, err = index.NewCassandraIndex(squirrelCassandra.Session, cassandraOptions.Keyspace)

		return err
	}, logger,
		"Error: Can't initialize the index",
		"Resolved: Initialized the index",
		retry.NewBackOff(30*time.Second))

	var squirrelStates *states.CassandraStates

	retry.Do(func() error {
		var err error
		squirrelStates, err = states.NewCassandraStates(squirrelCassandra.Session, cassandraOptions.Keyspace)

		return err
	}, logger,
		"Error: Can't initialize the index",
		"Resolved: Initialized the index",
		retry.NewBackOff(30*time.Second))

	var squirrelTSDB *tsdb.CassandraTSDB

	retry.Do(func() error {
		var err error
		squirrelTSDB, err = tsdb.NewCassandraTSDB(squirrelCassandra.Session, cassandraOptions.Keyspace, tsdbOptions,
			squirrelIndex, squirrelStates)

		return err
	}, logger,
		"Error: Can't initialize the TSDB",
		"Resolved: Initialized the TSDB",
		retry.NewBackOff(30*time.Second))

	squirrelStore := store.New()
	squirrelBatch := batch.New(tsdbOptions.BatchSize, squirrelStore, squirrelTSDB, squirrelTSDB)
	squirrelPrometheus := prometheus.New(squirrelIndex, squirrelBatch, squirrelBatch)

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

	runSquirrelStore := func() {
		defer wg.Done()
		squirrelStore.Run(ctx)
	}

	wg.Add(1)

	go runSquirrelStore()

	logger.Println("SquirrelDB is ready")

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

	squirrelCassandra.Close()

	signal.Stop(signalChan)
	close(signalChan)

	logger.Println("SquirrelDB is stopped")
}

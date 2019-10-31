package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"squirreldb/batch"
	"squirreldb/cassandra"
	"squirreldb/config"
	"squirreldb/index"
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
	batchSize := squirrelConfig.Int64("batch.size")
	prometheusListenAddress := squirrelConfig.String("prometheus.listen_address")

	cassandraOptions := cassandra.Options{
		Addresses:              squirrelConfig.Strings("cassandra.addresses"),
		ReplicationFactor:      squirrelConfig.Int("cassandra.replication_factor"),
		Keyspace:               squirrelConfig.String("cassandra.keyspace"),
		DefaultTimeToLive:      squirrelConfig.Int64("cassandra.default_time_to_live"),
		BatchSize:              batchSize,
		RawPartitionSize:       squirrelConfig.Int64("cassandra.partition_size.raw"),
		AggregateResolution:    squirrelConfig.Int64("cassandra.aggregate.resolution"),
		AggregateSize:          squirrelConfig.Int64("cassandra.aggregate.size"),
		AggregateStartOffset:   squirrelConfig.Int64("cassandra.aggregate.start_offset"),
		AggregatePartitionSize: squirrelConfig.Int64("cassandra.partition_size.aggregate"),

		DebugAggregateForce: squirrelConfig.Bool("debug.aggregate.force"), // TODO: Debug
		DebugAggregateSize:  squirrelConfig.Int64("debug.aggregate.size"), // TODO: Debug
	}

	var squirrelCassandra *cassandra.Cassandra

	retry.Do(func() error {
		var err error
		squirrelCassandra, err = cassandra.New(cassandraOptions)

		return err
	}, "main", "main",
		"Can't initialize the session",
		"Resolved: Initialized the session",
		retry.NewBackOff(30*time.Second))

	squirrelCassandra.LoadStates()

	squirrelStore := store.New()
	squirrelBatch := batch.New(batchSize, squirrelStore, squirrelCassandra, squirrelCassandra)
	squirrelIndex := index.New(squirrelCassandra)
	squirrelPrometheus := prometheus.New(squirrelIndex, squirrelBatch, squirrelBatch)

	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Run services
	runSquirrelCassandra := func() {
		defer wg.Done()
		squirrelCassandra.Run(ctx)
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

	if err := squirrelCassandra.SaveStates(); err != nil {
		logger.Println("Can't save the states (", err, ")")
	}

	squirrelCassandra.Close()

	signal.Stop(signalChan)
	close(signalChan)

	logger.Println("SquirrelDB is stopped")
}

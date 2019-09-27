package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"squirreldb/batch"
	"squirreldb/cassandra"
	"squirreldb/config"
	"squirreldb/prometheus"
	"squirreldb/retry"
	"squirreldb/store"
	"sync"
	"syscall"
)

func main() {
	logger := log.New(os.Stdout, "[main] ", log.LstdFlags)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	signals := make(chan os.Signal, 1)
	defer close(signals)

	squirrelStore := store.NewStore()
	squirrelCassandra := cassandra.NewCassandra()
	squirrelBatch := batch.NewBatch(squirrelStore, squirrelCassandra, squirrelCassandra)
	squirrelPrometheus := prometheus.NewPrometheus(squirrelBatch, squirrelBatch)

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	retry.Endlessly(config.CassandraRetryDelay, func() error {
		err := squirrelCassandra.InitSession("cassandra0:9042")

		if err != nil {
			logger.Printf("Can't init Cassandra session (%v)"+"\n", err)
		}

		return err
	}, logger)

	wg.Add(1)
	go squirrelPrometheus.RunServer(ctx, &wg)

	wg.Add(1)
	go squirrelStore.RunExpirator(ctx, &wg)

	wg.Add(1)
	go squirrelBatch.RunChecker(ctx, &wg)

	logger.Println("SquirrelDB ready")

	<-signals

	logger.Println("Stopping...")

	cancel()

	wg.Wait()

	squirrelCassandra.CloseSession()

	logger.Println("Stopped")
}

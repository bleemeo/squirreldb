package main

import (
	"context"
	"hamsterdb/batch"
	"hamsterdb/cassandra"
	"hamsterdb/config"
	"hamsterdb/prometheus"
	"hamsterdb/retry"
	"hamsterdb/store"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	logger := log.New(os.Stdout, "[main] ", log.LstdFlags)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	signals := make(chan os.Signal)
	defer close(signals)

	hamsterStore := store.NewStore()
	hamsterCassandra := cassandra.NewCassandra()
	hamsterBatch := batch.NewBatch(hamsterStore, hamsterCassandra)
	hamsterPrometheus := prometheus.NewPrometheus(hamsterCassandra, hamsterBatch)

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	retry.Endlessly(config.CassandraRetryDelay*time.Second, func() error {
		err := hamsterCassandra.InitSession("cassandra0:9042")

		if err != nil {
			logger.Printf("Can't init Cassandra session (%v)"+"\n", err)
		}

		return err
	}, logger)

	wg.Add(1)
	go hamsterPrometheus.RunServer(ctx, &wg)

	wg.Add(1)
	go hamsterStore.RunExpirator(ctx, &wg)

	wg.Add(1)
	go hamsterBatch.RunChecker(ctx, &wg)

	logger.Println("Hamster ready")

	<-signals

	logger.Println("Stopping...")

	cancel()

	wg.Wait()

	hamsterCassandra.CloseSession()

	logger.Println("Stopped")
}

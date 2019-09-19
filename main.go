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

	myStore := store.NewStore()
	myCassandra := cassandra.NewCassandra()
	myBatch := batch.NewBatch(myStore, myCassandra)
	myPrometheus := prometheus.NewPrometheus(myCassandra, myBatch)

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	retry.Endlessly(config.CassandraRetryDelay*time.Second, func() error {
		err := myCassandra.InitSession("cassandra0:9042")

		if err != nil {
			logger.Printf("Can't init Cassandra session (%v)"+"\n", err)
		}

		return err
	}, logger)

	wg.Add(1)
	go myPrometheus.RunServer(ctx, &wg)

	wg.Add(1)
	go myBatch.RunChecker(ctx, &wg)

	logger.Println("Hamster ready")

	<-signals

	logger.Println("Stopping...")

	cancel()

	wg.Wait()

	myCassandra.CloseSession()

	logger.Println("Stopped")
}

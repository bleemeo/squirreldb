package main

import (
	"context"
	"github.com/cenkalti/backoff"
	"log"
	"os"
	"os/signal"
	"squirreldb/batch"
	"squirreldb/cassandra"
	"squirreldb/prometheus"
	"squirreldb/store"
	"sync"
	"syscall"
	"time"
)

func main() {
	logger := log.New(os.Stdout, "[main] ", log.LstdFlags)

	squirrelStore := store.NewStore()
	squirrelCassandra := cassandra.NewCassandra()
	squirrelBatch := batch.NewBatch(squirrelStore, squirrelCassandra, squirrelCassandra)
	squirrelPrometheus := prometheus.NewPrometheus(squirrelBatch, squirrelBatch)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	defer close(signals)

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	_ = backoff.Retry(func() error {
		return squirrelCassandra.InitSession("cassandra0:9042")
	},
		&backoff.ExponentialBackOff{
			InitialInterval:     backoff.DefaultInitialInterval,
			RandomizationFactor: 0.5,
			Multiplier:          2,
			MaxInterval:         30 * time.Second,
			MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
			Clock:               backoff.SystemClock,
		})

	// Run services
	wg.Add(1)
	go squirrelPrometheus.RunServer(ctx, &wg)
	wg.Add(1)
	go squirrelBatch.RunChecker(ctx, &wg)
	wg.Add(1)
	go squirrelStore.RunExpirator(ctx, &wg)

	logger.Println("SquirrelDB ready")

	// Wait to receive a stop signal
	<-signals

	// Stop services
	logger.Println("Stopping...")

	cancel()

	wg.Wait()

	squirrelCassandra.CloseSession()

	logger.Println("Stopped")
}

package main

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
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
	config.C = config.New()

	if err := config.C.Init(); err != nil {
		logger.Fatalln("config: Init: Can't initialize config (", err, ")")
	}

	if config.C.Bool("help") {
		config.C.FlagSet.PrintDefaults()
		return
	}

	if config.C.Bool("test") {
		for _, key := range config.C.Keys() {
			fmt.Printf("%s:%v"+"\n", key, config.C.Get(key))
		}
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	squirrelStore := store.New()
	squirrelIndex := index.New()
	squirrelCassandra := cassandra.New()
	squirrelBatch := batch.New(squirrelStore, squirrelCassandra, squirrelCassandra)
	squirrelPrometheus := prometheus.New(squirrelIndex, squirrelBatch, squirrelBatch)

	cassandraAddresses := config.C.Strings("cassandra.addresses")

	_ = backoff.Retry(func() error {
		err := squirrelCassandra.Init(cassandraAddresses...)

		if err != nil {
			logger.Println("cassandra: Init: Can't initialize the session (", err, ")")
		}

		return err
	}, retry.NewBackOff(30*time.Second))

	// Run services
	wg.Add(1)
	go func() {
		defer wg.Done()
		squirrelCassandra.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		squirrelPrometheus.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		squirrelBatch.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		squirrelStore.Run(ctx)
	}()

	logger.Println("SquirrelDB ready")

	// Wait to receive a stop signal
	<-signals

	// Stop services
	logger.Println("Stopping...")

	cancel()
	wg.Wait()

	squirrelCassandra.Close()

	signal.Stop(signals)
	close(signals)

	logger.Println("Stopped")
}

package main

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/golang/glog"
	"log"
	"os"
	"os/signal"
	"squirreldb/batch"
	"squirreldb/cassandra"
	"squirreldb/config"
	"squirreldb/prometheus"
	"squirreldb/store"
	"sync"
	"syscall"
	"time"
)

func main() {
	logger := log.New(os.Stdout, "[main] ", log.LstdFlags)

	config.C = config.NewConfig()

	squirrelStore := store.NewStore()
	squirrelCassandra := cassandra.NewCassandra()
	squirrelBatch := batch.NewBatch(squirrelStore, squirrelCassandra, squirrelCassandra)
	squirrelPrometheus := prometheus.NewPrometheus(squirrelBatch, squirrelBatch)

	_ = backoff.Retry(func() error {
		err := config.C.Setup()

		if err != nil {
			logger.Println("config: Setup: Can't setup config (", err, ")")
		}

		return err
	}, &backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: 0.5,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Clock:               backoff.SystemClock,
	})

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

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	defer close(signals)

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	_ = backoff.Retry(func() error {
		err := squirrelCassandra.InitSession(config.C.Strings("cassandra.addresses")...)

		if err != nil {
			logger.Println("cassandra: InitSession: Can't initialize the session (", err, ")")
		}

		return err
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

	glog.Flush()
}

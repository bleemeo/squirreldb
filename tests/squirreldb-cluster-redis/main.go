package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

//nolint:gochecknoglobals
var (
	workerThreads   = flag.Int("test.threads", 5, "Number of concurrent threads per processes")
	workerProcesses = flag.Int("test.processes", 2, "Number of concurrent Cluster (equivalent to SquirrelDB process)")
	runTime         = flag.Duration("test.run-time", 10*time.Second, "Duration of the bench")
)

func main() {
	daemon.SetTestEnvironment()

	err := daemon.RunWithSignalHandler(run)

	metricResult, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range metricResult {
		_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
	}

	if err != nil {
		log.Fatal().Err(err).Msg("Run daemon failed")
	}
}

func run(ctx context.Context) error {
	cfg, warnings, err := daemon.Config()
	if err != nil {
		return err
	}

	if warnings != nil {
		return warnings
	}

	clients := make([]*BenchClient, *workerProcesses)

	for i := range clients {
		squirreldb := &daemon.SquirrelDB{
			Config: cfg,
			MetricRegistry: prometheus.WrapRegistererWith(
				map[string]string{"process": strconv.FormatInt(int64(i), 10)},
				prometheus.DefaultRegisterer,
			),
			Logger: log.With().Str("component", "daemon").Int("process", i).Logger(),
		}
		defer squirreldb.Stop()

		cluster, err := squirreldb.Cluster(ctx)
		if err != nil {
			return err
		}

		clients[i] = &BenchClient{
			cluster: cluster,
		}

		clients[i].Init()
	}

	var wg sync.WaitGroup

	start := time.Now()

	// First deadline is when we stop Publishing
	firstDeadline := time.Now().Add(*runTime)

	for i, cl := range clients {
		wg.Add(1)

		go func() {
			defer wg.Done()

			cl.Run(ctx, firstDeadline, i)
		}()
	}

	wg.Wait()
	log.Print("Publish stopped")

	maxDeadline := time.Now().Add(*runTime)
	totalTopic1 := 0
	totalTopic2 := 0

	for _, cl := range clients {
		totalTopic1 += cl.counter1
		totalTopic2 += cl.counter2
	}

	// Give few seconds for inflight message to arrive
	for time.Now().Before(maxDeadline) {
		ok := true

		for _, cl := range clients {
			cl.l.Lock()

			ok = len(cl.topic1) == totalTopic1 && len(cl.topic2) == totalTopic2

			cl.l.Unlock()

			if !ok {
				break
			}
		}

		if ok {
			break
		}

		time.Sleep(time.Second)
	}

	totalReceive := 0

	for i, cl := range clients {
		if cl != nil {
			cl.Close()
		}

		totalReceive += cl.msgTopic1 + cl.msgTopic2

		log.Printf(
			"Process %d send/receive: topic1: %d/%d topic2: %d/%d",
			i,
			cl.counter1,
			cl.msgTopic1,
			cl.counter2,
			cl.msgTopic2,
		)
	}

	delay := time.Since(start)
	log.Printf(
		"Send a total of %d messages (%.1f k msg/s) and received %d message (%.1f kmsg/s)",
		totalTopic1+totalTopic2,
		float64(totalTopic1+totalTopic2)/delay.Seconds()/1000,
		totalReceive,
		float64(totalReceive)/delay.Seconds()/1000,
	)

	for i, cl := range clients {
		if cl.err != nil {
			log.Printf("process %d had error: %v", i, cl.err)
			err = cl.err
		}

		if len(cl.topic1) != totalTopic1 {
			err = fmt.Errorf("process %d received %d message on topic1, want %d", i, len(cl.topic1), totalTopic1)

			log.Print(err)
		}

		if len(cl.topic2) != totalTopic2 {
			err = fmt.Errorf("process %d received %d message on topic2, want %d", i, len(cl.topic2), totalTopic2)

			log.Print(err)
		}
	}

	return err
}

type BenchClient struct {
	topic1    map[string]bool
	topic2    map[string]bool
	msgTopic1 int
	msgTopic2 int
	err       error

	l        sync.Mutex
	counter1 int
	counter2 int

	cluster types.Cluster
}

func (b *BenchClient) Close() {
	_ = b.cluster.Close()
}

func (b *BenchClient) Init() {
	b.topic1 = make(map[string]bool)
	b.topic2 = make(map[string]bool)

	b.cluster.Subscribe("topic1", func(payload []byte) {
		b.l.Lock()
		defer b.l.Unlock()

		b.msgTopic1++
		b.topic1[string(payload)] = true
	})

	b.cluster.Subscribe("topic2", func(payload []byte) {
		b.l.Lock()
		defer b.l.Unlock()

		b.msgTopic2++
		b.topic2[string(payload)] = true
	})
}

func (b *BenchClient) Run(ctx context.Context, deadline time.Time, processID int) {
	group, ctx := errgroup.WithContext(ctx)

	for n := range *workerThreads {
		group.Go(func() error {
			return b.thread(ctx, deadline, processID, n)
		})
	}

	b.err = group.Wait()
}

func (b *BenchClient) thread(ctx context.Context, deadline time.Time, processID int, workerID int) error {
	var (
		counter1 int
		counter2 int
		err      error
	)

	for ctx.Err() == nil && time.Now().Before(deadline) {
		payload := []byte(fmt.Sprintf("%d-%d-%d", processID, workerID, counter1+counter2))

		if rand.Float64() < 0.5 { //nolint:gosec
			err = b.cluster.Publish(ctx, "topic1", payload)
			counter1++
		} else {
			err = b.cluster.Publish(ctx, "topic2", payload)
			counter2++
		}

		if err != nil {
			break
		}
	}

	b.l.Lock()
	defer b.l.Unlock()

	b.counter1 += counter1
	b.counter2 += counter2

	return err
}

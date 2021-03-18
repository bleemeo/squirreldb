package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"squirreldb/daemon"
	"squirreldb/redis/cluster"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"golang.org/x/sync/errgroup"
)

//nolint: gochecknoglobals
var (
	workerThreads   = flag.Int("test.threads", 5, "Number of concurrent threads per processes")
	workerProcesses = flag.Int("test.processes", 2, "Number of concurrent Cluster (equivalent to SquirrelDB process)")
	runTime         = flag.Duration("test.run-time", 10*time.Second, "Duration of the bench")
)

func main() {
	reg := prometheus.NewRegistry()

	err := daemon.RunWithSignalHandler(func(ctx context.Context) error {
		return run(ctx, reg)
	})

	metricResult, _ := reg.Gather()
	for _, mf := range metricResult {
		_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, reg prometheus.Registerer) error {
	squirreldb, err := daemon.New()
	if err != nil {
		return err
	}

	clients := make([]*BenchClient, *workerProcesses)

	for i := range clients {
		clients[i] = &BenchClient{
			Addresses: squirreldb.Config.Strings("redis.addresses"),
			Registry: prometheus.WrapRegistererWith(
				map[string]string{"process": strconv.FormatInt(int64(i), 10)},
				reg,
			),
		}

		err := clients[i].Init(ctx)
		if err != nil {
			return err
		}
	}

	var wg sync.WaitGroup

	start := time.Now()

	// First deadline is when we stop Publishing
	firstDeadline := time.Now().Add(*runTime)

	for i, cl := range clients {
		wg.Add(1)

		cl := cl
		i := i

		go func() {
			defer wg.Done()

			cl.Run(ctx, firstDeadline, i)
		}()
	}

	wg.Wait()
	log.Println("Publish stopped")

	maxDeadline := time.Now().Add(*runTime / 2)
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

			ok = len(cl.topic1) == totalTopic1-cl.counter1 && len(cl.topic2) == totalTopic2-cl.counter2

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

		if len(cl.topic1) != totalTopic1-cl.counter1 {
			err = fmt.Errorf("process %d received %d message on topic1, want %d", i, len(cl.topic1), totalTopic1-cl.counter1)

			log.Println(err)
		}

		if len(cl.topic2) != totalTopic2-cl.counter2 {
			err = fmt.Errorf("process %d received %d message on topic2, want %d", i, len(cl.topic2), totalTopic2-cl.counter2)

			log.Println(err)
		}
	}

	return err
}

type BenchClient struct {
	Addresses []string
	Registry  prometheus.Registerer
	topic1    map[string]bool
	topic2    map[string]bool
	msgTopic1 int
	msgTopic2 int
	err       error

	l        sync.Mutex
	counter1 int
	counter2 int

	client *cluster.Cluster
}

func (b *BenchClient) Close() {
	_ = b.client.Stop()
}

func (b *BenchClient) Init(ctx context.Context) error {
	b.client = &cluster.Cluster{
		Addresses:        b.Addresses,
		MetricRegistry:   b.Registry,
		ChannelNamespace: "test:",
	}

	b.topic1 = make(map[string]bool)
	b.topic2 = make(map[string]bool)

	if err := b.client.Start(ctx); err != nil {
		return err
	}

	b.client.Subscribe("topic1", func(payload []byte) {
		b.l.Lock()
		defer b.l.Unlock()

		b.msgTopic1++
		b.topic1[string(payload)] = true
	})

	b.client.Subscribe("topic2", func(payload []byte) {
		b.l.Lock()
		defer b.l.Unlock()

		b.msgTopic2++
		b.topic2[string(payload)] = true
	})

	return nil
}

func (b *BenchClient) Run(ctx context.Context, deadline time.Time, processID int) {
	group, ctx := errgroup.WithContext(ctx)

	for n := 0; n < *workerThreads; n++ {
		n := n

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

		if rand.Float64() < 0.5 { // nolint: gosec
			err = b.client.Publish(ctx, "topic1", payload)
			counter1++
		} else {
			err = b.client.Publish(ctx, "topic2", payload)
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

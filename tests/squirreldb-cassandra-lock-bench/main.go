package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"squirreldb/cassandra/locks"
	"squirreldb/cassandra/session"
	"squirreldb/debug"
	"squirreldb/types"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// nolint: gochecknoglobals
var (
	cassandraAddresses        = flag.String("cassandra.addresses", "localhost:9042", "Cassandra cluster addresses")
	cassandraKeyspace         = flag.String("cassandra.keyspace", "squirreldb_test", "Cassandra keyspace")
	cassanraReplicationFactor = flag.Int("cassandra.replication", 1, "Cassandra replication factor")
	seed                      = flag.Int64("seed", 42, "Seed used in random generator")
	runDuration               = flag.Duration("run-time", time.Minute, "Duration of the bench")
	workDuration              = flag.Duration("work-duration", 5*time.Millisecond, "Duration of one work (randomized +/- 50%")
	wokerIdleDuration         = flag.Duration("idle-duration", time.Millisecond, "Sleep time after a job complete and before the new one start (randomized +/- 50%)")
	workerThreads             = flag.Int("worker-threads", 2, "Number of concurrent threads per processes")
	workerProcesses           = flag.Int("worker-processes", 2, "Number of concurrent index (equivalent to process) inserting data")
	tryLockDelay              = flag.Duration("try-lock-duration", 0, "If non-zero use try-lock with fixed sleep between attempt")
	recreateLock              = flag.Bool("recreate-lock", false, "Create the lock object in each time needed (default is create it once per processes)")
	lockTTL                   = flag.Duration("lock-ttl", 2*time.Second, "TTL of the locks")
	lockName                  = flag.String("lock-name", "benchmarking-lock", "Name prefix of the lock")
	count                     = flag.Int("count", 1, "Number of different lock/task")
)

func makeLock() *locks.CassandraLocks {
	cassandraSession, keyspaceCreated, err := session.New(session.Options{
		Addresses:         strings.Split(*cassandraAddresses, ","),
		ReplicationFactor: *cassanraReplicationFactor,
		Keyspace:          *cassandraKeyspace,
	})
	if err != nil {
		log.Fatalf("Unable to open Cassandra session: %v", err)
	}

	squirrelLocks, err := locks.New(cassandraSession, keyspaceCreated)
	if err != nil {
		log.Fatalf("Unable to create locks: %v", err)
	}

	return squirrelLocks
}

type result struct {
	ErrCount         int
	LockAcquired     int
	LockTried        int
	AcquireMaxTime   time.Duration
	AcquireTotalTime time.Duration
	TryMaxTime       time.Duration
	TryTotalTime     time.Duration
	UnlockMaxTime    time.Duration
	UnlockTotalTime  time.Duration
}

func main() {
	flag.Parse()

	debug.Level = 0

	value, found := os.LookupEnv("SQUIRRELDB_CASSANDRA_ADDRESSES")
	if found {
		*cassandraAddresses = value
	}

	value, found = os.LookupEnv("SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR")
	if found {
		tmp, err := strconv.ParseInt(value, 10, 0)
		if err != nil {
			log.Fatalf("Bad SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR: %v", err)
		}
		*cassanraReplicationFactor = int(tmp)
	}

	rand.Seed(*seed)

	ctx, cancel := context.WithTimeout(context.Background(), *runDuration)
	resultChan := make(chan result, (*workerProcesses)*(*workerThreads)*(*count))
	jobRunning := make([]int32, *count)

	defer cancel()

	var wg sync.WaitGroup

	start := time.Now()

	for p := 0; p < *workerProcesses; p++ {
		lockFactory := makeLock()

		for n := 0; n < *count; n++ {
			subLockName := fmt.Sprintf("%s-%d", *lockName, n)
			lock := lockFactory.CreateLock(subLockName, *lockTTL)

			for t := 0; t < *workerThreads; t++ {
				workerSeed := rand.Int63()
				p := p
				t := t
				n := n

				wg.Add(1)

				go func() {
					defer wg.Done()

					stats := worker(ctx, p, t, workerSeed, &jobRunning[n], lockFactory, subLockName, lock)
					resultChan <- stats
				}()
			}
		}
	}

	wg.Wait()

	duration := time.Since(start)

	close(resultChan)

	globalResult := result{}

	for r := range resultChan {
		globalResult.LockAcquired += r.LockAcquired
		globalResult.LockTried += r.LockTried
		globalResult.TryTotalTime += r.TryTotalTime
		globalResult.AcquireTotalTime += r.AcquireTotalTime
		globalResult.UnlockTotalTime += r.UnlockTotalTime
		globalResult.ErrCount += r.ErrCount

		if globalResult.TryMaxTime < r.TryMaxTime {
			globalResult.TryMaxTime = r.TryMaxTime
		}

		if globalResult.AcquireMaxTime < r.AcquireMaxTime {
			globalResult.AcquireMaxTime = r.AcquireMaxTime
		}

		if globalResult.UnlockMaxTime < r.UnlockMaxTime {
			globalResult.UnlockMaxTime = r.UnlockMaxTime
		}
	}

	log.Printf("In %v acquired %d locks and tried to acquire %d", duration, globalResult.LockAcquired, globalResult.LockTried)
	log.Printf("This result in %.2f lock acquired/s and %.2f lock tried/s", float64(globalResult.LockAcquired)/duration.Seconds(), float64(globalResult.LockTried)/duration.Seconds())

	if globalResult.LockTried > 0 {
		log.Printf(
			"Time to TryLock avg = %v max = %v",
			globalResult.TryTotalTime/time.Duration(globalResult.LockTried),
			globalResult.TryMaxTime,
		)
	}

	if globalResult.LockAcquired > 0 {
		log.Printf(
			"Time to    Lock avg = %v max = %v  Unlock avg = %v max = %v",
			globalResult.AcquireTotalTime/time.Duration(globalResult.LockAcquired),
			globalResult.AcquireMaxTime,
			globalResult.UnlockTotalTime/time.Duration(globalResult.LockAcquired),
			globalResult.UnlockMaxTime,
		)
	}

	if globalResult.ErrCount > 0 {
		log.Printf("Had %d errors, see logs", globalResult.ErrCount)
	}

	metricResult, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range metricResult {
		_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
	}

	if globalResult.ErrCount > 0 {
		os.Exit(1)
	}
}

func worker(ctx context.Context, p int, t int, workerSeed int64, jobRunning *int32, lockFactory *locks.CassandraLocks, subLockName string, lock types.TryLocker) result { // nolint: gocognit
	rnd := rand.New(rand.NewSource(workerSeed))
	r := result{}

	for ctx.Err() == nil {
		if *recreateLock {
			lock = lockFactory.CreateLock(subLockName, *lockTTL)
		}

		start := time.Now()
		acquired := false

		if *tryLockDelay > 0 {
			for ctx.Err() == nil {
				start := time.Now()
				acquired = lock.TryLock()

				duration := time.Since(start)

				r.LockTried++
				r.TryTotalTime += duration

				if r.TryMaxTime < duration {
					r.TryMaxTime = duration
				}

				if acquired {
					break
				}

				time.Sleep(*tryLockDelay)
			}
		} else {
			lock.Lock()
			acquired = true
		}

		if acquired {
			duration := time.Since(start)

			r.LockAcquired++
			r.AcquireTotalTime += duration

			if r.AcquireMaxTime < duration {
				r.AcquireMaxTime = duration
			}

			running := atomic.AddInt32(jobRunning, 1)
			if running != 1 {
				log.Printf("lock=%s P=%d, T=%d, job running = %d want 1", subLockName, p, t, running)
				r.ErrCount++
			}

			sleep := time.Duration(float64(*workDuration) * (rnd.Float64() + 0.5))
			time.Sleep(sleep)

			running = atomic.AddInt32(jobRunning, -1)
			if running != 0 {
				log.Printf("lock=%s P=%d, T=%d, job running = %d want 0", subLockName, p, t, running)
				r.ErrCount++
			}

			start = time.Now()

			lock.Unlock()

			duration = time.Since(start)

			r.UnlockTotalTime += duration

			if r.UnlockMaxTime < duration {
				r.UnlockMaxTime = duration
			}

			sleep = time.Duration(float64(*wokerIdleDuration) * (rnd.Float64() + 0.5))
			time.Sleep(sleep)
		}
	}

	return r
}

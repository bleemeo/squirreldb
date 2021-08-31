package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"squirreldb/daemon"
	"squirreldb/types"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

//nolint:lll,gochecknoglobals
var (
	seed            = flag.Int64("seed", 42, "Seed used in random generator")
	runDuration     = flag.Duration("run-time", 10*time.Second, "Duration of the bench")
	ctxTimeout      = flag.Duration("ctx-timeout", 15*time.Second, "Context deadline to acquire lock (0=unlimited)")
	workDuration    = flag.Duration("work-duration", 100*time.Millisecond, "Duration of one work (randomized +/- 50%")
	workerThreads   = flag.Int("worker-threads", 25, "Number of concurrent threads per processes")
	workerProcesses = flag.Int("worker-processes", 2, "Number of concurrent index (equivalent to process) inserting data")
	tryLockDelay    = flag.Duration("try-lock-duration", 15*time.Second, "If delay given to TryLock()")
	recreateLock    = flag.Bool("recreate-lock", false, "Create the lock object in each time needed (default is create it once per processes)")
	lockTTL         = flag.Duration("lock-ttl", 2*time.Second, "TTL of the locks")
	lockName        = flag.String("lock-name", "benchmarking-lock", "Name prefix of the lock")
	count           = flag.Int("count", 1, "Number of different lock/task")
)

type result struct {
	ErrCount         int
	LockAcquired     int
	LockFail         int
	LockTimeOut      int
	AcquireMaxTime   time.Duration
	AcquireTotalTime time.Duration
	FailMaxTime      time.Duration
	FailTotalTime    time.Duration
	UnlockMaxTime    time.Duration
	UnlockTotalTime  time.Duration
	WorkTotalTime    time.Duration
}

func main() {
	daemon.SetTestEnvironment()

	err := daemon.RunWithSignalHandler(run)

	metricResult, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range metricResult {
		_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cfg, err := daemon.Config()
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(*seed)

	ctx, cancel := context.WithTimeout(ctx, *runDuration)
	resultChan := make(chan result, (*workerProcesses)*(*workerThreads)*(*count))
	jobRunning := make([]int32, *count)

	defer cancel()

	var wg sync.WaitGroup

	start := time.Now()

	for p := 0; p < *workerProcesses; p++ {
		squirreldb := &daemon.SquirrelDB{
			Config: cfg,
			MetricRegistry: prometheus.WrapRegistererWith(
				map[string]string{"process": strconv.FormatInt(int64(p), 10)},
				prometheus.DefaultRegisterer,
			),
		}

		lockFactory, err := squirreldb.LockFactory()
		if err != nil {
			return err
		}

		for n := 0; n < *count; n++ {
			subLockName := fmt.Sprintf("%s-%d", *lockName, n)
			lock := lockFactory.CreateLock(subLockName, *lockTTL)

			for t := 0; t < *workerThreads; t++ {
				workerSeed := rand.Int63() //nolint:gosec
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
		globalResult.LockFail += r.LockFail
		globalResult.LockTimeOut += r.LockTimeOut
		globalResult.FailTotalTime += r.FailTotalTime
		globalResult.AcquireTotalTime += r.AcquireTotalTime
		globalResult.UnlockTotalTime += r.UnlockTotalTime
		globalResult.ErrCount += r.ErrCount
		globalResult.WorkTotalTime += r.WorkTotalTime

		if globalResult.FailMaxTime < r.FailMaxTime {
			globalResult.FailMaxTime = r.FailMaxTime
		}

		if globalResult.AcquireMaxTime < r.AcquireMaxTime {
			globalResult.AcquireMaxTime = r.AcquireMaxTime
		}

		if globalResult.UnlockMaxTime < r.UnlockMaxTime {
			globalResult.UnlockMaxTime = r.UnlockMaxTime
		}
	}

	workPercent := globalResult.WorkTotalTime.Seconds() / duration.Seconds() * 100
	log.Printf("Worked %.2f %% of time (%v)", workPercent, globalResult.WorkTotalTime)

	log.Printf("In %v acquired %d locks + failed %d + timeout %d",
		duration,
		globalResult.LockAcquired,
		globalResult.LockFail,
		globalResult.LockTimeOut,
	)

	log.Printf("This result in %.2f lock acquired/s and %.2f lock fail/s (+ %.2f timeout/s)",
		float64(globalResult.LockAcquired)/duration.Seconds(),
		float64(globalResult.LockFail)/duration.Seconds(),
		float64(globalResult.LockTimeOut)/duration.Seconds(),
	)

	if globalResult.LockFail > 0 {
		log.Printf(
			"Time to fail Lock avg = %v max = %v",
			globalResult.FailTotalTime/time.Duration(globalResult.LockFail),
			globalResult.FailMaxTime,
		)
	}

	if globalResult.LockAcquired > 0 {
		log.Printf(
			"Time to      Lock avg = %v max = %v  Unlock avg = %v max = %v",
			globalResult.AcquireTotalTime/time.Duration(globalResult.LockAcquired),
			globalResult.AcquireMaxTime,
			globalResult.UnlockTotalTime/time.Duration(globalResult.LockAcquired),
			globalResult.UnlockMaxTime,
		)
	}

	if globalResult.ErrCount > 0 {
		log.Printf("Had %d errors, see logs", globalResult.ErrCount)
	}

	if globalResult.ErrCount > 0 {
		return fmt.Errorf("had %d error", globalResult.ErrCount)
	}

	return nil
}

func worker(
	ctx context.Context,
	p int,
	t int,
	workerSeed int64,
	jobRunning *int32,
	lockFactory daemon.LockFactory,
	subLockName string,
	lock types.TryLocker,
) result {
	rnd := rand.New(rand.NewSource(workerSeed)) //nolint:gosec
	r := result{}

	for ctx.Err() == nil {
		var cancel context.CancelFunc

		ctx := ctx

		if *ctxTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, *ctxTimeout)
		}

		if *recreateLock {
			lock = lockFactory.CreateLock(subLockName, *lockTTL)
		}

		start := time.Now()
		acquired := lock.TryLock(ctx, *tryLockDelay)

		switch {
		case acquired:
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

			sleep = time.Duration(float64(*workDuration) * (rnd.Float64() + 0.5))
			r.WorkTotalTime += sleep
			time.Sleep(sleep)
		case ctx.Err() != nil:
			r.LockTimeOut++
		default:
			duration := time.Since(start)

			r.LockFail++
			r.FailTotalTime += duration

			if r.FailMaxTime < duration {
				r.FailMaxTime = duration
			}
		}

		if cancel != nil {
			cancel()
		}
	}

	return r
}

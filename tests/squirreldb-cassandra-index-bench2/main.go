// This benchmark is mostly used to test shard transition issue:
// * Our postings are shared by time, with each shard being 7 days long.
// * When a new shard beging, we need to have all postings for active metrics created for this new shard.
// * Which means that with naive implementation, when new shard start, lots of write are needed
//
// This benchmark will simulate multiple servers (bench.scale) and do LookupIDs (the request done during
// metric ingestion) for metrics of each similated servers. It does that at a fake clock time which is
// close to the new shard time.
// It will then show the time required for LookupIDs (with average, 90th quantile...) each 10 minutes near shard change,
// to show how much request time increase when shard change.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

//nolint:lll,gochecknoglobals
var (
	workerProcesses       = flag.Int("bench.worker-processes", 1, "Number of concurrent index (equivalent to process) inserting data")
	scaleAgent            = flag.Int("bench.scale-agent", 1, "The scale factor for agent. Number of agent created per tenant")
	scaleTenant           = flag.Int("bench.scale-tenant", 1, "The scale factor for tenant. Number of tenant created")
	fakeClockDuration     = flag.Duration("bench.fake-clock-duration", 140*time.Minute, "Fake duration the test will run. This affect the fake clock not real time")
	sleepDelay            = flag.Duration("bench.sleep-delay", 1*time.Millisecond, "Time sleeping between each step in fake clock. 0 means as fast as possible")
	fakeClockStep         = flag.Duration("bench.fake-clock-step", 10*time.Second, "Fake clock step, writes are send every fake step delay")
	initialDelayToThursay = flag.Duration("bench.initial-delay", -70*time.Minute, "Initial offset delay to previous Thursday 00:00 UTC")
	stickyMetrics         = flag.Bool("bench.sicky-metrics", false, "Force same metrics to go to the same SquirrelDB")
	warmupDuration        = flag.Duration("bench.warm-up-duration", 5*time.Minute, "Time at the beginning of the test where timing are ignored. Useful to ignore initial metrics creation")
)

// This constants should match value in CassandraIndex.
const (
	postingShardSize        = 7 * 24 * time.Hour
	backgroundCheckInterval = time.Minute
)

type fakeClock struct {
	l      sync.Mutex
	offset time.Duration
}

func (c *fakeClock) Now() time.Time {
	c.l.Lock()
	defer c.l.Unlock()

	return time.Now().Add(c.offset)
}

func (c *fakeClock) AddOffset(value time.Duration) {
	c.l.Lock()
	defer c.l.Unlock()

	c.offset += value
}

func (c *fakeClock) SetOffset(value time.Duration) {
	c.l.Lock()
	defer c.l.Unlock()

	c.offset = value
}

type requestCounter struct {
	cond     sync.Cond
	reqCount int
}

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

	squirreldb := &daemon.SquirrelDB{
		Config: cfg,
		MetricRegistry: prometheus.WrapRegistererWith(
			map[string]string{"process": "test1"},
			prometheus.DefaultRegisterer,
		),
		Logger: log.With().Str("component", "daemon").Int("process", 1).Logger(),
	}
	defer squirreldb.Stop()

	log.Printf("Dropping tables")

	err = squirreldb.DropCassandraData(ctx, false)
	if err != nil {
		return fmt.Errorf("failed to drop keyspace: %w", err)
	}

	clock := &fakeClock{}

	err = bench(ctx, cfg, clock)
	if err != nil {
		return err
	}

	squirreldbVerifier := &daemon.SquirrelDB{
		Config:         cfg,
		MetricRegistry: prometheus.NewRegistry(),
		Logger:         log.With().Str("component", "daemon").Str("process", "verifier").Logger(),
	}
	defer squirreldbVerifier.Stop()

	idx, err := squirreldbVerifier.Index(ctx, true, clock.Now)
	if err != nil {
		return err
	}

	indexVerifier, ok := idx.(types.VerifiableIndex)

	if !ok {
		return errors.New("can not verify, index isn't a CassandraIndex")
	}

	_, err = indexVerifier.Verifier(os.Stderr).
		WithStrictExpiration(true).
		WithStrictMetricCreation(true).
		Verify(ctx)
	if err != nil {
		return err
	}

	return nil
}

type DurationAndTime struct {
	Duration time.Duration
	At       time.Time
}

func cmpDuration(a, b DurationAndTime) int {
	return cmp.Compare(a.Duration, b.Duration)
}

func cmpTime(a, b DurationAndTime) int {
	if a.At.Before(b.At) {
		return -1
	}

	if a.At.Equal(b.At) {
		return 0
	}

	return 1
}

type workerStats struct {
	workerID         int
	AllDurations     []DurationAndTime
	RunOnceDurations []DurationAndTime
}

func findNextShardChange(t time.Time) time.Time {
	tUnix := t.UnixMilli()
	previousShardStartAt := time.UnixMilli(tUnix - tUnix%postingShardSize.Milliseconds())

	return previousShardStartAt.Add(postingShardSize)
}

func bench(ctx context.Context, cfg config.Config, clock *fakeClock) error {
	group, ctx := errgroup.WithContext(ctx)

	workChannel := make(chan []types.LookupRequest)
	resultChan := make(chan workerStats, (*workerProcesses))
	channels := make([]chan []types.LookupRequest, *workerProcesses)
	counter := &requestCounter{
		cond: *sync.NewCond(&sync.Mutex{}),
	}

	for n := 0; n < len(channels); n++ {
		channels[n] = make(chan []types.LookupRequest)
	}

	if !*stickyMetrics {
		group.Go(func() error {
			mergeChannels(ctx, workChannel, channels)

			return nil
		})
	}

	cluster := &dummy.LocalCluster{}

	for p := range *workerProcesses {
		squirreldb := &daemon.SquirrelDB{
			Config: cfg,
			MetricRegistry: prometheus.WrapRegistererWith(
				map[string]string{"process": fmt.Sprintf("bench%d", p)},
				prometheus.DefaultRegisterer,
			),
			ExistingCluster: cluster,
			Logger:          log.With().Str("component", "daemon").Int("process", p).Logger(),
		}

		defer squirreldb.Stop()

		indexInterface, err := squirreldb.Index(ctx, true, clock.Now)
		if err != nil {
			return err
		}

		index, ok := indexInterface.(IndexWithRun)
		if !ok {
			return errors.New("can not run index which doesn't implement IndexRunner")
		}

		group.Go(func() error {
			var (
				stats workerStats
				err   error
			)

			if *stickyMetrics {
				stats, err = worker(ctx, clock, counter, index, channels[p])
			} else {
				stats, err = worker(ctx, clock, counter, index, workChannel)
			}

			stats.workerID = p
			resultChan <- stats

			return err
		})
	}

	testStartTime := sentInsertRequest(ctx, clock, cfg.TenantLabelName, counter, channels)

	for _, ch := range channels {
		close(ch)
	}

	showWorkersStats(resultChan, testStartTime)

	err := group.Wait()

	return err
}

// sentInsertRequest write request to channels and return the fake time test started.
func sentInsertRequest(
	ctx context.Context,
	clock *fakeClock,
	tenantLabelName string,
	counter *requestCounter,
	channels []chan []types.LookupRequest,
) time.Time {
	commonAgentName := []string{
		"localhost",
		"webserver01",
		"webserver02",
		"database",
		"backup",
	}

	previousShardStartAt := findNextShardChange(time.Now().Add(-postingShardSize))
	initialTime := previousShardStartAt.Add(*initialDelayToThursay)
	currentTime := initialTime
	clock.SetOffset(time.Until(initialTime))

	log.Printf("Test start with fakeClock set to %s", clock.Now())

	testStartAt := clock.Now()
	testDuration := *fakeClockDuration
	testEndAt := testStartAt.Add(testDuration)
	lastProgressShow := 0

	serverIDs := make([]nameAndTenant, 0, (*scaleAgent)*(*scaleTenant))

	for tenantN := range *scaleTenant {
		tenantID := uuid.New().String()

		for agentN := range *scaleAgent {
			var agentName string

			if agentN < len(commonAgentName) {
				agentName = commonAgentName[agentN]
			} else {
				agentName = fmt.Sprintf("agent-%d-of-tenant-%d", agentN, tenantN)
			}

			serverIDs = append(serverIDs, nameAndTenant{name: agentName, tenant: tenantID})
		}
	}

	totalRequests := 0

	for currentTime.Before(testEndAt) {
		if ctx.Err() != nil {
			break
		}

		progression := clock.Now().Sub(testStartAt)
		progressionPercent := int(progression * 100 / testDuration)

		if progressionPercent > lastProgressShow+5 {
			lastProgressShow = progressionPercent
			log.Printf("Test time is at %s (%d%%)", clock.Now(), progressionPercent)
		}

		requests := makeInsertRequests(clock.Now(), tenantLabelName, serverIDs)

		totalRequests += len(requests)

		for idx, req := range requests {
			select {
			case channels[idx%len(channels)] <- req:
			case <-ctx.Done():
			}
		}

		// wait for exec of request
		counter.cond.L.Lock()

		for counter.reqCount < totalRequests && ctx.Err() == nil {
			counter.cond.Wait()
		}

		counter.cond.L.Unlock()

		time.Sleep(*sleepDelay)

		currentTime = currentTime.Add(*fakeClockStep)
		clock.SetOffset(time.Until(currentTime))
	}

	log.Printf("Number of requests sent: %d", totalRequests)

	return initialTime
}

func showWorkersStats(resultChan chan workerStats, testStartTime time.Time) {
	for range *workerProcesses {
		stats := <-resultChan

		showWorkerStats("LookupIDs", stats.workerID, stats.AllDurations, testStartTime)
		showWorkerStats("RunOnce", stats.workerID, stats.RunOnceDurations, testStartTime)
	}
}

func showWorkerStats(action string, workerID int, array []DurationAndTime, testStartTime time.Time) {
	if *warmupDuration > 0 {
		// Remove write before secondBatchTime, this will eliminate metrics creation time
		i := 0

		for _, row := range array {
			if row.At.Before(testStartTime.Add(*warmupDuration)) {
				continue
			}

			array[i] = row
			i++
		}

		array = array[:i]
	}

	slices.SortFunc(array, cmpDuration)

	showStatsForDurations(fmt.Sprintf("Workder %d %s", workerID, action), array)

	if len(array) == 0 {
		return
	}

	// Show stats around shard changes
	const bucketSizeInSeconds = 600

	startTime := slices.MinFunc(array, cmpTime).At
	endTime := slices.MaxFunc(array, cmpTime).At
	nextShardChange := findNextShardChange(startTime)
	buckets := make(map[int64][]DurationAndTime, 12)

	for nextShardChange.Before(endTime) {
		// Create buckets around shard changes: 6 before the nextShardChange, 6 after
		for n := -6; n <= 6; n++ {
			startTimeUnix := nextShardChange.Add(time.Duration(n) * 10 * time.Minute).Unix()
			buckets[startTimeUnix-startTimeUnix%bucketSizeInSeconds] = nil
		}

		nextShardChange = findNextShardChange(nextShardChange)
	}

	for _, row := range array {
		bucketSlot := row.At.Unix() - row.At.Unix()%bucketSizeInSeconds
		if array, ok := buckets[bucketSlot]; ok {
			array = append(array, row)
			buckets[bucketSlot] = array
		}
	}

	bucketKeys := make([]int64, 0, len(buckets))

	for k := range buckets {
		bucketKeys = append(bucketKeys, k)
	}

	slices.Sort(bucketKeys)

	for _, slot := range bucketKeys {
		array := buckets[slot]
		slotTime := time.Unix(slot, 0)
		showStatsForDurations(fmt.Sprintf("Workder %d %s at %s", workerID, action, slotTime), array)
	}
}

func showStatsForDurations(prefix string, array []DurationAndTime) {
	if len(array) == 0 {
		log.Printf(
			"%s: do 0 request", prefix,
		)

		return
	}

	var sumDurations time.Duration

	maxDuration := slices.MaxFunc(array, cmpDuration).Duration
	minDuration := slices.MinFunc(array, cmpDuration).Duration
	q95Duration := array[len(array)*95/100].Duration
	q99Duration := array[len(array)*99/100].Duration

	for _, row := range array {
		sumDurations += row.Duration
	}

	avgDuration := sumDurations / time.Duration(len(array))

	log.Printf(
		"%s: do %d requests in min/avg/q95/q99/max = %10s / %10s / %10s / %10s / %10s",
		prefix,
		len(array),
		minDuration,
		avgDuration,
		q95Duration,
		q99Duration,
		maxDuration,
	)
}

type nameAndTenant struct {
	name   string
	tenant string
}

// makeInsertRequests produce N set of same metrics, N depend on scale. One set of metrics represent one server
// (with cpu, disk, memory...). Each server is associated with one tenant.
func makeInsertRequests(
	now time.Time,
	tenantLabelName string,
	serverNamesAndTenant []nameAndTenant,
) [][]types.LookupRequest {
	results := make([][]types.LookupRequest, 0, len(serverNamesAndTenant))

	for _, serverID := range serverNamesAndTenant {
		oneSet := []types.LookupRequest{
			{
				Start: now,
				End:   now,
				Labels: labels.FromMap(map[string]string{
					"__name__":      "cpu_used",
					"instance":      serverID.name,
					tenantLabelName: serverID.tenant,
				}),
			},
			{
				Start: now,
				End:   now,
				Labels: labels.FromMap(map[string]string{
					"__name__":      "mem_used_perc",
					"instance":      serverID.name,
					tenantLabelName: serverID.tenant,
				}),
			},
			{
				Start: now,
				End:   now,
				Labels: labels.FromMap(map[string]string{
					"__name__":      "mem_total",
					"instance":      serverID.name,
					tenantLabelName: serverID.tenant,
				}),
			},
			{
				Start: now,
				End:   now,
				Labels: labels.FromMap(map[string]string{
					"__name__":      "swap_used_perc",
					"instance":      serverID.name,
					tenantLabelName: serverID.tenant,
				}),
			},
			{
				Start: now,
				End:   now,
				Labels: labels.FromMap(map[string]string{
					"__name__":      "system_load1",
					"instance":      serverID.name,
					tenantLabelName: serverID.tenant,
				}),
			},
		}

		for _, fsPath := range []string{"/", "/srv", "/home"} {
			oneSet = append(oneSet, types.LookupRequest{
				Start: now,
				End:   now,
				Labels: labels.FromMap(map[string]string{
					"__name__":      "disk_used_perc",
					"item":          fsPath,
					"instance":      serverID.name,
					tenantLabelName: serverID.tenant,
				}),
			})
		}

		for _, diskDevice := range []string{"/dev/sda", "/dev/sdb"} {
			oneSet = append(oneSet, types.LookupRequest{
				Start: now,
				End:   now,
				Labels: labels.FromMap(map[string]string{
					"__name__":      "io_reads",
					"item":          diskDevice,
					"instance":      serverID.name,
					tenantLabelName: serverID.tenant,
				}),
			})
		}

		for _, netDevice := range []string{"eth0", "eth1"} {
			oneSet = append(oneSet, types.LookupRequest{
				Start: now,
				End:   now,
				Labels: labels.FromMap(map[string]string{
					"__name__":      "net_recv_bits",
					"item":          netDevice,
					"instance":      serverID.name,
					tenantLabelName: serverID.tenant,
				}),
			})
		}

		results = append(results, oneSet)
	}

	return results
}

func mergeChannels(ctx context.Context, target chan []types.LookupRequest, sources []chan []types.LookupRequest) {
	var wg sync.WaitGroup

	for _, ch := range sources {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for msg := range ch {
				select {
				case target <- msg:
				case <-ctx.Done():
				}
			}
		}()
	}

	wg.Wait()
	close(target)
}

type IndexWithRun interface {
	types.Index
	types.IndexRunner
}

// worker is more or less equivalent to on SquirrelDB process.
func worker(
	ctx context.Context,
	clock *fakeClock,
	counter *requestCounter,
	localIndex IndexWithRun,
	workChanel chan []types.LookupRequest,
) (workerStats, error) {
	stats := workerStats{}
	previousRunOnce := clock.Now()

	localIndex.InternalRunOnce(ctx, clock.Now())

	var wg sync.WaitGroup

	for work := range workChanel {
		now := clock.Now()
		if now.Sub(previousRunOnce) >= backgroundCheckInterval {
			wg.Add(1)

			// The InternalRunOnce is running inside a gorouting has this
			// is closer to what really happen. RunOnce is executed concurrently
			// with LookupIDs
			go func() {
				defer wg.Done()

				start := time.Now()

				localIndex.InternalRunOnce(ctx, clock.Now())

				previousRunOnce = now

				duration := time.Since(start)
				stats.RunOnceDurations = append(stats.RunOnceDurations, DurationAndTime{Duration: duration, At: clock.Now()})
			}()
		}

		start := time.Now()

		_, _, err := localIndex.LookupIDs(ctx, work)

		duration := time.Since(start)

		if err != nil {
			return stats, fmt.Errorf("LookupIDs() failed: %w", err)
		}

		stats.AllDurations = append(stats.AllDurations, DurationAndTime{Duration: duration, At: clock.Now()})

		wg.Wait()

		counter.cond.L.Lock()

		counter.reqCount++

		counter.cond.Signal()
		counter.cond.L.Unlock()
	}

	return stats, nil
}

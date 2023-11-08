package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"squirreldb/cassandra/index"
	"squirreldb/config"
	"squirreldb/daemon"
	"squirreldb/dummy"
	"squirreldb/types"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/procfs"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog/log"
)

type queryResult struct {
	Name        string
	Time        time.Duration
	ResultCount int
	QueryCount  int
}

//nolint:gochecknoglobals
var (
	// Choose name that didn't conflict with test (that is name that won't
	// match any matchers from test().
	names = []string{
		"go_memstats_heap_alloc_bytes",
		"go_memstats_heap_released_bytes",
		"node_cpu_scaling_frequency_hertz",
		"node_disk_written_bytes_total",
		"node_filesystem_size_bytes",
		"node_load5",
		"node_memory_Active_anon_bytes",
		"node_memory_Cached_bytes",
		"node_memory_ShmemHugePages_bytes",
		"node_memory_Slab_bytes",
		"node_memory_SUnreclaim_bytes",
		"node_netstat_IpExt_OutOctets",
		"node_netstat_Udp6_NoPorts",
		"node_network_flags",
		"node_network_receive_frame_total",
		"node_network_receive_packets_total",
		"node_timex_offset_seconds",
		"node_timex_pps_error_total",
		"node_timex_tick_seconds",
		"node_vmstat_pgpgout",
	}

	//nolint:lll
	helps = []string{
		"node_filesystem_size_bytes Filesystem size in bytes.",
		"go_memstats_gc_cpu_fraction The fraction of this program's available CPU time used by the GC since the program started.",
		"node_memory_SReclaimable_bytes Memory information field SReclaimable_bytes.",
		"node_timex_pps_jitter_total Pulse per second count of jitter limit exceeded events.",
		"node_memory_Shmem_bytes Memory information field Shmem_bytes.",
		"node_netstat_Tcp_PassiveOpens Statistic TcpPassiveOpens.",
		"go_memstats_last_gc_time_seconds Number of seconds since 1970 of last garbage collection.",
		"node_hwmon_temp_celsius Hardware monitor for temperature (input)",
		"node_network_transmit_compressed_total Network device statistic transmit_compressed.",
		"node_load15 15m load average.",
		"node_netstat_Udp6_InErrors Statistic Udp6InErrors.",
		"node_memory_MemAvailable_bytes Memory information field MemAvailable_bytes.",
		"node_memory_ShmemHugePages_bytes Memory information field ShmemHugePages_bytes.",
		"node_network_device_id device_id value of /sys/class/net/<iface>.",
		"go_memstats_heap_sys_bytes Number of heap bytes obtained from system.",
		"node_netstat_Udp_InErrors Statistic UdpInErrors.",
		"node_memory_MemTotal_bytes Memory information field MemTotal_bytes.",
		"node_network_flags flags value of /sys/class/net/<iface>.",
		"node_memory_Active_file_bytes Memory information field Active_file_bytes.",
		"node_cpu_scaling_frequency_max_hrts Maximum scaled cpu thread frequency in hertz.",
	}
)

func bench(ctx context.Context, cfg config.Config, rnd *rand.Rand) error { //nolint:maintidx
	now := time.Now()

	proc, err := procfs.NewProc(os.Getpid())
	if err != nil {
		return fmt.Errorf("NewProc() failed: %w", err)
	}

	var maxRSS int

	cluster := &dummy.LocalCluster{}

	squirreldb := &daemon.SquirrelDB{
		Config:          cfg,
		ExistingCluster: cluster,
		MetricRegistry: prometheus.WrapRegistererWith(
			map[string]string{"process": "bench-query"},
			prometheus.DefaultRegisterer,
		),
		Logger: log.With().Str("component", "daemon").Logger(),
	}
	defer squirreldb.Stop()

	idx, err := squirreldb.Index(ctx, true)
	if err != nil {
		return err
	}

	ids, err := idx.AllIDs(ctx, now, now)
	if err != nil {
		return fmt.Errorf("AllIDs() failed: %w", err)
	}

	metricsBefore := len(ids)
	shardCount := *shardEnd - *shardStart + 1

	if !*skipWrite { //nolint:nestif
		var wg sync.WaitGroup

		workChannel := make(chan []types.LookupRequest)
		resultChan := make(chan int, (*workerProcesses)*(*workerThreads))
		channels := make([]chan []types.LookupRequest, *workerProcesses)

		if *fairLB {
			for n := 0; n < len(channels); n++ {
				channels[n] = make(chan []types.LookupRequest)
			}

			wg.Add(1)

			go func() {
				defer wg.Done()

				loadBalancer(workChannel, channels)
			}()
		}

		for p := 0; p < *workerProcesses; p++ {
			p := p
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

			localIndex, err := squirreldb.Index(ctx, false)
			if err != nil {
				return err
			}

			wg.Add(1)

			go func() {
				defer wg.Done()

				if *fairLB {
					worker(ctx, localIndex, channels[p], resultChan)
				} else {
					worker(ctx, localIndex, workChannel, resultChan)
				}
			}()
		}

		start := time.Now()

		if rss := sentInsertRequest(now, rnd, proc, workChannel, resultChan); rss > maxRSS {
			maxRSS = rss
		}

		close(workChannel)
		wg.Wait()
		close(resultChan)

		if *shardSize > 0 && shardCount > 0 {
			queryDur := (time.Since(start) / time.Duration(*shardSize*shardCount)).Round(time.Microsecond)
			log.Printf("Average insert for %d shards took %v/query", shardCount, queryDur)
		}
	}

	start := time.Now()
	ids, err = idx.AllIDs(ctx, now, now)

	if err != nil {
		return fmt.Errorf("AllIDs() failed: %w", err)
	}

	nbAdded := len(ids) - metricsBefore
	log.Printf("There is %d entry in the index (%d added). AllIDs took %v", len(ids), nbAdded, time.Since(start))

	queries := []struct {
		Fun  func(i int) []*labels.Matcher
		Name string
	}{
		{
			Name: "shard=N",
			Fun: func(_ int) []*labels.Matcher {
				return []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "shardID", fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)),
				}
			},
		},
		{
			Name: "shard=N name=X",
			Fun: func(_ int) []*labels.Matcher {
				return []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "shardID", fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)),
					labels.MustNewMatcher(labels.MatchEqual, "__name__", names[rnd.Intn(len(names))]),
				}
			},
		},
		{
			Name: "name=X shard=N",
			Fun: func(_ int) []*labels.Matcher {
				return []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", names[rnd.Intn(len(names))]),
					labels.MustNewMatcher(labels.MatchEqual, "shardID", fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)),
				}
			},
		},
		{
			Name: "shard=N name!=X",
			Fun: func(_ int) []*labels.Matcher {
				return []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "shardID", fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)),
					labels.MustNewMatcher(labels.MatchNotEqual, "__name__", names[rnd.Intn(len(names))]),
				}
			},
		},
		{
			Name: "shard=N name=~X",
			Fun: func(_ int) []*labels.Matcher {
				return []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "shardID", fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)),
					labels.MustNewMatcher(labels.MatchRegexp, "__name__", names[rnd.Intn(len(names))][:6]+".*"),
				}
			},
		},
		{
			Name: "shard=N name=~X simple regex",
			Fun: func(_ int) []*labels.Matcher {
				return []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "shardID", fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)),
					labels.MustNewMatcher(
						labels.MatchRegexp,
						"__name__",
						fmt.Sprintf("(%s|%s)", names[rnd.Intn(len(names))], names[rnd.Intn(len(names))]),
					),
				}
			},
		},
		{
			Name: "shard=N name=node_.* name!=node_netstat_Udp_InErrors",
			Fun: func(_ int) []*labels.Matcher {
				return []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "shardID", fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)),
					labels.MustNewMatcher(labels.MatchRegexp, "__name__", "node_.*"),
					labels.MustNewMatcher(labels.MatchNotEqual, "__name__", "node_netstat_Udp_InErrors"),
				}
			},
		},
		{
			Name: "shard=N name=X randomID=\"\"",
			Fun: func(_ int) []*labels.Matcher {
				return []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", names[rnd.Intn(len(names))]),
					labels.MustNewMatcher(labels.MatchEqual, "shardID", fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)),
					labels.MustNewMatcher(labels.MatchEqual, "randomID", ""),
				}
			},
		},
	}
	for _, q := range queries {
		if *onlyQuery != "" && q.Name != *onlyQuery {
			continue
		}

		if stat, err := proc.Stat(); err == nil && maxRSS < stat.ResidentMemory() {
			maxRSS = stat.ResidentMemory()
		}

		result, err := runQuery(ctx, now, q.Name, idx, q.Fun)
		if err != nil {
			return err
		}

		if result.QueryCount == 0 {
			continue
		}

		timePerQuery := (result.Time / time.Duration(result.QueryCount))
		log.Printf(
			"%30s  %.3f ms/query (returned %.1f metrics/query and done %d queries)",
			result.Name,
			float64(timePerQuery.Microseconds())/1000,
			float64(result.ResultCount)/float64(result.QueryCount),
			result.QueryCount,
		)
	}

	if *runExpiration { //nolint:nestif
		indexExpirerer, ok := idx.(types.IndexInternalExpirerer)
		if !ok {
			return fmt.Errorf("can not run expiration on index which doesn't implement IndexInternalExpirerer")
		}

		beforePurge := len(ids)
		beforeYesterday := time.Now().Truncate(24 * time.Hour).Add(-2 * 24 * time.Hour)

		err = indexExpirerer.InternalForceExpirationTimestamp(ctx, beforeYesterday)
		if err != nil {
			return fmt.Errorf("state.Write() failed: %w", err)
		}

		indexRunner, ok := idx.(types.IndexRunner)
		if !ok {
			return fmt.Errorf("can not run index which doesn't implement IndexRunner")
		}

		start = time.Now()

		indexRunner.InternalRunOnce(ctx, time.Now())

		stop := time.Now()

		if stat, err := proc.Stat(); err == nil && maxRSS < stat.ResidentMemory() {
			maxRSS = stat.ResidentMemory()
		}

		ids, err = idx.AllIDs(ctx, now, now)
		if err != nil {
			return fmt.Errorf("AllIDs() failed: %w", err)
		}

		log.Printf("RunOnce took %v and deleted %d metrics", stop.Sub(start), beforePurge-len(ids))
	}

	log.Printf("Peak memory seen = %d kB (rss)", maxRSS/1024)

	return nil
}

func sentInsertRequest(
	now time.Time,
	rnd *rand.Rand,
	proc procfs.Proc,
	workChannel chan []types.LookupRequest,
	resultChan chan int,
) int {
	shardCount := *shardEnd - *shardStart + 1
	instantStart := time.Now()
	instantCount := 0
	globalStart := time.Now()
	globalCount := 0
	pendingRequest := 0
	maxRSS := 0

	for n := 0; n < shardCount; n++ {
		if *shardSize == 0 {
			break
		}

		shardID := *shardStart + n
		shardStr := fmt.Sprintf("shard%06d", shardID)

		requests := makeInsertRequests(now, shardStr, rnd)

		for startIndex := 0; startIndex < len(requests); startIndex += *insertBatchSize {
			endIndex := startIndex + *insertBatchSize
			if endIndex > len(requests) {
				endIndex = len(requests)
			}

			for {
				var r int

				if pendingRequest >= *workerClients {
					r = <-resultChan
				} else {
					select {
					case r = <-resultChan:
					default:
						r = -1
					}
				}

				if r == -1 {
					break
				}

				pendingRequest--

				instantCount += r
				globalCount += r

				if globalCount%(10*(*shardSize)) == 0 {
					log.Printf(
						"Registered %d metrics at speed %v/query (global %d at %v/query)",
						instantCount,
						(time.Since(instantStart) / time.Duration(instantCount)).Round(time.Microsecond),
						globalCount,
						(time.Since(globalStart) / time.Duration(globalCount)).Round(time.Microsecond),
					)

					instantCount = 0
					instantStart = time.Now()
				}
			}

			pendingRequest++
			workChannel <- requests[startIndex:endIndex]
		}

		if stat, err := proc.Stat(); err == nil && maxRSS < stat.ResidentMemory() {
			maxRSS = stat.ResidentMemory()
		}
	}

	return maxRSS
}

// loadBalancer will sent requests to each outputs one after one, regardless if the outputs is busy/blocked.
//
// This more or less match default nginx behavior.
func loadBalancer(input chan []types.LookupRequest, outputs []chan []types.LookupRequest) {
	n := 0

	var wg sync.WaitGroup

	for w := range input {
		wg.Add(1)

		go func(n int, w []types.LookupRequest) {
			defer wg.Done()
			outputs[n] <- w
		}(n, w)

		n++
		n %= len(outputs)
	}

	wg.Wait()

	for _, c := range outputs {
		close(c)
	}
}

// worker is more or less equivalent to on SquirrelDB process.
func worker(ctx context.Context, localIndex types.Index, workChanel chan []types.LookupRequest, result chan int) {
	token := make(chan bool, *workerThreads)
	for n := 0; n < *workerThreads; n++ {
		token <- true
	}

	for work := range workChanel {
		work := work

		<-token

		go func() {
			_, _, err := localIndex.LookupIDs(ctx, work)
			if err != nil {
				log.Fatal().Err(err).Msg("LookupIDs() failed")
			}

			result <- len(work)
			token <- true
		}()
	}

	for n := 0; n < *workerThreads; n++ {
		<-token
	}
}

// benchInsert insert *metricCount metrics with random labels.
//
// It have:
// * __name__ with few values
// * shardID
// * randomID with high number of values
// * randomNNN with high number of values (actually same as randomID)
// * helpNNN with few values
//
// Metrics may also have additional labels (labelNN), ranging from 0 to 20 additional labels
// (most of the time, 3 additional labels). Few values for those labels.
func makeInsertRequests(now time.Time, shardID string, rnd *rand.Rand) []types.LookupRequest {
	metrics := make([]types.LookupRequest, *shardSize)

	// We remove 1 days (and max ttl update delay) so the expiration of the
	// metrics is yesterday
	negativeTTL := -int64((24*time.Hour + index.InternalMaxTTLUpdateDelay()).Seconds())

	for n := 0; n < *shardSize; n++ {
		userID := strconv.FormatInt(rnd.Int63n(100000), 10)
		labelsMap := map[string]string{
			"__name__":                               names[rnd.Intn(len(names))],
			"shardID":                                shardID,
			"randomID":                               userID,
			fmt.Sprintf("random%03d", rnd.Intn(100)): userID,
			fmt.Sprintf("help%03d", rnd.Intn(100)):   helps[rnd.Intn(len(helps))],
		}

		var addN int
		// 50% of metrics have 3 additional labels
		if rnd.Intn(2) == 0 {
			addN = 3
		} else {
			addN = rnd.Intn(20)
		}

		for i := 0; i < addN; i++ {
			labelsMap[fmt.Sprintf("label%02d", i)] = strconv.FormatInt(rnd.Int63n(20), 10)
		}

		promLabel := labels.FromMap(labelsMap)

		if *sortInsert {
			sort.Sort(promLabel)
		}

		request := types.LookupRequest{
			Start:  now,
			End:    now,
			Labels: promLabel,
		}

		if *expiredFaction > 0 && n%*expiredFaction == 0 {
			request.TTLSeconds = negativeTTL
		}

		metrics[n] = request
	}

	return metrics
}

func runQuery(
	ctx context.Context,
	now time.Time,
	name string,
	cassandraIndex types.Index,
	fun func(i int) []*labels.Matcher,
) (queryResult, error) {
	start := time.Now()
	count := 0

	var n int
	for n = 0; n < *queryCount; n++ {
		if time.Since(start) > *queryMaxTime {
			break
		}

		matchers := fun(n)

		ids, err := cassandraIndex.Search(ctx, now, now, matchers)
		if err != nil {
			return queryResult{}, fmt.Errorf("Search() failed: %w", err)
		}

		count += ids.Count()
	}

	return queryResult{
		Name:        name,
		Time:        time.Since(start),
		ResultCount: count,
		QueryCount:  n,
	}, nil
}

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"squirreldb/cassandra/index"
	"squirreldb/cassandra/states"
	"strconv"
	"time"

	"github.com/prometheus/procfs"
	"github.com/prometheus/prometheus/prompb"
)

type queryResult struct {
	Name        string
	Time        time.Duration
	ResultCount int
	QueryCount  int
}

//nolint: gochecknoglobals
var (
	// Choose name that didn't conflict with test (that is name that won't
	// match any matchers from test()
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

func bench(cassandraIndex *index.CassandraIndex, rnd *rand.Rand, squirrelStates *states.CassandraStates) { //nolint: gocognit
	proc, err := procfs.NewProc(os.Getpid())

	if err != nil {
		log.Fatalf("NewProc() failed: %v", err)
	}

	var maxRSS int

	ids, err := cassandraIndex.AllIDs()
	if err != nil {
		log.Fatalf("AllIDs() failed: %v", err)
	}

	metricsBefore := len(ids)

	var sumInsertTime time.Duration

	shardCount := *shardEnd - *shardStart + 1

	for n := 0; n < shardCount; n++ {
		if *shardSize == 0 {
			break
		}

		shardID := *shardStart + n
		shardStr := fmt.Sprintf("shard%06d", shardID)
		insertTime := benchInsert(cassandraIndex, shardStr, rnd)

		if n%10 == 0 || shardID == *shardEnd {
			log.Printf("Insert for shard %d took %v/query", shardID, (insertTime / time.Duration(*shardSize)).Round(time.Microsecond))
		}

		sumInsertTime += insertTime

		if stat, err := proc.Stat(); err == nil && maxRSS < stat.ResidentMemory() {
			maxRSS = stat.ResidentMemory()
		}
	}

	if *shardSize > 0 && shardCount > 0 {
		log.Printf("Average insert for %d shards took %v/query", shardCount, (sumInsertTime / time.Duration(*shardSize*shardCount)).Round(time.Microsecond))
	}

	start := time.Now()
	ids, err = cassandraIndex.AllIDs()

	if err != nil {
		log.Fatalf("AllIDs() failed: %v", err)
	}

	log.Printf("There is %d entry in the index (%d added). AllIDs took %v", len(ids), len(ids)-metricsBefore, time.Since(start))

	queries := []struct {
		Name string
		Fun  func(i int) []*prompb.LabelMatcher
	}{
		{
			Name: "shard=N",
			Fun: func(_ int) []*prompb.LabelMatcher {
				return []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "shardID", Value: fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)},
				}
			},
		},
		{
			Name: "shard=N name=X",
			Fun: func(_ int) []*prompb.LabelMatcher {
				return []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "shardID", Value: fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)},
					{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: names[rnd.Intn(len(names))]},
				}
			},
		},
		{
			Name: "name=X shard=N",
			Fun: func(_ int) []*prompb.LabelMatcher {
				return []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: names[rnd.Intn(len(names))]},
					{Type: prompb.LabelMatcher_EQ, Name: "shardID", Value: fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)},
				}
			},
		},
		{
			Name: "shard=N name!=X",
			Fun: func(_ int) []*prompb.LabelMatcher {
				return []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "shardID", Value: fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)},
					{Type: prompb.LabelMatcher_NEQ, Name: "__name__", Value: names[rnd.Intn(len(names))]},
				}
			},
		},
		{
			Name: "shard=N name=~X",
			Fun: func(_ int) []*prompb.LabelMatcher {
				return []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "shardID", Value: fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)},
					{Type: prompb.LabelMatcher_RE, Name: "__name__", Value: names[rnd.Intn(len(names))][:6] + ".*"},
				}
			},
		},
		{
			Name: "shard=N name=node_.* name!=node_netstat_Udp_InErrors",
			Fun: func(_ int) []*prompb.LabelMatcher {
				return []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "shardID", Value: fmt.Sprintf("shard%06d", rnd.Intn(shardCount)+*shardStart)},
					{Type: prompb.LabelMatcher_RE, Name: "__name__", Value: "node_.*"},
					{Type: prompb.LabelMatcher_NEQ, Name: "__name__", Value: "node_netstat_Udp_InErrors"},
				}
			},
		},
	}
	for _, q := range queries {
		if stat, err := proc.Stat(); err == nil && maxRSS < stat.ResidentMemory() {
			maxRSS = stat.ResidentMemory()
		}

		result := runQuery(q.Name, cassandraIndex, q.Fun)

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

	if *runExpiration {
		beforePurge := len(ids)
		beforeYesterday := time.Now().Truncate(24 * time.Hour).Add(-2 * 24 * time.Hour)
		err = squirrelStates.Write("index-expired-until", beforeYesterday.Format(time.RFC3339))

		if err != nil {
			log.Fatalf("state.Write() failed: %v", err)
		}

		start = time.Now()

		cassandraIndex.RunOnce(context.Background())

		stop := time.Now()

		if stat, err := proc.Stat(); err == nil && maxRSS < stat.ResidentMemory() {
			maxRSS = stat.ResidentMemory()
		}

		ids, err = cassandraIndex.AllIDs()
		if err != nil {
			log.Fatalf("AllIDs() failed: %v", err)
		}

		log.Printf("RunOnce took %v and deleted %d metrics", stop.Sub(start), beforePurge-len(ids))
	}

	log.Printf("Peak memory seen = %d kB (rss)", maxRSS/1024)
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
func benchInsert(cassandraIndex *index.CassandraIndex, shardID string, rnd *rand.Rand) time.Duration {
	metrics := make([][]prompb.Label, *shardSize)

	// We remove 1 days (and 1 hour) so the expiration of the metrics is yesterday
	// (the 1 hour is because of index cassandraTTLUpdateDelay)
	negativeTTL := strconv.FormatInt(-86400-3600, 10)

	for n := 0; n < *shardSize; n++ {
		userID := strconv.FormatInt(rnd.Int63n(100000), 10)
		labels := map[string]string{
			"__name__":                               names[rnd.Intn(len(names))],
			"shardID":                                shardID,
			"randomID":                               userID,
			fmt.Sprintf("random%03d", rnd.Intn(100)): userID,
			fmt.Sprintf("help%03d", rnd.Intn(100)):   helps[rnd.Intn(len(helps))],
		}

		var addN int
		// 50% of metrics have 3 additional labels
		if rnd.Intn(1) == 0 {
			addN = 3
		} else {
			addN = rnd.Intn(20)
		}

		for i := 0; i < addN; i++ {
			labels[fmt.Sprintf("label%02d", i)] = strconv.FormatInt(rnd.Int63n(20), 10)
		}

		if *expiredFaction > 0 && n%*expiredFaction == 0 {
			labels["__ttl__"] = negativeTTL
		}

		promLabel := map2Labels(labels)

		if *sortInsert {
			sort.Slice(promLabel, func(i, j int) bool {
				return promLabel[i].Name < promLabel[j].Name
			})
		}

		metrics[n] = promLabel
	}

	start := time.Now()

	for startIndex := 0; startIndex < len(metrics); startIndex += *insertBatchSize {
		endIndex := startIndex + *insertBatchSize
		if endIndex > len(metrics) {
			endIndex = len(metrics)
		}

		_, _, err := cassandraIndex.LookupIDs(metrics[startIndex:endIndex])
		if err != nil {
			log.Fatalf("benchInsert() failed: %v", err)
		}
	}

	return time.Since(start)
}

func runQuery(name string, cassandraIndex *index.CassandraIndex, fun func(i int) []*prompb.LabelMatcher) queryResult {
	start := time.Now()
	count := 0

	var n int
	for n = 0; n < *queryCount; n++ {
		matchers := fun(n)
		ids, err := cassandraIndex.Search(matchers)

		if err != nil {
			log.Fatalf("Search() failed: %v", err)
		}

		count += len(ids)

		if time.Since(start) > *queryMaxTime {
			break
		}
	}

	return queryResult{
		Name:        name,
		Time:        time.Since(start),
		ResultCount: count,
		QueryCount:  n,
	}
}

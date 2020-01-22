package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"squirreldb/cassandra/index"
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

type benchResult struct {
	metricsBefore int
	metricsAfter  int
	insertCount   int
	insertTime    time.Duration
	queries       []queryResult
	allUUIDsTime  time.Duration
	maxRSS        int
	benchCount    int
}

func (b benchResult) Log(name string) {
	log.Printf("== Run %s added %d metrics (%d metrics in DB)", name, b.metricsAfter-b.metricsBefore, b.metricsAfter)

	if b.insertCount > 0 {
		log.Printf(
			"insert took %v: %v/metrics",
			b.insertTime.Round(time.Millisecond),
			(b.insertTime / time.Duration(b.insertCount)).Round(time.Microsecond),
		)
	}

	log.Printf("AllUUIDs() took %v", (b.allUUIDsTime / time.Duration(b.benchCount)).Round(time.Millisecond))
	log.Printf("Peak memory seen = %d kB (rss)", b.maxRSS/1024)

	for _, q := range b.queries {
		if q.QueryCount == 0 {
			continue
		}

		log.Printf(
			"Query %s took %v and returned %d metrics (%v/query and %.1f metrics/query)",
			q.Name,
			(q.Time / time.Duration(b.benchCount)).Round(time.Millisecond),
			q.ResultCount/b.benchCount,
			(q.Time / time.Duration(q.QueryCount)).Round(time.Microsecond),
			float64(q.ResultCount)/float64(q.QueryCount),
		)
	}
}

func (b *benchResult) Add(other benchResult) {
	if other.metricsBefore < b.metricsBefore {
		b.metricsBefore = other.metricsBefore
	}

	if other.metricsAfter > b.metricsAfter {
		b.metricsAfter = other.metricsAfter
	}

	b.insertCount += other.insertCount

	if other.maxRSS > b.maxRSS {
		b.maxRSS = other.maxRSS
	}

	if b.queries == nil {
		b.queries = make([]queryResult, len(other.queries))
	}

	for i, qo := range other.queries {
		b.queries[i].Name = qo.Name
		b.queries[i].Time += qo.Time
		b.queries[i].ResultCount += qo.ResultCount
		b.queries[i].QueryCount += qo.QueryCount
	}

	b.insertTime += other.insertTime
	b.allUUIDsTime += other.allUUIDsTime
	b.benchCount++
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

func bench(cassandraIndex *index.CassandraIndex, shardID string, insertRnd *rand.Rand, queryRnd *rand.Rand) benchResult {
	proc, err := procfs.NewProc(os.Getpid())

	if err != nil {
		log.Fatalf("NewProc() failed: %v", err)
	}

	b := benchResult{
		benchCount:  1,
		insertCount: *shardSize,
	}

	uuids, err := cassandraIndex.AllUUIDs()
	if err != nil {
		log.Fatalf("AllUUIDs() failed: %v", err)
	}

	b.metricsBefore = len(uuids)

	b.insertTime = benchInsert(cassandraIndex, shardID, insertRnd)

	b.queries = make([]queryResult, 0)

	queries := []struct {
		Name string
		Fun  func(i int) []*prompb.LabelMatcher
	}{
		{
			Name: "whole-shard-eq",
			Fun: func(_ int) []*prompb.LabelMatcher {
				return []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "shardID", Value: shardID},
				}
			},
		},
		{
			Name: "name-eq",
			Fun: func(_ int) []*prompb.LabelMatcher {
				return []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "shardID", Value: shardID},
					{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: names[queryRnd.Intn(len(names))]},
				}
			},
		},
	}
	for _, q := range queries {
		if stat, err := proc.Stat(); err == nil && b.maxRSS < stat.ResidentMemory() {
			b.maxRSS = stat.ResidentMemory()
		}

		b.queries = append(b.queries, runQuery(q.Name, cassandraIndex, q.Fun))
	}

	start := time.Now()
	uuids, err = cassandraIndex.AllUUIDs()

	if err != nil {
		log.Fatalf("AllUUIDs() failed: %v", err)
	}

	b.allUUIDsTime = time.Since(start)
	b.metricsAfter = len(uuids)

	if stat, err := proc.Stat(); err == nil && b.maxRSS < stat.ResidentMemory() {
		b.maxRSS = stat.ResidentMemory()
	}

	return b
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
func benchInsert(cassandraIndex *index.CassandraIndex, shardID string, insertRnd *rand.Rand) time.Duration {
	metrics := make([][]*prompb.Label, *shardSize)

	for n := 0; n < *shardSize; n++ {
		userID := strconv.FormatInt(insertRnd.Int63n(100000), 10)
		labels := map[string]string{
			"__name__": names[insertRnd.Intn(len(names))],
			"shardID":  shardID,
			"randomID": userID,
			fmt.Sprintf("random%03d", insertRnd.Intn(100)): userID,
			fmt.Sprintf("help%03d", insertRnd.Intn(100)):   helps[insertRnd.Intn(len(helps))],
		}

		var addN int
		// 50% of metrics have 3 additional labels
		if insertRnd.Intn(1) == 0 {
			addN = 3
		} else {
			addN = insertRnd.Intn(20)
		}

		for i := 0; i < addN; i++ {
			labels[fmt.Sprintf("label%02d", i)] = strconv.FormatInt(insertRnd.Int63n(20), 10)
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

	for _, labels := range metrics {
		_, _, err := cassandraIndex.LookupUUID(labels)
		if err != nil {
			log.Fatalf("benchInsert() failed: %v", err)
		}
	}

	return time.Since(start)
}

func runQuery(name string, cassandraIndex *index.CassandraIndex, fun func(i int) []*prompb.LabelMatcher) queryResult {
	start := time.Now()
	count := 0

	for n := 0; n < *queryCount; n++ {
		matchers := fun(n)
		uuids, err := cassandraIndex.Search(matchers)

		if err != nil {
			log.Fatalf("Search() failed: %v", err)
		}

		count += len(uuids)
	}

	return queryResult{
		Name:        name,
		Time:        time.Since(start),
		ResultCount: count,
		QueryCount:  *queryCount,
	}
}

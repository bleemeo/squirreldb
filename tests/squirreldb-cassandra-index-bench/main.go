package main

import (
	"flag"
	"log"
	"math/rand"
	"reflect"
	"squirreldb/cassandra/index"
	"squirreldb/cassandra/session"
	"strings"

	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"
)

// nolint: gochecknoglobals
var (
	cassandraAddresses        = flag.String("cassandra.addresses", "localhost:9042", "Cassandra cluster addresses")
	cassandraKeyspace         = flag.String("cassandra.keyspace", "squirreldb_test", "Cassandra keyspace")
	cassanraReplicationFactor = flag.Int("cassandra.replication", 1, "Cassandra replication factor")
	includeUUID               = flag.Bool("index.include-uuid", false, "IncludeUUID")
	defaultTimeToLive         = flag.Int64("index.ttl", 86400*365, "Default time to live in seconds")
	randSeed                  = flag.Int64("seed", 42, "Seed used in random generator")
)

func main() {
	flag.Parse()

	cassandraSession, err := session.New(session.Options{
		Addresses:         strings.Split(*cassandraAddresses, ","),
		ReplicationFactor: *cassanraReplicationFactor,
		Keyspace:          *cassandraKeyspace,
	})
	if err != nil {
		log.Fatalf("Unable to open Cassandra session: %v", err)
	}

	cassandraIndex, err := index.New(cassandraSession, index.Options{
		DefaultTimeToLive: *defaultTimeToLive,
		IncludeUUID:       *includeUUID,
	})
	if err != nil {
		log.Fatalf("Unable to create index: %v", err)
	}

	cassandraIndex2, err := index.New(cassandraSession, index.Options{
		DefaultTimeToLive: *defaultTimeToLive,
		IncludeUUID:       *includeUUID,
	})
	if err != nil {
		log.Fatalf("Unable to create 2nd index: %v", err)
	}

	rand.Seed(*randSeed)
	log.Printf("Start validating test")
	test(cassandraIndex)
	log.Printf("Re-run validating test")
	test(cassandraIndex)
	log.Printf("Re-run validating test on fresh index")
	test(cassandraIndex2)
}

func map2Labels(input map[string]string) []*prompb.Label {
	result := make([]*prompb.Label, 0, len(input))
	for k, v := range input {
		result = append(result, &prompb.Label{
			Name:  k,
			Value: v,
		})
	}

	return result
}

func labels2Map(input []*prompb.Label) map[string]string {
	result := make(map[string]string, len(input))
	for _, l := range input {
		result[l.Name] = l.Value
	}
	return result
}

func test(cassandraIndex *index.CassandraIndex) {
	metrics := []map[string]string{
		{}, // index 0 is skipped to distinguish "not found" from 0
		{ // index 1
			"__name__": "up",
			"job":      "prometheus",
			"instance": "localhost:9090",
		},
		{ // index 2
			"__name__": "up",
			"job":      "node_exporter",
			"instance": "localhost:9100",
		},
		{ // index 3
			"__name__": "up",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
		},
		{ // index 4
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "0",
			"mode":     "idle",
		},
		{ // index 5
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "0",
			"mode":     "user",
		},
		{ // index 6
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "1",
			"mode":     "user",
		},
		{ // index 7
			"__name__":   "node_filesystem_avail_bytes",
			"job":        "node_exporter",
			"instance":   "localhost:9100",
			"device":     "/dev/mapper/vg0-root",
			"fstype":     "ext4",
			"mountpoint": "/",
		},
		{ // index 8
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "localhost:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "devel",
		},
		{ // index 9
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "remote:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "production",
		},
	}

	tests := []struct {
		Name            string
		Matchers        []*prompb.LabelMatcher
		MatchingMetrics []int
	}{
		{
			Name: "eq",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "up",
				},
			},
			MatchingMetrics: []int{1, 2, 3},
		},
		{
			Name: "eq-eq",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "mode",
					Value: "user",
				},
			},
			MatchingMetrics: []int{5, 6},
		},
		{
			Name: "eq-neq",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "mode",
					Value: "user",
				},
			},
			MatchingMetrics: []int{4},
		},
		{
			Name: "eq-nolabel",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "",
				},
			},
			MatchingMetrics: []int{7},
		},
		{
			Name: "eq-label",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "environment",
					Value: "",
				},
			},
			MatchingMetrics: []int{8, 9},
		},
		{
			Name: "re",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "u.",
				},
			},
			MatchingMetrics: []int{1, 2, 3},
		},
		{
			Name: "re-re",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_cpu_.*",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "mode",
					Value: "^u.*",
				},
			},
			MatchingMetrics: []int{5, 6},
		},
		{
			Name: "re-nre",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_(cpu|disk)_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "mode",
					Value: "u\\wer",
				},
			},
			MatchingMetrics: []int{4},
		},
		{
			Name: "re-re_nolabel",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "environment",
					Value: "^$",
				},
			},
			MatchingMetrics: []int{7},
		},
		{
			Name: "re-re_label",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "^$",
				},
			},
			MatchingMetrics: []int{8, 9},
		},
		{
			Name: "re-re*",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "environment",
					Value: ".*",
				},
			},
			MatchingMetrics: []int{7, 8, 9},
		},
		{
			Name: "re-nre*",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: ".*",
				},
			},
			MatchingMetrics: []int{},
		},
		{
			Name: "eq-nre_empty_and_devel",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "(|devel)",
				},
			},
			MatchingMetrics: []int{9},
		},
		{
			Name: "eq-nre-eq same label",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "^$",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "devel",
				},
			},
			MatchingMetrics: []int{8},
		},
	}

	metricsUUID := make([]gouuid.UUID, len(metrics))
	UUIDmetrics := make(map[gouuid.UUID]int, len(metrics))

	for i, labels := range metrics {
		if i == 0 {
			continue
		}

		uuid, ttl, err := cassandraIndex.LookupUUID(map2Labels(labels))
		if err != nil {
			log.Fatalf("LookupUUID(%v) failed: %v", labels, err)
		}

		if ttl != *defaultTimeToLive {
			log.Fatalf("TTL = %d, want default TTL (%d)", ttl, *defaultTimeToLive)
		}

		metricsUUID[i] = uuid
		UUIDmetrics[uuid] = i
	}

	for _, tt := range tests {
		wantUUIDToIndex := make(map[gouuid.UUID]int, len(tt.MatchingMetrics))

		for _, m := range tt.MatchingMetrics {
			wantUUIDToIndex[metricsUUID[m]] = m
		}

		uuids, err := cassandraIndex.Search(tt.Matchers)

		if err != nil {
			log.Fatalf("Search(%s) failed: %v", tt.Name, err)
		}

		gotUUIDToIndex := make(map[gouuid.UUID]int, len(uuids))

		for _, uuid := range uuids {
			gotUUIDToIndex[uuid] = UUIDmetrics[uuid]
		}

		if !reflect.DeepEqual(wantUUIDToIndex, gotUUIDToIndex) {
			log.Fatalf("Search(%s) = %v, want %v", tt.Name, gotUUIDToIndex, wantUUIDToIndex)
		}
	}

	if *includeUUID {
		uuid, _, err := cassandraIndex.LookupUUID([]*prompb.Label{
			{Name: "__uuid__", Value: metricsUUID[1].String()},
			{Name: "ignored", Value: "__uuid__ win"},
		})
		if err != nil {
			log.Fatalf("LookupUUID(__uuid__ valid) failed: %v", err)
		}
		if uuid != metricsUUID[1] {
			log.Fatalf("LookupUUID(__uuid__ valid) = %v, want %v", uuid, metricsUUID[1])
		}

		_, _, err = cassandraIndex.LookupUUID([]*prompb.Label{
			{Name: "__uuid__", Value: "00000000-0000-0000-0000-000000000001"},
			{Name: "ignored", Value: "__uuid__ win"},
		})
		if err == nil {
			log.Fatalf("LookupUUID(__uuid__ invalid) succeded. It must fail")
		}

		uuids, err := cassandraIndex.Search([]*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: "__uuid__", Value: metricsUUID[1].String()},
			{Type: prompb.LabelMatcher_EQ, Name: "ignored", Value: "only_uuid_is_used"},
		})
		if err != nil {
			log.Fatalf("Search(__uuid__ valid) failed: %v", err)
		}
		if len(uuids) != 1 || uuids[0] != metricsUUID[1] {
			log.Fatalf("Search(__uuid__ valid) = %v, want [%v]", uuids, metricsUUID[1])
		}

		uuids, err = cassandraIndex.Search([]*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: "__uuid__", Value: metricsUUID[2].String()},
			{Type: prompb.LabelMatcher_NEQ, Name: "__name__", Value: "up"},
		})
		if err != nil {
			log.Fatalf("Search(__uuid__ valid 2) failed: %v", err)
		}
		if len(uuids) != 1 || uuids[0] != metricsUUID[2] {
			log.Fatalf("Search(__uuid__ valid 2) = %v, want [%v]", uuids, metricsUUID[2])
		}
	}

	for i, uuid := range metricsUUID {
		if i == 0 {
			continue
		}
		labels, err := cassandraIndex.LookupLabels(uuid)
		if err != nil {
			log.Fatalf("LookupLabels(%d) failed: %v", i, err)
		}
		got := labels2Map(labels)
		if !reflect.DeepEqual(got, metrics[i]) {
			log.Fatalf("LookupLabels(%d) = %v, want %v", i, got, metrics[i])
		}
	}

	got, err := cassandraIndex.AllUUIDs()
	if err != nil {
		log.Fatalf("AllUUIDs() failed: %v", err)
	}
	gotMap := make(map[gouuid.UUID]int, len(got))
	for _, v := range got {
		gotMap[v] = UUIDmetrics[v]
	}
	if !reflect.DeepEqual(gotMap, UUIDmetrics) {
		log.Fatalf("AllUUIDs() = %v, want %v", gotMap, UUIDmetrics)
	}
}
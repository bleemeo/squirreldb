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
	}

	tests := []struct {
		Name            string
		Matchers        []*prompb.LabelMatcher
		MatchingMetrics []int
	}{
		{
			Name: "equal-one-label",
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
			Name: "equal-two-label",
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
}

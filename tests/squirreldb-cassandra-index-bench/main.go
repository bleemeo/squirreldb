package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"squirreldb/cassandra/index"
	"squirreldb/cassandra/session"
	"strings"

	"github.com/gocql/gocql"
	"github.com/prometheus/prometheus/prompb"
)

// nolint: gochecknoglobals
var (
	cassandraAddresses        = flag.String("cassandra.addresses", "localhost:9042", "Cassandra cluster addresses")
	cassandraKeyspace         = flag.String("cassandra.keyspace", "squirreldb_test", "Cassandra keyspace")
	cassanraReplicationFactor = flag.Int("cassandra.replication", 1, "Cassandra replication factor")
	includeUUID               = flag.Bool("index.include-uuid", false, "IncludeUUID")
	defaultTimeToLive         = flag.Int64("index.ttl", 86400*365, "Default time to live in seconds")
	insertSeed                = flag.Int64("bench.insert-seed", 42, "Seed used in random generator for insetion")
	querySeed                 = flag.Int64("bench.query-seed", 42, "Seed used in random generator for query")
	reuseSeed                 = flag.Bool("bench.same-seed", false, "Reuse the same seed for each shard")
	sortInsert                = flag.Bool("bench.insert-sorted", false, "Keep label sorted at insertion time (Prometheus do it)")
	queryCount                = flag.Int("bench.query", 1000, "Number of query to run")
	shardSize                 = flag.Int("bench.shard-size", 1000, "How many metrics to add in one shard (a shard is a label with the same value. Think tenant)")
	shardStart                = flag.Int("bench.shard-start", 1, "Start at shard number N")
	shardEnd                  = flag.Int("bench.shard-end", 5, "End at shard number N (included)")
	shardStep                 = flag.Int("bench.shard-step", 1, "Advance shard number by N")
	dropTables                = flag.Bool("drop", false, "Drop tables before")
)

func main() {
	flag.Parse()

	value, found := os.LookupEnv("SQUIRRELDB_CASSANDRA_ADDRESSES")
	if found {
		*cassandraAddresses = value
	}

	cassandraSession, err := session.New(session.Options{
		Addresses:         strings.Split(*cassandraAddresses, ","),
		ReplicationFactor: *cassanraReplicationFactor,
		Keyspace:          *cassandraKeyspace,
	})
	if err != nil {
		log.Fatalf("Unable to open Cassandra session: %v", err)
	}

	if *dropTables {
		drop(cassandraSession)
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

	rand.Seed(42)

	insertRnd := rand.New(rand.NewSource(*insertSeed))
	queryRnd := rand.New(rand.NewSource(*querySeed))

	log.Printf("Start validating test")
	test(cassandraIndex)
	log.Printf("Re-run validating test")
	test(cassandraIndex)
	log.Printf("Re-run validating test on fresh index")
	test(cassandraIndex2)

	results := make([]benchResult, 0)

	for shardID := *shardStart; shardID <= *shardEnd; shardID += *shardStep {
		if *reuseSeed {
			insertRnd.Seed(*insertSeed)
			queryRnd.Seed(*querySeed)
		}

		result := bench(cassandraIndex, fmt.Sprintf("shard%06d", shardID), insertRnd, queryRnd)
		result.Log(fmt.Sprintf("shard %d", shardID))
		results = append(results, result)
	}

	log.Printf("--- Average stat for %d runs ---", len(results))

	cummulative := benchResult{
		metricsBefore: results[0].metricsBefore,
	}

	for _, b := range results {
		cummulative.Add(b)
	}

	cummulative.Log("global")
}

func drop(session *gocql.Session) {
	queries := []string{
		"DROP TABLE IF EXISTS index_labels2uuid",
		"DROP TABLE IF EXISTS index_postings",
		"DROP TABLE IF EXISTS index_uuid2labels",
	}
	for _, query := range queries {
		if err := session.Query(query).Exec(); err != nil {
			log.Fatalf("Unable to drop table: %v", err)
		}
	}
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

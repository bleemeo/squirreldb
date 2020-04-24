package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"squirreldb/cassandra/index"
	"squirreldb/cassandra/locks"
	"squirreldb/cassandra/session"
	"squirreldb/cassandra/states"
	"squirreldb/debug"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/prompb"
)

// nolint: gochecknoglobals
var (
	cassandraAddresses        = flag.String("cassandra.addresses", "localhost:9042", "Cassandra cluster addresses")
	cassandraKeyspace         = flag.String("cassandra.keyspace", "squirreldb_test", "Cassandra keyspace")
	cassanraReplicationFactor = flag.Int("cassandra.replication", 1, "Cassandra replication factor")
	includeID                 = flag.Bool("index.include-id", false, "IncludeID")
	runExpiration             = flag.Bool("bench.expiration", false, "Run the expiration (should delete 1/2 of metrics")
	expiredFaction            = flag.Int("expired-fraction", 2, "one part over N of the metric will be expired")
	defaultTimeToLive         = flag.Duration("index.ttl", 365*24*time.Hour, "Default time to live")
	seed                      = flag.Int64("bench.seed", 42, "Seed used in random generator")
	sortInsert                = flag.Bool("bench.insert-sorted", false, "Keep label sorted at insertion time (Prometheus do it)")
	queryCount                = flag.Int("bench.query", 1000, "Number of query to run")
	queryMaxTime              = flag.Duration("bench.max-time", 5*time.Second, "Maxium time for one query time")
	shardSize                 = flag.Int("bench.shard-size", 1000, "How many metrics to add in one shard (a shard is a label with the same value. Think tenant)")
	shardStart                = flag.Int("bench.shard-start", 1, "Start at shard number N")
	shardEnd                  = flag.Int("bench.shard-end", 5, "End at shard number N (included)")
	insertBatchSize           = flag.Int("bench.batch-size", 1000, "Number of metrics to lookup at once")
	noDropTables              = flag.Bool("no-drop", false, "Don't drop tables before (in such case, you should not change shardSize and seed)")
)

func main() {
	flag.Parse()

	debug.Level = 1

	value, found := os.LookupEnv("SQUIRRELDB_CASSANDRA_ADDRESSES")
	if found {
		*cassandraAddresses = value
	}

	cassandraSession, keyspaceCreated, err := session.New(session.Options{
		Addresses:         strings.Split(*cassandraAddresses, ","),
		ReplicationFactor: *cassanraReplicationFactor,
		Keyspace:          *cassandraKeyspace,
	})
	if err != nil {
		log.Fatalf("Unable to open Cassandra session: %v", err)
	}

	if !*noDropTables {
		log.Printf("Droping tables")
		drop(cassandraSession)
	}

	squirrelLocks, err := locks.New(cassandraSession, keyspaceCreated)
	if err != nil {
		log.Fatalf("Unable to create locks: %v", err)
	}

	squirrelStates, err := states.New(cassandraSession, squirrelLocks.SchemaLock())
	if err != nil {
		log.Fatalf("Unable to create states: %v", err)
	}

	cassandraIndex, err := index.New(cassandraSession, index.Options{
		DefaultTimeToLive: *defaultTimeToLive,
		IncludeID:         *includeID,
		LockFactory:       squirrelLocks,
		States:            squirrelStates,
		SchemaLock:        squirrelLocks.SchemaLock(),
	})
	if err != nil {
		log.Fatalf("Unable to create index: %v", err)
	}

	cassandraIndex2, err := index.New(cassandraSession, index.Options{
		DefaultTimeToLive: *defaultTimeToLive,
		IncludeID:         *includeID,
		LockFactory:       squirrelLocks,
		States:            squirrelStates,
		SchemaLock:        squirrelLocks.SchemaLock(),
	})
	if err != nil {
		log.Fatalf("Unable to create 2nd index: %v", err)
	}

	rand.Seed(*seed)

	log.Printf("Start validating test")
	test(cassandraIndex)
	log.Printf("Re-run validating test")
	test(cassandraIndex)
	log.Printf("Re-run validating test on fresh index")
	test(cassandraIndex2)

	rnd := rand.New(rand.NewSource(*seed))
	bench(cassandraIndex, rnd, squirrelStates)

	result, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range result {
		_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
	}
}

func drop(session *gocql.Session) {
	queries := []string{
		"DROP TABLE IF EXISTS index_labels2id",
		"DROP TABLE IF EXISTS index_postings",
		"DROP TABLE IF EXISTS index_id2labels",
		"DROP TABLE IF EXISTS index_expiration",
	}
	for _, query := range queries {
		if err := session.Query(query).Exec(); err != nil {
			log.Fatalf("Unable to drop table: %v", err)
		}
	}
}

func map2Labels(input map[string]string) []prompb.Label {
	result := make([]prompb.Label, 0, len(input))
	for k, v := range input {
		result = append(result, prompb.Label{
			Name:  k,
			Value: v,
		})
	}

	return result
}

func labels2Map(input []prompb.Label) map[string]string {
	result := make(map[string]string, len(input))
	for _, l := range input {
		result[l.Name] = l.Value
	}

	return result
}

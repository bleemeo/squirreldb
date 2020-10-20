package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"squirreldb/cassandra/index"
	"squirreldb/cassandra/locks"
	"squirreldb/cassandra/session"
	"squirreldb/cassandra/states"
	"squirreldb/debug"
	"squirreldb/types"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// nolint: gochecknoglobals
var (
	cassandraAddresses        = flag.String("cassandra.addresses", "localhost:9042", "Cassandra cluster addresses")
	cassandraKeyspace         = flag.String("cassandra.keyspace", "squirreldb", "Cassandra keyspace")
	cassanraReplicationFactor = flag.Int("cassandra.replication", 1, "Cassandra replication factor")
	defaultTimeToLive         = flag.Duration("index.ttl", 365*24*time.Hour, "Default time to live")
	doExport                  = flag.Bool("export", false, "Do index export to stdout")
	doImport                  = flag.Bool("import", false, "Do index import from stdin")
	verify                    = flag.Bool("verify", false, "Run the index verification process")
	fix                       = flag.Bool("fix", false, "During the index verification, fix issues")
	dropTables                = flag.Bool("drop-tables", false, "Drop table of index before processing")
	expirationText            = flag.String("expiration", time.Now().Add(365*24*time.Hour).Format(time.RFC3339), "Expiration of imported metrics")
)

func makeSession() (*gocql.Session, bool) {
	cassandraSession, keyspaceCreated, err := session.New(session.Options{
		Addresses:         strings.Split(*cassandraAddresses, ","),
		ReplicationFactor: *cassanraReplicationFactor,
		Keyspace:          *cassandraKeyspace,
	})
	if err != nil {
		log.Fatalf("Unable to open Cassandra session: %v", err)
	}

	return cassandraSession, keyspaceCreated
}

func makeIndex() *index.CassandraIndex {
	cassandraSession, keyspaceCreated := makeSession()

	squirrelLocks, err := locks.New(cassandraSession, keyspaceCreated)
	if err != nil {
		log.Fatalf("Unable to create locks: %v", err)
	}

	squirrelStates, err := states.New(cassandraSession, squirrelLocks.CreateLock("schema-lock", 10*time.Second))
	if err != nil {
		log.Fatalf("Unable to create states: %v", err)
	}

	cassandraIndex, err := index.New(cassandraSession, index.Options{
		DefaultTimeToLive: *defaultTimeToLive,
		LockFactory:       squirrelLocks,
		States:            squirrelStates,
		SchemaLock:        squirrelLocks.CreateLock("schema-lock", 10*time.Second),
	})
	if err != nil {
		log.Fatalf("Unable to create index: %v", err)
	}

	return cassandraIndex
}

func main() {
	flag.Parse()

	debug.Level = 1

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

	if *dropTables {
		session, _ := makeSession()

		err := index.InternalDropTables(session)
		if err != nil {
			log.Fatalf("unable to drop tables: %v", err)
		}
	}

	cassandraIndex := makeIndex()

	if *doImport {
		err := runImport(cassandraIndex)
		if err != nil {
			log.Fatalf("unable to import: %v", err)
		}
	}

	if *verify {
		verifyHadIssue, err := cassandraIndex.Verify(context.Background(), os.Stderr, *fix, *fix)

		if err != nil {
			log.Fatal(err)
		}

		if verifyHadIssue {
			log.Fatal("verify failed")
		}
	}

	if *doExport {
		err := runExport(cassandraIndex)
		if err != nil {
			log.Fatalf("unable to export: %v", err)
		}
	}

	result, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range result {
		_, _ = expfmt.MetricFamilyToText(os.Stderr, mf)
	}
}

func runImport(cassandraIndex *index.CassandraIndex) error {
	reader := csv.NewReader(os.Stdin)
	metrics := make([]labels.Labels, 1000)
	ids := make([]types.MetricID, 1000)
	expirations := make([]time.Time, 1000)

	expiration, err := time.Parse(time.RFC3339, *expirationText)
	if err != nil {
		return err
	}

	for {
		metrics = metrics[:0]
		ids = ids[:0]
		expirations = expirations[:0]

		for len(ids) < 1000 {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}

			if err != nil {
				return nil
			}

			if len(record) != 2 {
				return errors.New("unknown CSV format, expect 2 column per row")
			}

			id, err := strconv.ParseInt(record[0], 10, 0)
			if err != nil {
				return err
			}

			ids = append(ids, types.MetricID(id))

			lbls, err := parser.ParseMetric(record[1])
			if err != nil {
				return err
			}

			metrics = append(metrics, lbls)
			expirations = append(expirations, expiration)
		}

		if len(metrics) == 0 {
			break
		}

		err := cassandraIndex.InternalCreateMetric(metrics, ids, expirations)
		if err != nil {
			return err
		}
	}

	return nil
}

func runExport(cassandraIndex *index.CassandraIndex) error {
	writer := csv.NewWriter(os.Stdout)

	ids, err := cassandraIndex.AllIDs()
	if err != nil {
		return err
	}

	for start := 0; start < len(ids); start += 1000 {
		end := start + 1000
		if end > len(ids) {
			end = len(ids)
		}

		lbls, err := cassandraIndex.LookupLabels(ids[start:end])
		if err != nil {
			return err
		}

		for i, id := range ids[start:end] {
			err := writer.Write([]string{
				strconv.FormatInt(int64(id), 10),
				lbls[i].String(),
			})
			if err != nil {
				return err
			}
		}
	}

	writer.Flush()

	return nil
}
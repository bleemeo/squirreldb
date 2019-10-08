package cassandra

import (
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"log"
	"os"
	"squirreldb/config"
	"time"
)

var (
	keyspace            = config.CassandraKeyspace
	dataTable           = config.CassandraKeyspace + "." + config.CassandraDataTable
	aggregatedDataTable = config.CassandraKeyspace + "." + config.CassandraAggregatedDataTable
	logger              = log.New(os.Stdout, "[cassandra] ", log.LstdFlags)
	exponentialBackOff  = &backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: 0.5,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
)

type Cassandra struct {
	session *gocql.Session
}

// NewCassandra creates a new Cassandra object
func NewCassandra() *Cassandra {
	return &Cassandra{}
}

// CloseSession closes Cassandra's session
func (c *Cassandra) CloseSession() {
	c.session.Close()
}

// InitSession initializes Cassandra's session and create keyspace and metrics table
func (c *Cassandra) InitSession(hosts ...string) error {
	cluster := gocql.NewCluster(hosts...)
	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	createKeyspace := session.Query(
		"CREATE KEYSPACE IF NOT EXISTS " + keyspace + " " +
			"WITH REPLICATION = {" +
			"'class' : 'SimpleStrategy'," +
			"'replication_factor' : " + config.C.String("cassandra.replication_factor") + "};",
	)

	if err := createKeyspace.Exec(); err != nil {
		session.Close()
		return err
	}

	createDataTable := session.Query(
		"CREATE TABLE IF NOT EXISTS " + dataTable + " (" +
			"metric_uuid uuid," +
			"base_ts bigint," +
			"offset_ts int," +
			"insert_time timeuuid," +
			"values blob," +
			"PRIMARY KEY ((metric_uuid, base_ts), offset_ts, insert_time)) " +
			"WITH CLUSTERING ORDER BY (offset_ts DESC) " +
			"AND compression = {" +
			"'chunk_length_in_kb': '256'," +
			"'class': 'org.apache.cassandra.io.compress.DeflateCompressor'} " +
			"AND compaction = {" +
			"'class': 'TimeWindowCompactionStrategy'," +
			"'compaction_window_unit': 'DAYS'," +
			"'compaction_window_size': 6} " +
			"AND default_time_to_live = " + config.C.String("cassandra.time_to_live"),
	)

	if err := createDataTable.Exec(); err != nil {
		session.Close()
		return err
	}

	createAggregatedDataTable := session.Query(
		"CREATE TABLE IF NOT EXISTS " + aggregatedDataTable + " (" +
			"metric_uuid uuid," +
			"base_ts bigint," +
			"offset_ts int," +
			"insert_time timeuuid," +
			"values blob," +
			"PRIMARY KEY ((metric_uuid, base_ts), offset_ts, insert_time)) " +
			"WITH CLUSTERING ORDER BY (offset_ts DESC) " +
			"AND compression = {" +
			"'chunk_length_in_kb': '256'," +
			"'class': 'org.apache.cassandra.io.compress.DeflateCompressor'} " +
			"AND compaction = {" +
			"'class': 'TimeWindowCompactionStrategy'," +
			"'compaction_window_unit': 'DAYS'," +
			"'compaction_window_size': 90} " +
			"AND default_time_to_live = " + config.C.String("cassandra.time_to_live"),
	)

	if err := createAggregatedDataTable.Exec(); err != nil {
		session.Close()
		return err
	}

	c.session = session

	return nil
}

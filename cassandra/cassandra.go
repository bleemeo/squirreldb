package cassandra

import (
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"squirreldb/config"
	"strconv"
	"time"
)

var (
	keyspace           = config.CassandraKeyspace
	metricsTable       = config.CassandraKeyspace + "." + config.CassandraMetricsTable
	exponentialBackOff = &backoff.ExponentialBackOff{
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
			"'replication_factor' : " + strconv.FormatInt(config.CassandraReplicationFactor, 10) + "};",
	)

	if err := createKeyspace.Exec(); err != nil {
		session.Close()
		return err
	}

	timeToLiveSecs := int64(config.CassandraMetricRetention.Seconds())

	createTable := session.Query(
		"CREATE TABLE IF NOT EXISTS " + metricsTable + " (" +
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
			"AND default_time_to_live = " + strconv.FormatInt(timeToLiveSecs, 10),
	)

	if err := createTable.Exec(); err != nil {
		session.Close()
		return err
	}

	c.session = session

	return nil
}

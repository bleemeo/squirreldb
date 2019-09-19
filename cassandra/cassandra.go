package cassandra

import (
	"github.com/gocql/gocql"
	"hamsterdb/config"
	"strconv"
)

var (
	keyspace     = config.CassandraKeyspace
	metricsTable = config.CassandraKeyspace + "." + config.CassandraMetricsTable
)

type Cassandra struct {
	session *gocql.Session
}

func NewCassandra() *Cassandra {
	return &Cassandra{}
}

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

	createTable := session.Query(
		"CREATE TABLE IF NOT EXISTS " + metricsTable + " (" +
			"labels text," +
			"timestamp bigint," +
			"offset_timestamp int," +
			"insert_time timeuuid," +
			"values blob," +
			"PRIMARY KEY ((labels, timestamp), offset_timestamp, insert_time)) " +
			"WITH CLUSTERING ORDER BY (offset_timestamp DESC) " +
			"AND compression = {" +
			"'chunk_length_in_kb': '256'," +
			"'class': 'org.apache.cassandra.io.compress.DeflateCompressor'} " +
			"AND compaction = {" +
			"'class': 'TimeWindowCompactionStrategy'," +
			"'compaction_window_unit': 'DAYS'," +
			"'compaction_window_size': 6} " +
			"AND default_time_to_live = " + strconv.FormatInt(config.CassandraMetricRetention*86400, 10),
	)

	if err := createTable.Exec(); err != nil {
		session.Close()
		return err
	}

	c.session = session

	return nil
}

func (c *Cassandra) CloseSession() {
	c.session.Close()
}

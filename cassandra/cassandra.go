package cassandra

import (
	"crypto/md5"
	"github.com/gocql/gocql"
	"hamsterdb/config"
	"hamsterdb/types"
	"log"
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

func (c *Cassandra) CloseSession() {
	c.session.Close()
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
			"AND default_time_to_live = " + strconv.FormatInt(config.CassandraMetricRetention, 10),
	)

	if err := createTable.Exec(); err != nil {
		session.Close()
		return err
	}

	c.session = session

	return nil
}

func MetricUUID(m types.Metric) gocql.UUID {
	uuidString, exists := m.Labels["__uuid__"]
	var uuid gocql.UUID
	var err error

	if !exists {
		hash := md5.New()
		labels := m.CanonicalLabels()

		hash.Write([]byte(labels))

		hashed := hash.Sum(nil)

		uuid, err = gocql.UUIDFromBytes(hashed)

		if err != nil {
			log.Printf("MetricUUID: Can't generate UUID from bytes (%v)"+"\n", err)
		}
	} else {
		uuid, err = gocql.ParseUUID(uuidString)

		if err != nil {
			log.Printf("MetricUUID: Can't generate UUID from string (%v)"+"\n", err)
		}
	}

	return uuid
}

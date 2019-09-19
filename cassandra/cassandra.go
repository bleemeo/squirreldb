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
			"timestamp bigint," +
			"offset_timestamp int," +
			"insert_time timeuuid," +
			"values blob," +
			"PRIMARY KEY ((metric_uuid, timestamp), offset_timestamp, insert_time)) " +
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

func MetricUUID(m *types.Metric) gocql.UUID {
	var err error
	var uuid gocql.UUID
	var uuidBytes []byte
	uuidString, exists := m.Labels["__uuid__"]

	if !exists {
		hash := md5.New()

		hash.Write([]byte(m.CanonicalLabels()))

		uuidBytes = hash.Sum(nil)

		uuid, err = gocql.UUIDFromBytes(uuidBytes)

		if err != nil {
			log.Printf("MetricUUID: %v"+"\n", err)
		}
	} else {
		uuid, err = gocql.ParseUUID(uuidString)

		if err != nil {
			log.Printf("MetricUUID: %v"+"\n", err)
		}

		delete(m.Labels, "__uuid__")
	}

	return uuid
}

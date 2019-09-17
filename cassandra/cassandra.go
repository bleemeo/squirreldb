package cassandra

import (
	"github.com/gocql/gocql"
	"hamsterdb/config"
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

func (cassandra *Cassandra) InitSession(hosts ...string) error {
	cluster := gocql.NewCluster(hosts...)
	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	cassandra.session = session

	// Create keyspace
	keyspaceQuery := session.Query(
		"CREATE KEYSPACE IF NOT EXISTS " + keyspace + " " +
			"WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")

	if err := keyspaceQuery.Exec(); err != nil {
		return err
	}

	// Create metrics table
	metricsQuery := session.Query(
		"CREATE TABLE IF NOT EXISTS " + metricsTable + " (" +
			"labels text," +
			"timestamp bigint," +
			"values blob," +
			"PRIMARY KEY (labels, timestamp))" +
			"WITH CLUSTERING ORDER BY (timestamp DESC);")

	if err := metricsQuery.Exec(); err != nil {
		return err
	}

	return nil
}

func (cassandra *Cassandra) CloseSession() {
	cassandra.session.Close()
}

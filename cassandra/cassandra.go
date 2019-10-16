package cassandra

import (
	"github.com/gocql/gocql"
	"log"
	"os"
	"squirreldb/config"
	"strings"
)

var (
	keyspace            = config.CassandraKeyspace
	dataTable           = config.CassandraKeyspace + "." + config.CassandraDataTable
	aggregatedDataTable = config.CassandraKeyspace + "." + config.CassandraAggregatedDataTable
	logger              = log.New(os.Stdout, "[store] ", log.LstdFlags)
)

type Cassandra struct {
	session *gocql.Session
}

// New creates a new Cassandra object
func New() *Cassandra {
	return &Cassandra{}
}

// Close closes Cassandra
func (c *Cassandra) Close() {
	c.session.Close()
}

// Init initializes session and create keyspace, data and aggregated data tables
func (c *Cassandra) Init(hosts ...string) error {
	cluster := gocql.NewCluster(hosts...)
	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	replicationFactor := config.C.String("cassandra.replication_factor")
	createKeyspaceReplacer := strings.NewReplacer("$KEYSPACE", keyspace, "$REPLICATION_FACTOR", replicationFactor)
	createKeyspace := session.Query(createKeyspaceReplacer.Replace(`
		CREATE KEYSPACE IF NOT EXISTS $KEYSPACE_NAME
		WITH REPLICATION = {
			'class': 'SimpleStrategy',
			'replication_factor': $REPLICATION_FACTOR
		};
	`))

	if err := createKeyspace.Exec(); err != nil {
		session.Close()
		return err
	}

	defaultTimeToLive := config.C.String("cassandra.default_time_to_live")
	createDataTableReplacer := strings.NewReplacer("$DATA_TABLE", dataTable, "$DEFAULT_TIME_TO_LIVE", defaultTimeToLive)
	createDataTable := session.Query(createDataTableReplacer.Replace(`
        CREATE TABLE IF NOT EXISTS $DATA_TABLE (
			metric_uuid uuid,
			base_ts bigint,
			offset_ts int,
			insert_time timeuuid,
			values blob,
			PRIMARY KEY ((metric_uuid, base_ts), offset_ts, insert_time)
		)
		WITH CLUSTERING ORDER BY (offset_ts DESC)
		AND COMPRESSION = {
			'chunk_length_in_kb': '256',
			'class': 'org.apache.cassandra.io.compress.DeflateCompressor'
		}
		AND COMPACTION = {
			'class': 'TimeWindowCompactionStrategy',
			'compaction_window_unit': 'DAYS',
			'compaction_window_size': 6
		}
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE
	`))

	if err := createDataTable.Exec(); err != nil {
		session.Close()
		return err
	}

	createAggregatedDataTableReplacer := strings.NewReplacer("$AGGREGATED_DATA_TABLE", aggregatedDataTable, "$DEFAULT_TIME_TO_LIVE", defaultTimeToLive)
	createAggregatedDataTable := session.Query(createAggregatedDataTableReplacer.Replace(`
        CREATE TABLE IF NOT EXISTS $AGGREGATED_DATA_TABLE (
			metric_uuid uuid,
			base_ts bigint,
			offset_ts int,
			insert_time timeuuid,
			values blob,
			PRIMARY KEY ((metric_uuid, base_ts), offset_ts, insert_time)
		)
		WITH CLUSTERING ORDER BY (offset_ts DESC)
		AND COMPRESSION = {
			'chunk_length_in_kb': '256',
			'class': 'org.apache.cassandra.io.compress.DeflateCompressor'
		}
		AND COMPACTION = {
			'class': 'TimeWindowCompactionStrategy',
			'compaction_window_unit': 'DAYS',
			'compaction_window_size': 6
		}
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE
	`))

	if err := createAggregatedDataTable.Exec(); err != nil {
		session.Close()
		return err
	}

	c.session = session

	return nil
}

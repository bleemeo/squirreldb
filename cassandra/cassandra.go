package cassandra

import (
	"github.com/gocql/gocql"
	"log"
	"os"
	"squirreldb/types"
	"strconv"
	"strings"
)

const (
	DataTable          = "data"
	AggregateDataTable = "data_aggregated"
	IndexTable         = "index"
	StatesTable        = "states"
)

var logger = log.New(os.Stdout, "[cassandra] ", log.LstdFlags)

type Options struct {
	Addresses              []string
	ReplicationFactor      int
	Keyspace               string
	DefaultTimeToLive      int64
	BatchSize              int64
	RawPartitionSize       int64
	AggregateResolution    int64
	AggregateSize          int64
	AggregateStartOffset   int64
	AggregatePartitionSize int64

	DebugAggregateForce bool
	DebugAggregateSize  int64

	statesTable        string
	dataTable          string
	aggregateDataTable string
	indexTable         string
}

type Cassandra struct {
	session *gocql.Session
	indexer types.MetricIndexer
	options Options
}

// New creates a new Cassandra object
func New(options Options) (*Cassandra, error) {
	cluster := gocql.NewCluster(options.Addresses...)
	session, err := cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	session.SetConsistency(gocql.LocalQuorum)

	options.dataTable = options.Keyspace + "." + DataTable
	options.aggregateDataTable = options.Keyspace + "." + AggregateDataTable
	options.indexTable = options.Keyspace + "." + "\"" + IndexTable + "\""
	options.statesTable = options.Keyspace + "." + StatesTable

	replicationFactor := strconv.FormatInt(int64(options.ReplicationFactor), 10)

	createKeyspaceReplacer := strings.NewReplacer("$KEYSPACE", options.Keyspace, "$REPLICATION_FACTOR", replicationFactor)
	createKeyspace := session.Query(createKeyspaceReplacer.Replace(`
		CREATE KEYSPACE IF NOT EXISTS $KEYSPACE
		WITH REPLICATION = {
			'class': 'SimpleStrategy',
			'replication_factor': $REPLICATION_FACTOR
		};
	`))

	if err := createKeyspace.Exec(); err != nil {
		session.Close()
		return nil, err
	}

	defaultTimeToLive := strconv.FormatInt(options.DefaultTimeToLive, 10)

	createDataTableReplacer := strings.NewReplacer("$DATA_TABLE", options.dataTable, "$DEFAULT_TIME_TO_LIVE", defaultTimeToLive)
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
		return nil, err
	}

	createAggregatedDataTableReplacer := strings.NewReplacer("$AGGREGATED_DATA_TABLE", options.aggregateDataTable, "$DEFAULT_TIME_TO_LIVE", defaultTimeToLive)
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
			'compaction_window_size': 90
		}
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE
	`))

	if err := createAggregatedDataTable.Exec(); err != nil {
		session.Close()
		return nil, err
	}

	createIndexTableReplacer := strings.NewReplacer("$INDEX_TABLE", options.indexTable)
	createIndexTable := session.Query(createIndexTableReplacer.Replace(`
		CREATE TABLE IF NOT EXISTS $INDEX_TABLE (
			metric_uuid uuid,
			labels map<text, text>,
			PRIMARY KEY (metric_uuid)
		)
	`))

	if err := createIndexTable.Exec(); err != nil {
		session.Close()
		return nil, err
	}

	createStatesTableReplacer := strings.NewReplacer("$STATES_TABLE", options.statesTable)
	createStatesTable := session.Query(createStatesTableReplacer.Replace(`
		CREATE TABLE IF NOT EXISTS $STATES_TABLE (
			name text,
			value text,
			PRIMARY KEY (name)
		)
	`))

	if err := createStatesTable.Exec(); err != nil {
		session.Close()
		return nil, err
	}

	cassandra := Cassandra{
		session: session,
		options: options,
	}

	return &cassandra, nil
}

// Close closes Cassandra
func (c *Cassandra) Close() {
	c.session.Close()
}

package tsdb

import (
	"time"

	"github.com/gocql/gocql"

	"log"
	"os"
	"squirreldb/types"
	"strconv"
	"strings"
)

const (
	dataTableName          = "data"
	aggregateDataTableName = "data_aggregated"
	retryMaxDelay          = 30 * time.Second
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[tsdb] ", log.LstdFlags)

type Options struct {
	DefaultTimeToLive         int64
	BatchSize                 int64
	RawPartitionSize          int64
	AggregatePartitionSize    int64
	AggregateResolution       int64
	AggregateSize             int64
	AggregateIntendedDuration int64

	dataTable          string
	aggregateDataTable string
}

type CassandraTSDB struct {
	session *gocql.Session
	options Options

	index  types.Index
	locker types.Locker
	state  types.State
}

// New created a new CassandraTSDB object
func New(session *gocql.Session, keyspace string, options Options, index types.Index, locker types.Locker, state types.State) (*CassandraTSDB, error) {
	options.dataTable = keyspace + "." + dataTableName
	options.aggregateDataTable = keyspace + "." + aggregateDataTableName
	defaultTimeToLive := strconv.FormatInt(options.DefaultTimeToLive, 10)
	dataTableCreateQuery := dataTableCreateQuery(session, options.dataTable, defaultTimeToLive)

	if err := dataTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	aggregateDataTableCreateQuery := aggregateDataTableCreateQuery(session, options.aggregateDataTable, defaultTimeToLive)

	if err := aggregateDataTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	tsdb := &CassandraTSDB{
		session: session,
		options: options,
		index:   index,
		locker:  locker,
		state:   state,
	}

	return tsdb, nil
}

// Returns data table create Query
func dataTableCreateQuery(session *gocql.Session, dataTable, defaultTimeToLive string) *gocql.Query {
	replacer := strings.NewReplacer("$DATA_TABLE", dataTable, "$DEFAULT_TIME_TO_LIVE", defaultTimeToLive)
	query := session.Query(replacer.Replace(`
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

	return query
}

// Returns aggregate data table create Query
func aggregateDataTableCreateQuery(session *gocql.Session, aggregateDataTable, defaultTimeToLive string) *gocql.Query {
	replacer := strings.NewReplacer("$AGGREGATED_DATA_TABLE", aggregateDataTable, "$DEFAULT_TIME_TO_LIVE", defaultTimeToLive)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $AGGREGATED_DATA_TABLE (
			metric_uuid uuid,
			base_ts bigint,
			offset_ts int,
			values blob,
			PRIMARY KEY ((metric_uuid, base_ts), offset_ts)
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

	return query
}
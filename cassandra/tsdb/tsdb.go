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
	retryMaxDelay = 30 * time.Second
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
}

type lockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type CassandraTSDB struct {
	session *gocql.Session
	options Options

	index       types.Index
	lockFactory lockFactory
	state       types.State
}

// New created a new CassandraTSDB object
func New(session *gocql.Session, options Options, index types.Index, lockFactory lockFactory, state types.State) (*CassandraTSDB, error) {
	defaultTimeToLive := strconv.FormatInt(options.DefaultTimeToLive, 10)
	dataTableCreateQuery := dataTableCreateQuery(session, defaultTimeToLive)

	if err := dataTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	aggregateDataTableCreateQuery := aggregateDataTableCreateQuery(session, defaultTimeToLive)

	if err := aggregateDataTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	tsdb := &CassandraTSDB{
		session:     session,
		options:     options,
		index:       index,
		lockFactory: lockFactory,
		state:       state,
	}

	return tsdb, nil
}

// Returns data table create Query
func dataTableCreateQuery(session *gocql.Session, defaultTimeToLive string) *gocql.Query {
	replacer := strings.NewReplacer("$DEFAULT_TIME_TO_LIVE", defaultTimeToLive)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS data (
			metric_uuid uuid,
			base_ts bigint,
			offset_ms int,
			insert_time timeuuid,
			values blob,
			PRIMARY KEY ((metric_uuid, base_ts), offset_ms, insert_time)
		)
		WITH CLUSTERING ORDER BY (offset_ms DESC)
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
func aggregateDataTableCreateQuery(session *gocql.Session, defaultTimeToLive string) *gocql.Query {
	replacer := strings.NewReplacer("$DEFAULT_TIME_TO_LIVE", defaultTimeToLive)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS data_aggregated (
			metric_uuid uuid,
			base_ts bigint,
			offset_second int,
			values blob,
			PRIMARY KEY ((metric_uuid, base_ts), offset_second)
		)
		WITH CLUSTERING ORDER BY (offset_second DESC)
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

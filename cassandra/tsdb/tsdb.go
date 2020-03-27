package tsdb

import (
	"sync"
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
	DefaultTimeToLive         time.Duration
	BatchSize                 time.Duration
	RawPartitionSize          time.Duration
	AggregatePartitionSize    time.Duration
	AggregateResolution       time.Duration
	AggregateSize             time.Duration
	AggregateIntendedDuration time.Duration
	SchemaLock                sync.Locker
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
	options.SchemaLock.Lock()
	defer options.SchemaLock.Unlock()

	dataTableCreateQuery := dataTableCreateQuery(session, options.DefaultTimeToLive)
	dataTableCreateQuery.Consistency(gocql.All)

	if err := dataTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	aggregateDataTableCreateQuery := aggregateDataTableCreateQuery(session, options.DefaultTimeToLive)
	aggregateDataTableCreateQuery.Consistency(gocql.All)

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
func dataTableCreateQuery(session *gocql.Session, defaultTimeToLive time.Duration) *gocql.Query {
	replacer := strings.NewReplacer("$DEFAULT_TIME_TO_LIVE", strconv.FormatInt(int64(defaultTimeToLive.Seconds()), 10))
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS data (
			metric_id bigint,
			base_ts bigint,
			offset_ms int,
			insert_time timeuuid,
			values blob,
			PRIMARY KEY ((metric_id, base_ts), offset_ms, insert_time)
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
func aggregateDataTableCreateQuery(session *gocql.Session, defaultTimeToLive time.Duration) *gocql.Query {
	replacer := strings.NewReplacer("$DEFAULT_TIME_TO_LIVE", strconv.FormatInt(int64(defaultTimeToLive.Seconds()), 10))
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS data_aggregated (
			metric_id bigint,
			base_ts bigint,
			offset_second int,
			values blob,
			PRIMARY KEY ((metric_id, base_ts), offset_second)
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

package tsdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"squirreldb/types"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	rawPartitionSize       = 5 * 24 * time.Hour
	aggregatePartitionSize = 80 * 24 * time.Hour
	aggregateResolution    = 5 * time.Minute
	aggregateSize          = 24 * time.Hour
)

const (
	retryMaxDelay = 30 * time.Second
	// Format write in Cassandra changed from this date.
	// Only the first timestamp count (base_ts + offset_ts).
	// All SquirrelDB must be updated before this date.
	newFormatFrom = "2021-06-15T09:00:00Z"
)

var (
	errPointsEmptyValues = errors.New("empty points values")
	errUnsupportedFormat = errors.New("unsupporter format version")
)

//nolint:gochecknoglobals
var logger = log.New(os.Stdout, "[tsdb] ", log.LstdFlags)

type Options struct {
	DefaultTimeToLive         time.Duration
	AggregateIntendedDuration time.Duration
	SchemaLock                sync.Locker
}

type lockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type CassandraTSDB struct {
	session *gocql.Session
	options Options
	metrics *metrics

	wg     sync.WaitGroup
	cancel context.CancelFunc

	l                 sync.Mutex
	fullyAggregatedAt time.Time

	index            types.Index
	lockFactory      lockFactory
	state            types.State
	newFormatCutoff  int64
	pointsBufferPool sync.Pool
	bytesPool        sync.Pool
	xorChunkPool     chunkenc.Pool
}

// New created a new CassandraTSDB object.
func New(
	reg prometheus.Registerer,
	session *gocql.Session,
	options Options,
	index types.Index,
	lockFactory lockFactory,
	state types.State,
) (*CassandraTSDB, error) {
	options.SchemaLock.Lock()
	defer options.SchemaLock.Unlock()

	newFormatCutoff, err := time.Parse(time.RFC3339, newFormatFrom)
	if err != nil {
		return nil, fmt.Errorf("invalid newFormatFrom: %w", err)
	}

	dataTableCreateQuery := dataTableCreateQuery(session, options.DefaultTimeToLive)
	dataTableCreateQuery.Consistency(gocql.All)

	if err := dataTableCreateQuery.Exec(); err != nil {
		return nil, fmt.Errorf("create table data: %w", err)
	}

	aggregateDataTableCreateQuery := aggregateDataTableCreateQuery(session, options.DefaultTimeToLive)
	aggregateDataTableCreateQuery.Consistency(gocql.All)

	if err := aggregateDataTableCreateQuery.Exec(); err != nil {
		return nil, fmt.Errorf("create table data_aggregated: %w", err)
	}

	tsdb := &CassandraTSDB{
		session:     session,
		options:     options,
		metrics:     newMetrics(reg),
		index:       index,
		lockFactory: lockFactory,
		state:       state,
		pointsBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]types.MetricPoint, 15)
			},
		},
		bytesPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 15)
			},
		},
		xorChunkPool:    chunkenc.NewPool(),
		newFormatCutoff: newFormatCutoff.Unix(),
	}

	return tsdb, nil
}

// Start starts all Cassandra Index services.
func (c *CassandraTSDB) Start(_ context.Context) error {
	if c.cancel != nil {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)

	go c.run(ctx)

	return nil
}

// Stop stop and wait all Cassandra Index services.
func (c *CassandraTSDB) Stop() error {
	if c.cancel == nil {
		return errors.New("not started")
	}

	c.cancel()
	c.cancel = nil
	c.wg.Wait()

	return nil
}

func (c *CassandraTSDB) getPointsBuffer() []types.MetricPoint {
	pbuffer, ok := c.pointsBufferPool.Get().(*[]types.MetricPoint)

	var buffer []types.MetricPoint
	if ok {
		buffer = (*pbuffer)[:0]
	}

	return buffer
}

func (c *CassandraTSDB) putPointsBuffer(v []types.MetricPoint) {
	// Don't kept too large buffer in the pool
	if len(v) > 10000 {
		return
	}

	c.pointsBufferPool.Put(&v)
}

// Returns data table create Query.
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
		AND COMPACTION = {
			'class': 'TimeWindowCompactionStrategy',
			'compaction_window_unit': 'DAYS',
			'compaction_window_size': 6
		}
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE
	`))

	return query
}

// Returns aggregate data table create Query.
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
		AND COMPACTION = {
			'class': 'TimeWindowCompactionStrategy',
			'compaction_window_unit': 'DAYS',
			'compaction_window_size': 90
		}
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE
	`))

	return query
}

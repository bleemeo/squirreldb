package tsdb

import (
	"context"
	"errors"
	"fmt"
	"squirreldb/logger"
	"squirreldb/types"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/rs/zerolog"
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

type Options struct {
	SchemaLock                sync.Locker
	DefaultTimeToLive         time.Duration
	AggregateIntendedDuration time.Duration
}

type lockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type CassandraTSDB struct {
	session *gocql.Session
	options Options
	metrics *metrics
	logger  zerolog.Logger

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
	ctx context.Context,
	reg prometheus.Registerer,
	session *gocql.Session,
	options Options,
	index types.Index,
	lockFactory lockFactory,
	state types.State,
	logger zerolog.Logger,
) (*CassandraTSDB, error) {
	options.SchemaLock.Lock()
	defer options.SchemaLock.Unlock()

	newFormatCutoff, err := time.Parse(time.RFC3339, newFormatFrom)
	if err != nil {
		return nil, fmt.Errorf("invalid newFormatFrom: %w", err)
	}

	if err := dataTableCreate(ctx, session, options.DefaultTimeToLive); err != nil {
		return nil, fmt.Errorf("create table data: %w", err)
	}

	if err := aggregateDataTableCreate(ctx, session, options.DefaultTimeToLive); err != nil {
		return nil, fmt.Errorf("create table data_aggregated: %w", err)
	}

	tsdb := &CassandraTSDB{
		session:     session,
		options:     options,
		metrics:     newMetrics(reg),
		logger:      logger,
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

	go func() {
		defer logger.ProcessPanic()

		c.run(ctx)
	}()

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

func dataTableCreate(ctx context.Context, session *gocql.Session, defaultTimeToLive time.Duration) error {
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
			'compaction_window_size': 6,
			'tombstone_threshold': 0.2,
			'unchecked_tombstone_compaction': true,
			'tombstone_compaction_interval': 604800
		}
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE`),
	).Consistency(gocql.All).WithContext(ctx)

	return query.Exec()
}

func aggregateDataTableCreate(ctx context.Context, session *gocql.Session, defaultTimeToLive time.Duration) error {
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
			'compaction_window_size': 90,
			'tombstone_threshold': 0.2,
			'unchecked_tombstone_compaction': true,
			'tombstone_compaction_interval': 8467200
		}
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE`),
	).Consistency(gocql.All).WithContext(ctx)

	return query.Exec()
}

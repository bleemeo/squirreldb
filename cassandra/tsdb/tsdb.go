// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/batch"
	"github.com/bleemeo/squirreldb/cassandra/connection"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy/temporarystore"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/queryable/convert"
	redisTemporarystore "github.com/bleemeo/squirreldb/redis/temporarystore"
	"github.com/bleemeo/squirreldb/types"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/rs/zerolog"
)

const (
	rawPartitionSize       = 5 * 24 * time.Hour
	aggregatePartitionSize = 80 * 24 * time.Hour
	aggregateResolution    = 5 * time.Minute
	aggregateSize          = 24 * time.Hour
)

const retryMaxDelay = 30 * time.Second

var (
	errPointsEmptyValues = errors.New("empty points values")
	errUnsupportedFormat = errors.New("unsupported format version")
	errNotImplemented    = errors.New("not implemented")
)

type Options struct {
	SchemaLock                sync.Locker
	DefaultTimeToLive         time.Duration
	AggregateIntendedDuration func() time.Duration
	ReadOnly                  bool
	DisablePreAggregation     bool
	BatchSize                 time.Duration
	Redis                     config.Redis // If Redis is unset, use an in-memory store for batching
}

type lockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type CassandraTSDB struct {
	connection *connection.Connection
	options    Options
	metrics    *metrics
	logger     zerolog.Logger

	wg     sync.WaitGroup
	cancel context.CancelFunc

	l                    sync.Mutex
	fullyAggregatedAt    time.Time
	lastLogPointInPastAt time.Time

	index            types.Index
	lockFactory      lockFactory
	state            types.State
	batcher          *batch.Batch
	pointsBufferPool sync.Pool
	bytesPool        sync.Pool
	xorChunkPool     chunkenc.Pool
}

// New created a new CassandraTSDB object.
func New(
	ctx context.Context,
	reg prometheus.Registerer,
	connection *connection.Connection,
	options Options,
	index types.Index,
	lockFactory lockFactory,
	state types.State,
	logger zerolog.Logger,
) (*CassandraTSDB, error) {
	if options.ReadOnly {
		logger.Debug().Msg("Read-only mode is activated. Not trying to create tables and assuming they exist")
	} else {
		options.SchemaLock.Lock()
		defer options.SchemaLock.Unlock()

		if err := dataTableCreate(ctx, connection, options.DefaultTimeToLive); err != nil {
			return nil, fmt.Errorf("create table data: %w", err)
		}

		if err := aggregateDataTableCreate(ctx, connection, options.DefaultTimeToLive); err != nil {
			return nil, fmt.Errorf("create table data_aggregated: %w", err)
		}
	}

	var temporaryStore batch.TemporaryStore

	if len(options.Redis.Addresses) > 0 && options.Redis.Addresses[0] != "" {
		var err error

		temporaryStore, err = redisTemporarystore.New(
			ctx,
			prometheus.WrapRegistererWith(
				map[string]string{"backend": "cassandra"},
				reg,
			),
			redisTemporarystore.Options{
				RedisOptions: options.Redis,
				Keyspace:     options.Redis.Keyspace,
			},
			logger.With().Str("component", "temporary_store").Logger(),
		)
		if err != nil {
			return nil, err
		}
	} else {
		temporaryStore = temporarystore.New( //nolint:contextcheck
			prometheus.WrapRegistererWith(
				map[string]string{"backend": "cassandra"},
				reg,
			),
			logger.With().Str("component", "temporary_store").Logger(),
		)
	}

	tsdb := &CassandraTSDB{
		connection:  connection,
		options:     options,
		metrics:     newMetrics(reg),
		logger:      logger,
		index:       index,
		lockFactory: lockFactory,
		state:       state,
		batcher: batch.New(
			prometheus.WrapRegistererWith(
				map[string]string{"backend": "cassandra"},
				reg,
			),
			options.BatchSize,
			temporaryStore,
			logger.With().Str("component", "batch").Logger(),
		),
		pointsBufferPool: sync.Pool{
			New: func() any {
				return make([]types.MetricPoint, 15)
			},
		},
		bytesPool: sync.Pool{
			New: func() any {
				return make([]byte, 15)
			},
		},
		xorChunkPool: chunkenc.NewPool(),
	}

	return tsdb, nil
}

// Start starts all Cassandra Index services.
func (c *CassandraTSDB) Start(_ context.Context) error { //nolint: contextcheck
	if c.cancel != nil {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Go(func() {
		defer logger.ProcessPanic()

		c.batcher.Run(ctx, c.writerInterface())
	})

	c.wg.Go(func() {
		defer logger.ProcessPanic()

		c.run(ctx)
	})

	return nil
}

// Stop stop and wait all Cassandra Index services.
func (c *CassandraTSDB) Stop() error {
	if c.cancel == nil {
		c.wg.Wait()

		return nil
	}

	c.cancel()
	c.cancel = nil
	c.wg.Wait()

	return nil
}

func (c *CassandraTSDB) AppenderWithOptions(ctx context.Context, opts types.AppenderOptions) storage.Appender {
	return &batchingAppender{
		c:                 c,
		ttl:               opts.WriteTTL,
		ctx:               ctx,
		pendingTimeSeries: make(map[uint64]types.Timeseries),
	}
}

func (c *CassandraTSDB) ChunkQuerierWithOptions(
	opts types.QuerierOptions,
	mint int64,
	maxt int64,
) (storage.ChunkQuerier, error) {
	q, err := c.QuerierWithOptions(opts, mint, maxt)
	if err != nil {
		return nil, err
	}

	return convert.NewQuerierToChunkQuerier(q), nil
}

func (c *CassandraTSDB) QuerierWithOptions(opts types.QuerierOptions, mint, maxt int64) (storage.Querier, error) {
	backendOption, _ := opts.BackendOptions.(querierOptions)

	return &querier{
		c:             c,
		mint:          mint,
		maxt:          maxt,
		opts:          opts,
		backendOption: backendOption,
	}, nil
}

func (c *CassandraTSDB) Close(context.Context) error {
	return c.Stop()
}

func (c *CassandraTSDB) Index() types.Index {
	return c.index
}

func (c *CassandraTSDB) Flush(ctx context.Context) error {
	return c.batcher.Flush(ctx)
}

func (c *CassandraTSDB) ParseHTTPOptions(header http.Header) (any, error) {
	result := querierOptions{}

	value := header.Get(types.HeaderForcePreAggregated)
	if value != "" {
		tmp, err := strconv.ParseBool(value)
		if err != nil {
			return result, err
		}

		result.ForcePreAggregated = tmp
	}

	value = header.Get(types.HeaderForceRaw)
	if value != "" {
		tmp, err := strconv.ParseBool(value)
		if err != nil {
			return result, err
		}

		result.ForceRaw = tmp
		if result.ForceRaw {
			result.ForcePreAggregated = false
		}
	}

	return result, nil
}

func (c *CassandraTSDB) Querier(mint, maxt int64) (storage.Querier, error) {
	return c.QuerierWithOptions(types.QuerierOptions{}, mint, maxt)
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

// writerInterface do the write to the persistent storage (Cassandra) and should be
// only used by the batcher. The write must be batched with this writer.
func (c *CassandraTSDB) writerInterface() types.MetricWriter {
	return &tsdbWriter{
		c: c,
	}
}

func (c *CassandraTSDB) warnPointTooOld(tMin int64) {
	c.l.Lock()
	defer c.l.Unlock()

	const warnDebounceDelay = 10 * time.Second

	if time.Since(c.lastLogPointInPastAt) > warnDebounceDelay {
		c.logger.Warn().Msgf("Points with timestamp %v will be ignored by pre-aggregation", time.Unix(tMin/1000, 0))

		c.lastLogPointInPastAt = time.Now()
	}
}

func dataTableCreate(ctx context.Context, connection *connection.Connection, defaultTimeToLive time.Duration) error {
	session, err := connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

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
		AND gc_grace_seconds = 86400
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE`),
	).Consistency(gocql.All).WithContext(ctx)

	return query.Exec()
}

func aggregateDataTableCreate(ctx context.Context,
	connection *connection.Connection,
	defaultTimeToLive time.Duration,
) error {
	session, err := connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

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
		AND gc_grace_seconds = 86400
		AND DEFAULT_TIME_TO_LIVE = $DEFAULT_TIME_TO_LIVE`),
	).Consistency(gocql.All).WithContext(ctx)

	return query.Exec()
}

// InternalDecodePoints is an internal function to decode points.
// You should not rely on this function and it might change without warning. Used for squirreldb_debig_tools.
func InternalDecodePoints(buffer []byte) ([]types.MetricPoint, error) {
	c := &CassandraTSDB{
		xorChunkPool: chunkenc.NewPool(),
	}

	return c.decodePoints(buffer, nil)
}

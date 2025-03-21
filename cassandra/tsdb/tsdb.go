// Copyright 2015-2025 Bleemeo
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/connection"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/types"

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

const retryMaxDelay = 30 * time.Second

var (
	errPointsEmptyValues = errors.New("empty points values")
	errUnsupportedFormat = errors.New("unsupporter format version")
)

type Options struct {
	SchemaLock                sync.Locker
	DefaultTimeToLive         time.Duration
	AggregateIntendedDuration func() time.Duration
	ReadOnly                  bool
	DisablePreAggregation     bool
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

	l                 sync.Mutex
	fullyAggregatedAt time.Time

	index            types.Index
	lockFactory      lockFactory
	state            types.State
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

	tsdb := &CassandraTSDB{
		connection:  connection,
		options:     options,
		metrics:     newMetrics(reg),
		logger:      logger,
		index:       index,
		lockFactory: lockFactory,
		state:       state,
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

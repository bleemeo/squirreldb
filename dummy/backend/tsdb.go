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

package backend

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/bleemeo/squirreldb/batch"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy/temporarystore"
	"github.com/bleemeo/squirreldb/queryable/convert"
	redisTemporarystore "github.com/bleemeo/squirreldb/redis/temporarystore"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

// discardWriter is the types.MetricWriter the batcher flushes to: it drops every
// point. It is what makes the dummy backend "write to /dev/null" while still
// exercising the whole batcher + temporary store machinery upstream of it.
type discardWriter struct{}

func (discardWriter) Write(context.Context, []types.MetricData) error {
	return nil
}

// batchingTSDB is a types.TSDB that routes writes through the shared batcher
// (batch.Batch) and a temporary store, then discards the flushed points. It
// mirrors the real backends' write path (clickhouse/tsdb, cassandra/tsdb) minus
// the database write, so a load test can tell whether its bottleneck is the
// batcher/temporary store or the actual storage backend. Reads always return
// empty (nothing is stored).
type batchingTSDB struct {
	batcher *batch.Batch

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// newBatchingTSDB builds the batcher and its temporary store (Redis when
// configured, in-memory otherwise — same contract as the real backends) and
// starts the background flush loop writing to discardWriter.
func newBatchingTSDB(
	ctx context.Context,
	reg prometheus.Registerer,
	batchSize config.Batch,
	redisCfg config.Redis,
	logger zerolog.Logger,
) (*batchingTSDB, error) {
	store, err := newTemporaryStore(ctx, reg, redisCfg, logger)
	if err != nil {
		return nil, err
	}

	t := &batchingTSDB{
		batcher: batch.New(
			prometheus.WrapRegistererWith(
				map[string]string{"backend": "dummy"},
				reg,
			),
			batchSize.Size,
			store,
			logger.With().Str("component", "batch").Logger(),
		),
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t.cancel = cancel

	t.wg.Go(func() {
		t.batcher.Run(runCtx, discardWriter{})
	})

	return t, nil
}

// dummyRedisKeyPrefix is added to the Redis keyspace of the dummy temporary store.
const dummyRedisKeyPrefix = "dummy-"

// newTemporaryStore returns a Redis-backed store when Redis is configured, or an
// in-memory store otherwise — the same logic (and same fail-loudly contract on a
// Redis error) as the Cassandra and ClickHouse backends.
func newTemporaryStore(
	ctx context.Context,
	reg prometheus.Registerer,
	redisCfg config.Redis,
	logger zerolog.Logger,
) (batch.TemporaryStore, error) {
	storeLogger := logger.With().Str("component", "temporary_store").Logger()

	if len(redisCfg.Addresses) > 0 && redisCfg.Addresses[0] != "" {
		store, err := redisTemporarystore.New(
			ctx,
			prometheus.WrapRegistererWith(
				map[string]string{"backend": "dummy"},
				reg,
			),
			redisTemporarystore.Options{
				RedisOptions: redisCfg,
				// The Cassandra batcher uses the same key names: a distinct
				// prefix keeps the dummy backend from sharing the points
				// buffered for Cassandra when both run together.
				Keyspace: redisCfg.Keyspace + dummyRedisKeyPrefix,
			},
			storeLogger,
		)
		if err != nil {
			return nil, fmt.Errorf("redis temporary store: %w", err)
		}

		return store, nil
	}

	return temporarystore.New( //nolint:contextcheck
		prometheus.WrapRegistererWith(
			map[string]string{"backend": "dummy"},
			reg,
		),
		storeLogger,
	), nil
}

func (t *batchingTSDB) AppenderWithOptions(ctx context.Context, opts types.AppenderOptions) storage.Appender {
	return &batchingAppender{
		t:                 t,
		ttl:               opts.WriteTTL,
		ctx:               ctx,
		pendingTimeSeries: make(map[uint64]types.Timeseries),
	}
}

func (t *batchingTSDB) QuerierWithOptions(_ types.QuerierOptions, _, _ int64) (storage.Querier, error) {
	return storage.NoopQuerier(), nil
}

func (t *batchingTSDB) ChunkQuerierWithOptions(_ types.QuerierOptions, _, _ int64) (storage.ChunkQuerier, error) {
	return convert.NewQuerierToChunkQuerier(storage.NoopQuerier()), nil
}

// Flush flushes points still buffered in the batcher to the discard writer. It
// mirrors clickhouse/tsdb's Flush; it is not part of types.TSDB.
func (t *batchingTSDB) Flush(ctx context.Context) error {
	return t.batcher.Flush(ctx, &discardWriter{})
}

func (t *batchingTSDB) Close(context.Context) error {
	if t.cancel != nil {
		t.cancel()
	}

	// The batcher does a final flush and closes the temporary store on shutdown.
	t.wg.Wait()

	return nil
}

// batchingAppender accumulates the points of a single remote-write request,
// keyed by labels hash, and on Commit hands them to the batcher (which buffers
// them in the temporary store and eventually flushes them to discardWriter). It
// mirrors clickhouse/tsdb's appender: the labels hash is used directly as the
// metric ID; there is no index lookup.
type batchingAppender struct {
	t   *batchingTSDB
	ttl int64
	ctx context.Context //nolint:containedctx

	pendingTimeSeries map[uint64]types.Timeseries
}

func (app *batchingAppender) Append(
	_ storage.SeriesRef,
	l labels.Labels,
	t int64,
	v float64,
) (storage.SeriesRef, error) {
	labelsHash := l.Hash()

	ts, ok := app.pendingTimeSeries[labelsHash]
	if !ok {
		app.pendingTimeSeries[labelsHash] = types.Timeseries{
			Labels: l,
			Points: []types.MetricPoint{{Timestamp: t, Value: v}},
		}
	} else {
		ts.Points = append(ts.Points, types.MetricPoint{Timestamp: t, Value: v})
		app.pendingTimeSeries[labelsHash] = ts
	}

	return 0, nil
}

func (app *batchingAppender) Commit() error {
	defer func() {
		app.pendingTimeSeries = make(map[uint64]types.Timeseries)
	}()

	if len(app.pendingTimeSeries) == 0 {
		return nil
	}

	pending := slices.Collect(maps.Values(app.pendingTimeSeries))
	metrics := make([]types.MetricData, len(pending))

	for i, ts := range pending {
		id := types.MetricID(ts.Labels.Hash()) //nolint:gosec // hash used as an opaque batching key

		metrics[i] = types.MetricData{
			ID:         id,
			Points:     ts.Points,
			TimeToLive: app.ttl,
		}
	}

	if err := app.t.batcher.Write(app.ctx, discardWriter{}, metrics); err != nil {
		return fmt.Errorf("write metrics: %w", err)
	}

	return nil
}

func (app *batchingAppender) Rollback() error {
	app.pendingTimeSeries = make(map[uint64]types.Timeseries)

	return nil
}

func (app *batchingAppender) SetOptions(*storage.AppendOptions) {}

func (app *batchingAppender) AppendExemplar(
	storage.SeriesRef, labels.Labels, exemplar.Exemplar,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) AppendHistogram(
	storage.SeriesRef, labels.Labels, int64, *histogram.Histogram, *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) AppendHistogramCTZeroSample(
	storage.SeriesRef, labels.Labels, int64, int64, *histogram.Histogram, *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) UpdateMetadata(
	storage.SeriesRef, labels.Labels, metadata.Metadata,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) AppendCTZeroSample(
	storage.SeriesRef, labels.Labels, int64, int64,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

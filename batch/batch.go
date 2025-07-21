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

/*
Package batch will group points in batch to send multiple consecutive points of the same
metrics to TSDB.

It rely on a temporary store that has fast random-access, either an in-memory store or Redis.

In the temporary store, all new points will be appened on a per-metric key.
For each metric, one SquirrelDB instance will be the owner and will flush points once a deadline
is reached.

The SquirrelDB instance that is owner, is the instance that appened point on an empty metric key.

The ownership could be:
* dropped by current owner, when the metric key become empty
* transferred when the current owner is shutting down
* taken-over by any SquirrelDB if metric flush deadline is overdue (which means that current owner is dead)

A SquirrelDB may also flush a metric if the number of points exceed a threshold (which could happen
during backlog processing). This could be done by a non-owner, since we can't guaranteed that the owner will
receive points for that metric.
*/
package batch

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/tsdb"
	"github.com/bleemeo/squirreldb/retry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

const (
	backgroundTaskInterval = 15 * time.Second

	// Each run of the background task, take the ownership of N metrics and sleep few milliseconds.
	// This avoid that one SquirrelDB take ownership of all metrics from another instance.
	transferredOwnershipLimit      = 1000
	transferredOwnershipSleepDelay = 50 * time.Millisecond

	takeoverInterval = 120 * time.Second
	// Each run of the takeover task takeover N metrics that are overdue and sleep few milliseconds.
	// This avoid that one SquirrelDB takeover of all overdue metrics.
	takeoverLimit      = 1000
	takeoverSleepDelay = 500 * time.Millisecond

	overdueThreshold = 5 * time.Minute

	// over-read from memory store of this amount. This is done to skip trying read from persitent
	// store if point timestamp and request timestamp aren't aligned.
	memoryOverreadMs = 60000
)

const flushMetricLimit = 1000 // Maximum number of metrics to send in one time

// TemporaryStore is an interface to the temporary points associated to metrics.
type TemporaryStore interface {
	// Append add points to the in-memory store and return the number of points in the store (after append)
	// for each metrics.
	Append(ctx context.Context, points []types.MetricData) ([]int, error)

	// GetSetPointsAndOffset do an atomic read-and-set on the metric points (atomic per-metric).
	// It also set the offset for each metrics and add metric ids to known metric.
	GetSetPointsAndOffset(ctx context.Context, points []types.MetricData, offsets []int) ([]types.MetricData, error)

	// ReadPointsAndOffset simply return points of write offset for metrics
	ReadPointsAndOffset(ctx context.Context, ids []types.MetricID) ([]types.MetricData, []int, error)

	// MarkToExpire will mark points, write offset and flushdeadline to expire
	// This is used to delete those entry. Since it should only be deleted if empty but
	// redis doesn't support this, it use an expiration longer than the flush deadline to ensure
	// only empty & no longer user metrics are deleted
	// It also forget the metric from known metrics.
	// Expiration is removed by GetSetPointsAndOffset and GetSetFlushDeadline
	MarkToExpire(ctx context.Context, ids []types.MetricID, ttl time.Duration) error

	// GetSetFlushDeadline do an atomic read-and-set on the metric flush deadline (atomic per-metric).
	GetSetFlushDeadline(ctx context.Context, deadlines map[types.MetricID]time.Time) (map[types.MetricID]time.Time, error)

	// AddToTransfert add the metrics to a list of metrics to transfert ownership from one SquirrelDB to another
	AddToTransfert(ctx context.Context, ids []types.MetricID) error

	// GetTransfert return & remove count metric from the list of metrics to transfert
	GetTransfert(ctx context.Context, count int) (map[types.MetricID]time.Time, error)

	// GetAllKnownMetrics return all known metrics with their deadline
	GetAllKnownMetrics(ctx context.Context) (map[types.MetricID]time.Time, error)
}

// stateData contains information about points stored in memory store for one metrics.
type stateData struct {
	flushDeadline time.Time
}

// Batch receive a stream of points and send batch for points to the writer. It use a memory store to keep points before
// flushing them to the writer.
// It also allow to read points merging value from the persistent store (reader) and the memory store.
type Batch struct {
	batchSize time.Duration

	states map[types.MetricID]stateData
	mutex  sync.Mutex

	memoryStore TemporaryStore
	reader      types.MetricReader
	writer      types.MetricWriter
	metrics     *metrics
	logger      zerolog.Logger
}

// New creates a new Batch object.
func New(
	reg prometheus.Registerer,
	batchSize time.Duration,
	memoryStore TemporaryStore,
	reader types.MetricReader,
	writer types.MetricWriter,
	logger zerolog.Logger,
) *Batch {
	batch := &Batch{
		batchSize:   batchSize,
		states:      make(map[types.MetricID]stateData),
		memoryStore: memoryStore,
		reader:      reader,
		writer:      writer,
		metrics:     newMetrics(reg),
		logger:      logger,
	}

	return batch
}

type readIter struct {
	b       *Batch
	ctx     context.Context //nolint:containedctx
	err     error
	current types.MetricData
	request types.MetricRequest
	offset  int
}

// ReadIter returns the deduplicated and sorted points read from the temporary and persistent storage
// according to the request.
func (b *Batch) ReadIter(ctx context.Context, request types.MetricRequest) (types.MetricDataSet, error) {
	return &readIter{
		b:       b,
		ctx:     ctx,
		request: request,
	}, nil
}

// Write implements MetricWriter.
func (b *Batch) Write(ctx context.Context, metrics []types.MetricData) error {
	return b.write(ctx, metrics, time.Now())
}

// Run starts Batch service (e.g. flushing points after a deadline).
func (b *Batch) Run(ctx context.Context) {
	tickerBackground := time.NewTicker(backgroundTaskInterval)
	tickerTakeover := time.NewTicker(takeoverInterval)

	defer tickerBackground.Stop()
	defer tickerTakeover.Stop()

	for {
		select {
		case <-tickerBackground.C:
			err := b.check(ctx, time.Now(), false, false)
			if err != nil {
				b.logger.Err(err).Msg("Batch service background check failed")
			}
		case <-tickerTakeover.C:
			b.checkTakeover(ctx, time.Now())
		case <-ctx.Done():
			err := b.check(context.Background(), time.Now(), true, true) //nolint: contextcheck
			if err != nil {
				b.logger.Err(err).Msg("Unable to shutdown batch service")
			}

			b.logger.Trace().Msg("Batch service stopped")

			return
		}
	}
}

// Flush force writing all in-memory (or in Redis) metrics from this SquirrelDB instance to TSDB.
func (b *Batch) Flush() error {
	return b.check(context.Background(), time.Now(), true, false)
}

// Checks the current states and flush those whose flush date has expired.
//
// If force is true, each state is flushed.
func (b *Batch) check(ctx context.Context, now time.Time, force bool, shutdown bool) error {
	start := time.Now()

	defer func() {
		b.metrics.BackgroundSeconds.Observe(time.Since(start).Seconds())
	}()

	for !shutdown {
		metrics, err := b.memoryStore.GetTransfert(ctx, transferredOwnershipLimit)
		if err != nil {
			b.logger.Err(err).Msg("Unable to query memory store for metrics transfer")

			metrics = nil
		}

		if len(metrics) == 0 {
			break
		}

		b.mutex.Lock()

		for id, deadline := range metrics {
			b.states[id] = stateData{
				flushDeadline: deadline,
			}
		}

		b.mutex.Unlock()

		b.metrics.TransferOwner.Add(float64(len(metrics)))

		if len(metrics) < transferredOwnershipLimit {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(randomDuration(transferredOwnershipSleepDelay)):
		}
	}

	ids := make([]types.MetricID, 0)

	b.mutex.Lock()

	for id, data := range b.states {
		if now.After(data.flushDeadline) || force || shutdown {
			ids = append(ids, id)
		}
	}

	b.mutex.Unlock()

	for startIndex := 0; startIndex < len(ids); startIndex += flushMetricLimit {
		endIndex := startIndex + flushMetricLimit
		if endIndex > len(ids) {
			endIndex = len(ids)
		}

		err := b.flush(ctx, ids[startIndex:endIndex], now, shutdown)
		if err != nil {
			return err
		}
	}

	return nil
}

// randomDuration return a delay with a +/- 20% jitter.
func randomDuration(target time.Duration) time.Duration {
	jitter := target / 5
	jitterFactor := rand.Float64()*2 - 1 //nolint:gosec

	return target + time.Duration(jitterFactor*float64(jitter))
}

func (b *Batch) checkTakeover(ctx context.Context, now time.Time) {
	for {
		metrics, err := b.memoryStore.GetAllKnownMetrics(ctx)
		if err != nil {
			b.logger.Err(err).Msg("Unable to query memory store for metrics overdue")

			return
		}

		overdue := make(map[types.MetricID]time.Time)

		for id, deadline := range metrics {
			if deadline.Add(overdueThreshold).Before(now) {
				overdue[id] = deadline
			}

			if len(overdue) > takeoverLimit {
				break
			}
		}

		if len(overdue) == 0 {
			break
		}

		err = b.takeoverMetrics(ctx, overdue, now)
		if err != nil {
			b.logger.Err(err).Msg("Unable to takeover metrics")

			return
		}

		if len(overdue) < takeoverLimit {
			break
		}

		// Add random sleep to reduce change of collision if two SquirrelDB
		// run checkTakeover
		select {
		case <-ctx.Done():
			return
		case <-time.After(randomDuration(takeoverSleepDelay)):
		}
	}
}

func (b *Batch) takeoverMetrics(ctx context.Context, metrics map[types.MetricID]time.Time, now time.Time) error {
	ids := make([]types.MetricID, len(metrics))
	i := 0

	b.mutex.Lock()

	for id, deadline := range metrics {
		b.states[id] = stateData{
			flushDeadline: deadline,
		}
		ids[i] = id
		i++
	}

	b.mutex.Unlock()

	err := b.flush(ctx, ids, now, false)

	b.metrics.Takeover.WithLabelValues("in").Add(float64(len(metrics)))

	return err
}

// setPoints update the list of points and offsets for given metrics
//
// It also ensure that any new points that arrived between the initial read of previousMetrics and
// the time the set of newMetrics is done are re-added.
//
// # It return a boolean telling if there is points for each metrics in the memory store
//
// This function may recursivelly call itself, deep count the number of recursing and avoid infinite recussion.
func (b *Batch) setPointsAndOffset(
	ctx context.Context,
	previousMetrics []types.MetricData,
	setMetrics []types.MetricData,
	offsets []int,
	deep int,
) ([]bool, error) {
	var currentMetrics []types.MetricData

	err := retry.Print(func() error {
		var err error

		currentMetrics, err = b.memoryStore.GetSetPointsAndOffset(ctx, setMetrics, offsets)

		return err //nolint:wrapcheck
	},
		retry.NewExponentialBackOff(ctx, 30*time.Second),
		b.logger,
		"write points in temporary store",
	)
	if err != nil {
		return nil, fmt.Errorf("read from store failed: %w", err)
	}

	results := make([]bool, len(setMetrics))

	var (
		countNewPoint     int
		countNeedReSet    int
		appendPoints      []types.MetricData
		rePreviousMetrics []types.MetricData
		reSetMetrics      []types.MetricData
		reOffsets         []int
		reToCurrentIndex  []int
	)

	for i, previousMetric := range previousMetrics {
		var newPoints []types.MetricPoint

		currentMetric := currentMetrics[i]
		setMetric := setMetrics[i]
		needOffsetFix := false
		idxPrevious := 0

		for idxCurrent, p := range currentMetric.Points {
			// The difference between previous and current could only be due to:
			// * append of points at the end of the list
			// * someone doing a setPointsAndOffset after filtering previous but keping the order
			// So if a point didn't match between current and previous is either:
			// * we reached the end of previous and points were appened
			// * OR the points was filtered out from previous and we need to go to next point from previous
			for idxPrevious < len(previousMetric.Points) && previousMetric.Points[idxPrevious].Timestamp != p.Timestamp {
				idxPrevious++

				needOffsetFix = true
			}

			if idxPrevious == len(previousMetric.Points) {
				newPoints = currentMetric.Points[idxCurrent:]

				break
			}
		}

		if len(newPoints) > 0 { //nolint:nestif
			countNewPoint += len(newPoints)

			if needOffsetFix && deep < 3 {
				rePreviousMetrics = append(rePreviousMetrics, types.MetricData{
					ID:         setMetric.ID,
					TimeToLive: setMetric.TimeToLive,
					Points:     setMetric.Points,
				})

				reSetMetrics = append(reSetMetrics, types.MetricData{
					ID:         setMetric.ID,
					TimeToLive: setMetric.TimeToLive,
					Points:     append(setMetric.Points, newPoints...),
				})

				reToCurrentIndex = append(reToCurrentIndex, i)

				if deep == 2 {
					// Too much retry... as last chance we use offset 0. This may
					// result in little too much write in TSDB but should avoid losing point.
					reOffsets = append(reOffsets, 0)
				} else {
					reOffsets = append(reOffsets, offsets[i])
				}
			} else {
				appendPoints = append(appendPoints, types.MetricData{
					ID:         setMetric.ID,
					TimeToLive: setMetric.TimeToLive,
					Points:     newPoints,
				})
			}
		}

		if len(setMetric.Points) > 0 || len(newPoints) > 0 {
			results[i] = true
		}
	}

	if len(appendPoints) > 0 {
		err := retry.Print(func() error {
			_, err := b.memoryStore.Append(ctx, appendPoints)

			return err //nolint:wrapcheck
		},
			retry.NewExponentialBackOff(ctx, 30*time.Second),
			b.logger,
			"append points in temporary store",
		)
		if err != nil {
			return nil, fmt.Errorf("append points: %w", err)
		}
	}

	if len(reSetMetrics) > 0 {
		countNeedReSet += len(reSetMetrics)

		tmp, err := b.setPointsAndOffset(ctx, rePreviousMetrics, reSetMetrics, reOffsets, deep+1)
		if err != nil {
			return nil, err
		}

		for i, v := range tmp {
			results[reToCurrentIndex[i]] = v
		}
	}

	b.metrics.ConflictFlushTotal.Add(float64(countNeedReSet))
	b.metrics.NewPointsDuringFlush.Add(float64(countNewPoint))

	return results, nil
}

// Flushes the points corresponding to the specified metrics state list
// It does the following:
//   - Read points & write offset from memoryStore (excepted for new metric for which we just become owner)
//   - Send all points after write offset to TSDB (excepted for new metrics, we write nothing)
//   - Filter to keep only point more recent than batchSize (excepted for new metric, here we kept all points
//     that come from states)
//   - Get + Set to memoryStore the points filtered
//   - Update states (in-memory and in temporaryStore).
func (b *Batch) flush(
	ctx context.Context,
	ids []types.MetricID,
	now time.Time,
	shutdown bool,
) error {
	states := make([]stateData, len(ids))

	b.mutex.Lock()

	for i, id := range ids {
		states[i] = b.states[id]
	}

	b.mutex.Unlock()

	var (
		readPointsCount, setPointsCount int
		metrics                         []types.MetricData
		offsets                         []int
	)

	err := retry.Print(func() error {
		var err error

		metrics, offsets, err = b.memoryStore.ReadPointsAndOffset(ctx, ids)

		return err //nolint:wrapcheck
	}, retry.NewExponentialBackOff(ctx, 30*time.Second),
		b.logger,
		"get points from the memory store",
	)
	if err != nil {
		return fmt.Errorf("read memory store: %w", err)
	}

	metricsToWrite := make([]types.MetricData, 0, len(metrics))

	for i, storeData := range metrics {
		readPointsCount += len(storeData.Points)
		offset := offsets[i]

		if offset > len(storeData.Points) {
			msg := "Batch.flush(): unexpected offset == %d is too big for metric ID %d (only %d points)"
			b.logger.Warn().Msgf(msg, offset, storeData.ID, len(storeData.Points))
			offset = len(storeData.Points)
		}

		storeData.Points = storeData.Points[offset:]
		if len(storeData.Points) > 0 {
			metricsToWrite = append(metricsToWrite, storeData)
		}
	}

	b.metrics.DuplicatedPoints.Add(float64(sortMetrics(metricsToWrite)))

	err = retry.Print(func() error {
		return b.writer.Write(ctx, metricsToWrite)
	},
		retry.NewExponentialBackOff(ctx, 30*time.Second),
		b.logger,
		"write points in persistent store",
	)
	if err != nil {
		return fmt.Errorf("write to persistent store: %w", err)
	}

	keptMetrics := make([]types.MetricData, len(metrics))
	offsets = make([]int, len(metrics))

	for i, m := range metrics {
		keptMetrics[i] = types.MetricData{
			ID:         m.ID,
			TimeToLive: m.TimeToLive,
			Points:     make([]types.MetricPoint, 0, len(m.Points)/2),
		}

		cutoffTime := now.Add(-b.batchSize)
		cutoffTimestamp := cutoffTime.UnixNano() / 1000000

		for _, p := range m.Points {
			if p.Timestamp >= cutoffTimestamp {
				keptMetrics[i].Points = append(keptMetrics[i].Points, p)
			}
		}

		offsets[i] = len(keptMetrics[i].Points)
		setPointsCount += len(keptMetrics[i].Points)
	}

	b.metrics.FlushPoints.WithLabelValues("read").Add(float64(readPointsCount))
	b.metrics.FlushPoints.WithLabelValues("set").Add(float64(setPointsCount))

	results, err := b.setPointsAndOffset(ctx, metrics, keptMetrics, offsets, 0)
	if err != nil {
		return err
	}

	var (
		idToExpire         []types.MetricID
		transfertOwnership []types.MetricID
	)

	newDeadlines := make(map[types.MetricID]time.Time)

	b.mutex.Lock()

	for i, hasPoint := range results {
		id := ids[i]

		if !hasPoint {
			idToExpire = append(idToExpire, id)
			delete(b.states, id)
		} else if _, isOwner := b.states[ids[i]]; isOwner {
			newDeadlines[id] = flushTimestamp(id, now, b.batchSize)

			if shutdown {
				transfertOwnership = append(transfertOwnership, id)
			}
		}
	}

	b.mutex.Unlock()

	err = retry.Print(func() error {
		return b.memoryStore.AddToTransfert(ctx, transfertOwnership)
	},
		retry.NewExponentialBackOff(ctx, 30*time.Second),
		b.logger,
		"transfert ownership using memory store",
	)
	if err != nil {
		return fmt.Errorf("transfert ownership: %w", err)
	}

	err = retry.Print(func() error {
		// The TTL should be long enough to allow another SquirrelDB to detect
		// that this metrics has no ownership.
		// maximum deadline is now + b.batchSize
		// Give two takeoverInterval as safety margin.
		ttl := 2*takeoverInterval + overdueThreshold + b.batchSize

		return b.memoryStore.MarkToExpire(ctx, idToExpire, ttl)
	},
		retry.NewExponentialBackOff(ctx, 30*time.Second),
		b.logger,
		"mark metrics to expire in memory store",
	)
	if err != nil {
		return fmt.Errorf("mark metrics to expire: %w", err)
	}

	var storeDeadlines map[types.MetricID]time.Time

	err = retry.Print(func() error {
		var err error

		storeDeadlines, err = b.memoryStore.GetSetFlushDeadline(ctx, newDeadlines)

		return err //nolint:wrapcheck
	},
		retry.NewExponentialBackOff(ctx, 30*time.Second),
		b.logger,
		"set deadline in memory store",
	)
	if err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	b.mutex.Lock()

	for id, newDeadline := range newDeadlines {
		previousDeadline := b.states[id].flushDeadline
		b.states[id] = stateData{
			flushDeadline: newDeadline,
		}

		if !storeDeadlines[id].Equal(previousDeadline) {
			b.metrics.Takeover.WithLabelValues("out").Inc()
			delete(b.states, id)
		}
	}

	b.mutex.Unlock()

	return nil
}

// sortMetrics sorts and deduplicate points only if needed.
// It do a copy if sort is needed.
// Return the number of point that were duplicated.
func sortMetrics(input []types.MetricData) int {
	var count int

	for i, data := range input {
		var (
			needSort bool
			maxTS    int64
		)

		for j, point := range data.Points {
			if j == 0 || point.Timestamp > maxTS {
				maxTS = point.Timestamp
			} else {
				needSort = true

				break
			}
		}

		if needSort {
			pointsCopy := make([]types.MetricPoint, len(data.Points))

			copy(pointsCopy, data.Points)
			pointsCopy = types.DeduplicatePoints(pointsCopy)

			count += len(data.Points) - len(pointsCopy)
			input[i].Points = pointsCopy
		}
	}

	return count
}

func (i *readIter) Err() error {
	if i.err != nil {
		return i.err
	}

	return i.ctx.Err()
}

func (i *readIter) At() types.MetricData {
	return i.current
}

func (i *readIter) Next() bool {
	start := time.Now()

	defer func() {
		i.b.metrics.RequestsSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	for {
		if i.offset >= len(i.request.IDs) || i.ctx.Err() != nil {
			return false
		}

		if i.err != nil {
			return false
		}

		id := i.request.IDs[i.offset]
		i.offset++

		hasNext := i.tryNext(id)
		if hasNext {
			if i.request.EnableDebug {
				i.b.logger.Info().Msgf(
					"Read ID=%d: %d points",
					id,
					len(i.current.Points),
				)
			}

			if i.request.EnableVerboseDebug {
				i.b.logger.Info().Msgf("Read ID=%d, points=%v", id, i.current.Points)
			}

			return true
		}
	}
}

func (i *readIter) tryNext(id types.MetricID) bool {
	idRequest := types.MetricRequest{
		IDs:                []types.MetricID{id},
		FromTimestamp:      i.request.FromTimestamp,
		ToTimestamp:        i.request.ToTimestamp,
		StepMs:             i.request.StepMs,
		Function:           i.request.Function,
		ForcePreAggregated: i.request.ForcePreAggregated,
		ForceRaw:           i.request.ForceRaw,
		EnableDebug:        i.request.EnableDebug,
		EnableVerboseDebug: i.request.EnableVerboseDebug,
	}

	temporaryMetrics, err := i.b.readTemporary(
		i.ctx, []types.MetricID{id}, i.request.FromTimestamp-memoryOverreadMs, i.request.ToTimestamp,
	)
	if err != nil {
		i.err = err

		return false
	}

	data := types.MetricData{
		ID: id,
	}

	if len(temporaryMetrics) > 0 && len(temporaryMetrics[0].Points) > 0 {
		temporaryData := temporaryMetrics[0]
		idRequest.ToTimestamp = temporaryData.Points[0].Timestamp - 1

		// because we over-read, we need to filter first point. They are sorted which make it simple
		data.Points = temporaryData.Points
		for len(data.Points) > 0 && data.Points[0].Timestamp < idRequest.FromTimestamp {
			data.Points = data.Points[1:]
		}

		if tsdb.RequestUseAggregatedData(i.request) {
			data = tsdb.AggregateWithFuncType(data, i.request.Function)
		}
	}

	if idRequest.FromTimestamp <= idRequest.ToTimestamp {
		persistentMetrics, err := i.b.reader.ReadIter(i.ctx, idRequest)
		if err != nil {
			i.err = err

			return false
		}

		for persistentMetrics.Next() {
			persistentData := persistentMetrics.At()
			if persistentData.ID == id {
				data.Points = append(persistentData.Points, data.Points...)

				break
			}
		}

		err = persistentMetrics.Err()
		if err != nil {
			i.err = err

			return false
		}
	}

	if len(data.Points) > 0 {
		i.current = data
		i.b.metrics.RequestsPoints.WithLabelValues("read").Add(float64(len(data.Points)))

		return true
	}

	return false
}

// Returns the deduplicated and sorted points read from the temporary storage according to the request.
func (b *Batch) readTemporary(
	ctx context.Context,
	ids []types.MetricID,
	fromTimestamp, toTimestamp int64,
) ([]types.MetricData, error) {
	metrics, _, err := b.memoryStore.ReadPointsAndOffset(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("store read fail: %w", err)
	}

	temporaryMetrics := make([]types.MetricData, 0, len(ids))

	for _, data := range metrics {
		var (
			maxTS    int64
			needSort bool
		)

		temporaryData := types.MetricData{
			ID:         data.ID,
			TimeToLive: data.TimeToLive,
		}

		for i, point := range data.Points {
			if (point.Timestamp >= fromTimestamp) && (point.Timestamp <= toTimestamp) {
				if i == 0 || point.Timestamp > maxTS {
					temporaryData.Points = append(temporaryData.Points, point)
					maxTS = point.Timestamp
				} else {
					temporaryData.Points = append(temporaryData.Points, point)
					needSort = true
				}
			}
		}

		if needSort {
			temporaryData.Points = types.DeduplicatePoints(temporaryData.Points)
		}

		if len(temporaryData.Points) > 0 {
			temporaryMetrics = append(temporaryMetrics, temporaryData)
		}
	}

	return temporaryMetrics, nil
}

// Writes metrics in the temporary storage
// Each metric has a state, which will allow you to know if the size of a batch, or the flush date, is reached.
// If this is the case, the state is added to the list of states to flush.
func (b *Batch) write(
	ctx context.Context,
	metrics []types.MetricData,
	now time.Time,
) error {
	start := time.Now()

	defer func() {
		b.metrics.RequestsSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	if len(metrics) == 0 {
		return nil
	}

	var (
		writtenPointsCount     int
		ownerShipInitialPoints []types.MetricData
	)

	metricToFlush := make(map[types.MetricID]any)

	pointsCount, err := b.memoryStore.Append(ctx, metrics)
	if err != nil {
		return fmt.Errorf("fail to append points: %w", err)
	}

	b.mutex.Lock()

	for i, count := range pointsCount {
		addedCount := len(metrics[i].Points)
		previousCount := count - addedCount
		writtenPointsCount += addedCount

		if previousCount < 0 {
			b.mutex.Unlock()

			return fmt.Errorf("unexpected \"previousCount\" == %d in batch.Write", previousCount)
		}

		if previousCount == 0 {
			b.states[metrics[i].ID] = stateData{
				flushDeadline: flushTimestamp(metrics[i].ID, now, b.batchSize),
			}

			ownerShipInitialPoints = append(ownerShipInitialPoints, metrics[i])
		}

		// Yes we compare number of point with time. This test only here
		// to avoid unbounded grow of temporary store when processing backlog
		// of data.
		if count > int(b.batchSize.Seconds()) {
			_, isOwner := b.states[metrics[i].ID]
			n := count / int(b.batchSize.Seconds())

			if isOwner {
				metricToFlush[metrics[i].ID] = nil
			} else if n > 1 && previousCount < n*int(b.batchSize.Seconds()) {
				// If the number of points crossed a multiple of our b.batchSize
				// (that is count > b.batchSize and previousCount < N*b.batchSize)
				// and it's not the first threshold, then always flush.
				//
				// This will catch case where the current owner is dead, but give him
				// the first threshold to act.
				metricToFlush[metrics[i].ID] = nil

				b.metrics.NonOwnerWrite.Inc()
			}
		}
	}

	b.mutex.Unlock()

	if len(ownerShipInitialPoints) > 0 {
		offsets := make([]int, len(ownerShipInitialPoints))

		_, err := b.setPointsAndOffset(ctx, ownerShipInitialPoints, ownerShipInitialPoints, offsets, 0)
		if err != nil {
			return err
		}

		deadlines := make(map[types.MetricID]time.Time, len(ownerShipInitialPoints))

		b.mutex.Lock()

		for _, m := range ownerShipInitialPoints {
			deadlines[m.ID] = b.states[m.ID].flushDeadline
		}

		b.mutex.Unlock()

		_, err = b.memoryStore.GetSetFlushDeadline(ctx, deadlines)
		if err != nil {
			return fmt.Errorf("fail to update flush deadline: %w", err)
		}
	}

	b.metrics.RequestsPoints.WithLabelValues("write").Add(float64(writtenPointsCount))

	if len(metricToFlush) != 0 {
		tmp := make([]types.MetricID, len(metricToFlush))
		i := 0

		for id := range metricToFlush {
			tmp[i] = id
			i++
		}

		for startIndex := 0; startIndex < len(tmp); startIndex += flushMetricLimit {
			endIndex := startIndex + flushMetricLimit
			if endIndex > len(tmp) {
				endIndex = len(tmp)
			}

			err = b.flush(ctx, tmp[startIndex:endIndex], now, false)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Returns a flush date
// It follows the formula:
// timestamp = (now + batchSize) - (now + batchSize + (id.int % batchSize)) % batchSize.
func flushTimestamp(id types.MetricID, now time.Time, batchSize time.Duration) time.Time {
	timestamp := now.Unix() + int64(batchSize.Seconds())
	offset := int64(id) % int64(batchSize.Seconds())

	timestamp -= (timestamp + offset) % int64(batchSize.Seconds())

	return time.Unix(timestamp, 0)
}

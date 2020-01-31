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
	"log"
	"math/rand"
	"os"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"

	gouuid "github.com/gofrs/uuid"
)

const (
	backgroundTaskInterval = 15 * time.Second

	// Each run of the background task, take the ownership of N metrics and sleep few milliseconds.
	// This avoid that one SquirrelDB take ownership of all metrics from another instance
	transferredOwnershipLimit      = 1000
	transferredOwnershipSleepDelay = 50 * time.Millisecond

	takeoverInterval = 120 * time.Second
	// Each run of the takeover task takeover N metrics that are overdue and sleep few milliseconds.
	// This avoid that one SquirrelDB takeover of all overdue metrics
	takeoverLimit      = 1000
	takeoverSleepDelay = 500 * time.Millisecond

	overdueThreshold = 5 * time.Minute
)

const flushMetricLimit = 1000 // Maximum number of metrics to send in one time

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[batch] ", log.LstdFlags)

// TemporaryStore is an interface to the temporary points associated to metrics.
type TemporaryStore interface {
	// Append add points to the in-memory store and return the number of points in the store (after append) for each metrics
	Append(points []types.MetricData) ([]int, error)

	// GetSetPointsAndOffset do an atomic read-and-set on the metric points (atomic per-metric).
	// It also set the offset for each metrics and add metric uuids to known metric.
	GetSetPointsAndOffset(points []types.MetricData, offsets []int) ([]types.MetricData, error)

	// ReadPointsAndOffset simply return points of write offset for metrics
	ReadPointsAndOffset(uuids []gouuid.UUID) ([]types.MetricData, []int, error)

	// MarkToExpire will mark points, write offset and flushdeadline to expire
	// This is used to delete those entry. Since it should only be deleted if empty but
	// redis doesn't support this, it use an expiration longer than the flush deadline to ensure
	// only empty & no longer user metrics are deleted
	// It also forget the metric from known metrics.
	// Expiration is removed by GetSetPointsAndOffset and GetSetFlushDeadline
	MarkToExpire(uuids []gouuid.UUID, ttl time.Duration) error

	// GetSetFlushDeadline do an atomic read-and-set on the metric flush deadline (atomic per-metric).
	GetSetFlushDeadline(deadlines map[gouuid.UUID]time.Time) (map[gouuid.UUID]time.Time, error)

	// AddToTransfert add the metrics to a list of metrics to transfert ownership from one SquirrelDB to another
	AddToTransfert(uuids []gouuid.UUID) error

	// GetTransfert return & remove count metric from the list of metrics to transfert
	GetTransfert(count int) (map[gouuid.UUID]time.Time, error)

	// GetAllKnownMetrics return all known metrics with their deadline
	GetAllKnownMetrics() (map[gouuid.UUID]time.Time, error)
}

// stateData contains information about points stored in memory store for one metrics
type stateData struct {
	flushDeadline time.Time
}

// Batch receive a stream of points and send batch for points to the writer. It use a memory store to keep points before
// flushing them to the writer.
// It also allow to read points merging value from the persistent store (reader) and the memory store.
type Batch struct {
	batchSize int64

	states map[gouuid.UUID]stateData
	mutex  sync.Mutex

	memoryStore TemporaryStore
	reader      types.MetricReader
	writer      types.MetricWriter
}

// New creates a new Batch object
func New(batchSize int64, memoryStore TemporaryStore, reader types.MetricReader, writer types.MetricWriter) *Batch {
	batch := &Batch{
		batchSize:   batchSize,
		states:      make(map[gouuid.UUID]stateData),
		memoryStore: memoryStore,
		reader:      reader,
		writer:      writer,
	}

	return batch
}

// Read implements MetricReader
func (b *Batch) Read(request types.MetricRequest) (map[gouuid.UUID]types.MetricData, error) {
	return b.read(request)
}

// Write implements MetricWriter
func (b *Batch) Write(metrics []types.MetricData) error {
	return b.write(metrics, time.Now())
}

// Run starts Batch service (e.g. flushing points after a deadline)
func (b *Batch) Run(ctx context.Context) {
	tickerBackground := time.NewTicker(backgroundTaskInterval)
	tickerTakeover := time.NewTicker(takeoverInterval)

	defer tickerBackground.Stop()
	defer tickerTakeover.Stop()

	for ctx.Err() == nil {
		select {
		case <-tickerBackground.C:
			b.check(ctx, time.Now(), false, false)
		case <-tickerTakeover.C:
			b.checkTakeover(ctx, time.Now())
		case <-ctx.Done():
			b.check(ctx, time.Now(), true, true)
			debug.Print(2, logger, "Batch service stopped")

			return
		}
	}
}

// Flush force writing all in-memory (or in Redis) metrics from this SquirrelDB instance to TSDB
func (b *Batch) Flush() error {
	b.check(context.Background(), time.Now(), true, false)

	return nil
}

// Checks the current states and flush those whose flush date has expired
// If force is true, each state is flushed
func (b *Batch) check(ctx context.Context, now time.Time, force bool, shutdown bool) {
	start := time.Now()

	for {
		metrics, err := b.memoryStore.GetTransfert(transferredOwnershipLimit)
		if err != nil {
			logger.Printf("Unable to query memory store for metrics transfer: %v", err)

			metrics = nil
		}

		if len(metrics) == 0 {
			break
		}

		b.mutex.Lock()
		for uuid, deadline := range metrics {
			b.states[uuid] = stateData{
				flushDeadline: deadline,
			}
		}
		b.mutex.Unlock()

		transferOwnerTotal.Add(float64(len(metrics)))

		if len(metrics) < transferredOwnershipLimit {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(randomDuration(transferredOwnershipSleepDelay)):
		}
	}

	uuids := make([]gouuid.UUID, 0)

	b.mutex.Lock()
	for uuid, data := range b.states {
		if now.After(data.flushDeadline) || force || shutdown {
			uuids = append(uuids, uuid)
		}
	}
	b.mutex.Unlock()

	for startIndex := 0; startIndex < len(uuids); startIndex += flushMetricLimit {
		endIndex := startIndex + flushMetricLimit
		if endIndex > len(uuids) {
			endIndex = len(uuids)
		}

		b.flush(uuids[startIndex:endIndex], now, shutdown)
	}

	backgroundSeconds.Observe(time.Since(start).Seconds())
}

// randomDuration return a delay with a +/- 20% jitter
func randomDuration(target time.Duration) time.Duration {
	jitter := target / 5
	jitterFactor := rand.Float64()*2 - 1

	return target + time.Duration(jitterFactor*float64(jitter))
}

func (b *Batch) checkTakeover(ctx context.Context, now time.Time) {
	for {
		metrics, err := b.memoryStore.GetAllKnownMetrics()
		if err != nil {
			logger.Printf("Unable to query memory store for metrics overdue: %v", err)
			return
		}

		overdue := make(map[gouuid.UUID]time.Time)

		for uuid, deadline := range metrics {
			if deadline.Add(overdueThreshold).Before(now) {
				overdue[uuid] = deadline
			}

			if len(uuid) > takeoverLimit {
				break
			}
		}

		if len(overdue) == 0 {
			break
		}

		b.takeoverMetrics(overdue, now)

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

func (b *Batch) takeoverMetrics(metrics map[gouuid.UUID]time.Time, now time.Time) {
	uuids := make([]gouuid.UUID, len(metrics))
	i := 0

	b.mutex.Lock()

	for uuid, deadline := range metrics {
		b.states[uuid] = stateData{
			flushDeadline: deadline,
		}
		uuids[i] = uuid
	}
	b.mutex.Unlock()

	b.flush(uuids, now, false)
	takeoverInTotal.Add(float64(len(metrics)))
}

// setPoints update the list of points and offsets for given metrics
//
// It also ensure that any new points that arrived between the initial read of previousMetrics and
// the time the set of newMetrics is done are re-added.
//
// It return a boolean telling if there is points for each metrics in the memory store
//
// This function may recursivelly call itself, deep count the number of recursing and avoid infinite recussion
func (b *Batch) setPointsAndOffset(previousMetrics []types.MetricData, setMetrics []types.MetricData, offsets []int, deep int) []bool { // nolint: gocognit
	var currentMetrics []types.MetricData

	retry.Print(func() error {
		var err error

		currentMetrics, err = b.memoryStore.GetSetPointsAndOffset(setMetrics, offsets)

		return err
	},
		retry.NewExponentialBackOff(30*time.Second),
		logger,
		"write points in temporary store",
	)

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

		if len(newPoints) > 0 {
			countNewPoint += len(newPoints)

			if needOffsetFix && deep < 3 {
				rePreviousMetrics = append(rePreviousMetrics, types.MetricData{
					UUID:       setMetric.UUID,
					TimeToLive: setMetric.TimeToLive,
					Points:     setMetric.Points,
				})

				reSetMetrics = append(reSetMetrics, types.MetricData{
					UUID:       setMetric.UUID,
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
					UUID:       setMetric.UUID,
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
		retry.Print(func() error {
			_, err := b.memoryStore.Append(appendPoints)

			return err
		},
			retry.NewExponentialBackOff(30*time.Second),
			logger,
			"append points in temporary store",
		)
	}

	if len(reSetMetrics) > 0 {
		countNeedReSet += len(reSetMetrics)
		tmp := b.setPointsAndOffset(rePreviousMetrics, reSetMetrics, reOffsets, deep+1)

		for i, v := range tmp {
			results[reToCurrentIndex[i]] = v
		}
	}

	conflictFlushTotal.Add(float64(countNeedReSet))
	newPointsDuringFlushTotal.Add(float64(countNewPoint))

	return results
}

// Flushes the points corresponding to the specified metrics state list
// It does the following:
// * Read points & write offset from memoryStore (excepted for new metric for which we just become owner)
// * Send all points after write offset to TSDB (excepted for new metrics, we write nothing)
// * Filter to keep only point more recent than batchSize (excepted for new metric, here we kept all points that come from states)
// * Get + Set to memoryStore the points filtered
// * Update states (in-memory and in temporaryStore)
func (b *Batch) flush(uuids []gouuid.UUID, now time.Time, shutdown bool) {
	states := make([]stateData, len(uuids))

	b.mutex.Lock()

	for i, uuid := range uuids {
		states[i] = b.states[uuid]
	}

	b.mutex.Unlock()

	var (
		readPointsCount, setPointsCount int
	)

	var (
		metrics []types.MetricData
		offsets []int
	)

	retry.Print(func() error {
		var err error
		metrics, offsets, err = b.memoryStore.ReadPointsAndOffset(uuids) // nolint: scopelint

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"get points from the memory store",
	)

	metricsToWrite := make([]types.MetricData, 0, len(metrics))

	for i, data := range metrics {
		readPointsCount += len(data.Points)
		storeData := data
		offset := offsets[i]

		if offset > len(storeData.Points) {
			logger.Printf("Batch.flush(): unexpected offset == %d is too big for metric %s (only %d points)", offset, data.UUID.String(), len(data.Points))
			offset = len(storeData.Points)
		}

		storeData.Points = storeData.Points[offset:]
		if len(storeData.Points) > 0 {
			metricsToWrite = append(metricsToWrite, storeData)
		}
	}

	duplicatedPointsTotal.Add(float64(sortMetrics(metricsToWrite)))

	retry.Print(func() error {
		return b.writer.Write(metricsToWrite)
	},
		retry.NewExponentialBackOff(30*time.Second),
		logger,
		"write points in persistent store",
	)

	keptMetrics := make([]types.MetricData, len(metrics))
	offsets = make([]int, len(metrics))

	for i, m := range metrics {
		keptMetrics[i] = types.MetricData{
			UUID:       m.UUID,
			TimeToLive: m.TimeToLive,
			Points:     make([]types.MetricPoint, 0, len(m.Points)/2),
		}

		cutoffTimestamp := (now.Unix() - b.batchSize) * 1000

		for _, p := range m.Points {
			if p.Timestamp >= cutoffTimestamp {
				keptMetrics[i].Points = append(keptMetrics[i].Points, p)
			}
		}

		offsets[i] = len(keptMetrics[i].Points)
		setPointsCount += len(keptMetrics[i].Points)
	}

	flushPointsTotalRead.Add(float64(readPointsCount))
	flushPointsTotalSet.Add(float64(setPointsCount))

	results := b.setPointsAndOffset(metrics, keptMetrics, offsets, 0)

	var (
		uuidToExpire       []gouuid.UUID
		transfertOwnership []gouuid.UUID
	)

	newDeadlines := make(map[gouuid.UUID]time.Time)

	b.mutex.Lock()
	for i, hasPoint := range results {
		uuid := uuids[i]

		if !hasPoint {
			uuidToExpire = append(uuidToExpire, uuid)
			delete(b.states, uuid)
		} else if _, isOwner := b.states[uuids[i]]; isOwner {
			newDeadlines[uuid] = flushTimestamp(uuid, now, b.batchSize)
			if shutdown {
				transfertOwnership = append(transfertOwnership, uuid)
			}
		}
	}
	b.mutex.Unlock()

	retry.Print(func() error {
		return b.memoryStore.AddToTransfert(transfertOwnership)
	},
		retry.NewExponentialBackOff(30*time.Second),
		logger,
		"transfert ownership using memory store",
	)

	retry.Print(func() error {
		// The TTL should be long enough to allow another SquirrelDB to detect
		// that this metrics has no ownership.
		// maximum deadline is now + b.batchSize
		// Give two takeoverInterval as safely margin
		ttl := 2*takeoverInterval + overdueThreshold + time.Duration(b.batchSize)*time.Second
		return b.memoryStore.MarkToExpire(uuidToExpire, ttl)
	},
		retry.NewExponentialBackOff(30*time.Second),
		logger,
		"mark metrics to expire in memory store",
	)

	var storeDeadlines map[gouuid.UUID]time.Time

	retry.Print(func() error {
		var err error

		storeDeadlines, err = b.memoryStore.GetSetFlushDeadline(newDeadlines)

		return err
	},
		retry.NewExponentialBackOff(30*time.Second),
		logger,
		"set deadline in memory store",
	)

	b.mutex.Lock()

	for uuid, newDeadline := range newDeadlines {
		previousDeadline := b.states[uuid].flushDeadline
		b.states[uuid] = stateData{
			flushDeadline: newDeadline,
		}

		if !storeDeadlines[uuid].Equal(previousDeadline) {
			takeoverOutTotal.Inc()
			delete(b.states, uuid)
		}
	}
	b.mutex.Unlock()
}

// sortMetrics sorts and deduplicate points only if needed.
// It do a copy if sort is needed.
// Return the number of point that were duplicated
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

// Returns the deduplicated and sorted points read from the temporary and persistent storage according to the request
func (b *Batch) read(request types.MetricRequest) (map[gouuid.UUID]types.MetricData, error) {
	start := time.Now()

	defer func() {
		requestsSecondsRead.Observe(time.Since(start).Seconds())
	}()

	if len(request.UUIDs) == 0 {
		return nil, nil
	}

	metrics := make(map[gouuid.UUID]types.MetricData, len(request.UUIDs))

	var readPointsCount int

	for _, uuid := range request.UUIDs {
		uuidRequest := types.MetricRequest{
			UUIDs:         []gouuid.UUID{uuid},
			FromTimestamp: request.FromTimestamp,
			ToTimestamp:   request.ToTimestamp,
			StepMs:        request.StepMs,
			Function:      request.Function,
		}

		temporaryMetrics, err := b.readTemporary(uuidRequest)
		if err != nil {
			return nil, err
		}

		temporaryData := temporaryMetrics[uuid]

		if len(temporaryData.Points) > 0 {
			uuidRequest.ToTimestamp = temporaryData.Points[0].Timestamp - 1
		}

		data := types.MetricData{
			Points: temporaryData.Points,
		}

		if uuidRequest.ToTimestamp >= uuidRequest.FromTimestamp {
			persistentMetrics, err := b.reader.Read(uuidRequest)
			if err != nil {
				return nil, err
			}

			persistentData := persistentMetrics[uuid]
			data.Points = append(persistentData.Points, data.Points...)
		}

		if len(data.Points) > 0 {
			metrics[uuid] = data
			readPointsCount += len(data.Points)
		}
	}

	requestsPointsTotalRead.Add(float64(readPointsCount))

	return metrics, nil
}

// Returns the deduplicated and sorted points read from the temporary storage according to the request
func (b *Batch) readTemporary(request types.MetricRequest) (map[gouuid.UUID]types.MetricData, error) {
	metrics, _, err := b.memoryStore.ReadPointsAndOffset(request.UUIDs)

	if err != nil {
		return nil, err
	}

	temporaryMetrics := make(map[gouuid.UUID]types.MetricData, len(request.UUIDs))

	for i, data := range metrics {
		var (
			maxTS    int64
			needSort bool
		)

		uuid := request.UUIDs[i]

		temporaryData := types.MetricData{
			TimeToLive: data.TimeToLive,
		}

		for i, point := range data.Points {
			if (point.Timestamp >= request.FromTimestamp) && (point.Timestamp <= request.ToTimestamp) {
				if i == 0 || point.Timestamp > maxTS {
					temporaryData.Points = append(temporaryData.Points, point)
					maxTS = point.Timestamp
				} else if point.Timestamp < maxTS {
					temporaryData.Points = append(temporaryData.Points, point)
					needSort = true
				}
			}
		}

		if needSort {
			temporaryData.Points = types.DeduplicatePoints(temporaryData.Points)
		}

		if len(data.Points) > 0 {
			temporaryMetrics[uuid] = temporaryData
		}
	}

	return temporaryMetrics, nil
}

// Writes metrics in the temporary storage
// Each metric has a state, which will allow you to know if the size of a batch, or the flush date, is reached
// If this is the case, the state is added to the list of states to flush
func (b *Batch) write(metrics []types.MetricData, now time.Time) error {
	start := time.Now()

	defer func() {
		requestsSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	if len(metrics) == 0 {
		return nil
	}

	var (
		writtenPointsCount     int
		ownerShipInitialPoints []types.MetricData
	)

	metricToFlush := make(map[gouuid.UUID]interface{})

	pointsCount, err := b.memoryStore.Append(metrics)
	if err != nil {
		return err
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
			b.states[metrics[i].UUID] = stateData{
				flushDeadline: flushTimestamp(metrics[i].UUID, now, b.batchSize),
			}

			ownerShipInitialPoints = append(ownerShipInitialPoints, metrics[i])
		}

		// Yes we compare number of point with time. This test only here
		// to avoid unbounded grow of temporary store when processing backlog
		// of data.
		if count > int(b.batchSize) {
			_, isOwner := b.states[metrics[i].UUID]
			n := count / int(b.batchSize)

			if isOwner {
				metricToFlush[metrics[i].UUID] = nil
			} else if n > 1 && count < n*int(b.batchSize) {
				// If the number of points crossed a multiple of our b.batchSize
				// (that is count < N*b.batchSize and count >= N*b.batchSize)
				// and it's not the first threshold, then always flush.
				//
				// This will catch case where the current owner is dead, but give him
				// the first threshold to act.
				metricToFlush[metrics[i].UUID] = nil
				nonOwnerWriteTotal.Inc()
			}
		}
	}

	b.mutex.Unlock()

	if len(ownerShipInitialPoints) > 0 {
		offsets := make([]int, len(ownerShipInitialPoints))

		b.setPointsAndOffset(ownerShipInitialPoints, ownerShipInitialPoints, offsets, 0)

		deadlines := make(map[gouuid.UUID]time.Time, len(ownerShipInitialPoints))

		b.mutex.Lock()

		for _, m := range ownerShipInitialPoints {
			deadlines[m.UUID] = b.states[m.UUID].flushDeadline
		}

		b.mutex.Unlock()
		_, err = b.memoryStore.GetSetFlushDeadline(deadlines)

		if err != nil {
			return err
		}
	}

	requestsPointsTotalWrite.Add(float64(writtenPointsCount))

	if len(metricToFlush) != 0 {
		tmp := make([]gouuid.UUID, len(metricToFlush))
		i := 0

		for uuid := range metricToFlush {
			tmp[i] = uuid
			i++
		}

		for startIndex := 0; startIndex < len(tmp); startIndex += flushMetricLimit {
			endIndex := startIndex + flushMetricLimit
			if endIndex > len(tmp) {
				endIndex = len(tmp)
			}

			b.flush(tmp[startIndex:endIndex], now, false)
		}
	}

	return nil
}

// Returns a flush date
// It follows the formula:
// timestamp = (now + batchSize) - (now + batchSize + (uuid.int % batchSize)) % batchSize
func flushTimestamp(uuid gouuid.UUID, now time.Time, batchSize int64) time.Time {
	timestamp := now.Unix() + batchSize
	offset := int64(types.UintFromUUID(uuid) % uint64(batchSize))

	timestamp -= (timestamp + offset) % batchSize

	return time.Unix(timestamp, 0)
}

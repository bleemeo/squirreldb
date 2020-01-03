package batch

import (
	"context"
	"log"
	"os"
	"squirreldb/compare"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

const checkerInterval = 60

const flushMetricLimit = 1000 // Maximum number of metrics to send in one time

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[batch] ", log.LstdFlags)

// Store is an interface to store points associated to metrics. Batch use a memory store (i.e. temporary)
type Store interface {
	Append(newMetrics, existingMetrics map[types.MetricUUID]types.MetricData, timeToLive int64) error
	Get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error)
	Set(metrics map[types.MetricUUID]types.MetricData, timeToLive int64) error
}

// stateData contains information about points stored in memory store for one metrics
type stateData struct {
	pointCount          int
	firstPointTimestamp int64
	lastPointTimestamp  int64
	flushTimestamp      int64
}

// Batch receive a stream of points and send batch for points to the writer. It use a memory store to keep points before
// flushing them to the writer.
// It also allow to read points merging value from the persistent store (reader) and the memory store.
type Batch struct {
	batchSize int64

	states map[types.MetricUUID]stateData
	mutex  sync.Mutex

	memoryStore           Store
	memoryStoreTimeToLive int64
	reader                types.MetricReader
	writer                types.MetricWriter
}

// New creates a new Batch object
func New(batchSize int64, memoryStore Store, reader types.MetricReader, writer types.MetricWriter) *Batch {
	// When adding metrics, the maximum deadline is now + batchSize
	// Every checkerInterval when ensure that metrics expired are flushed
	// Give a 60 second safe margin
	memoryStoreTimeToLive := batchSize + checkerInterval + 60

	batch := &Batch{
		batchSize:             batchSize,
		states:                make(map[types.MetricUUID]stateData),
		memoryStore:           memoryStore,
		memoryStoreTimeToLive: memoryStoreTimeToLive,
		reader:                reader,
		writer:                writer,
	}

	return batch
}

// Read implements MetricReader
func (b *Batch) Read(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	return b.read(request)
}

// Write implements MetricWriter
func (b *Batch) Write(metrics map[types.MetricUUID]types.MetricData) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.write(metrics, time.Now())
}

// Run starts Batch service (e.g. flushing points after a deadline)
func (b *Batch) Run(ctx context.Context) {
	interval := checkerInterval * time.Second
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			b.check(time.Now(), false)
		case <-ctx.Done():
			b.check(time.Now(), true)
			debug.Print(2, logger, "Batch service stopped")

			return
		}
	}
}

// Checks the current states and flush those whose flush date has expired
// If force is true, each state is flushed
func (b *Batch) check(now time.Time, force bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	states := make(map[types.MetricUUID][]stateData)

	for uuid, data := range b.states {
		if (data.flushTimestamp <= now.Unix()) || force {
			states[uuid] = append(states[uuid], data)

			delete(b.states, uuid)

			if (len(states) % flushMetricLimit) == 0 {
				b.flush(states, now)

				states = make(map[types.MetricUUID][]stateData)
			}
		}
	}

	b.flush(states, now)
}

// Flushes the points corresponding to the specified metrics state list
func (b *Batch) flush(states map[types.MetricUUID][]stateData, now time.Time) {
	if len(states) == 0 {
		return
	}

	var (
		readDuration, deleteDuration        time.Duration
		readPointsCount, deletedPointsCount int
	)

	uuids := make([]types.MetricUUID, 0, len(states))

	for uuid := range states {
		uuids = append(uuids, uuid)
	}

	var metrics map[types.MetricUUID]types.MetricData

	retry.Print(func() error {
		start := time.Now()

		var err error
		metrics, err = b.memoryStore.Get(uuids)

		readDuration += time.Since(start)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"get points from the memory store",
	)

	requestsSecondsRead.Observe(readDuration.Seconds())

	metricsToWrite := make(map[types.MetricUUID]types.MetricData)
	metricsToSet := make(map[types.MetricUUID]types.MetricData)

	for uuid, statesData := range states {
		data, exists := metrics[uuid]

		if !exists {
			continue
		}

		dataToWrite, dataToSet := b.flushData(uuid, data, statesData, now)
		metricsToWrite[uuid] = dataToWrite
		metricsToSet[uuid] = dataToSet

		readPointsCount += len(data.Points)
		deletedPointsCount += len(data.Points) - len(dataToSet.Points)
	}

	requestsPointsTotalRead.Add(float64(readPointsCount))
	requestsPointsTotalDelete.Add(float64(deletedPointsCount))

	retry.Print(func() error {
		return b.writer.Write(metricsToWrite)
	},
		retry.NewExponentialBackOff(30*time.Second),
		logger,
		"write points in persistent store",
	)

	retry.Print(func() error {
		start := time.Now()

		err := b.memoryStore.Set(metricsToSet, b.memoryStoreTimeToLive)

		deleteDuration += time.Since(start)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"get points from the memory store",
	)

	requestsSecondsDelete.Observe(deleteDuration.Seconds())
}

// Flushes the points corresponding to the status list of the specified metric
// The points corresponding to the specified state list will be written in the persistent storage
// The points corresponding to the current metric state and points longer than 5 minutes will be written in the temporary storage
func (b *Batch) flushData(uuid types.MetricUUID, data types.MetricData, statesData []stateData, now time.Time) (types.MetricData, types.MetricData) {
	if len(data.Points) == 0 {
		return types.MetricData{}, types.MetricData{}
	}

	currentStateData, exists := b.states[uuid]
	cutoffTimestamp := now.Unix() - b.batchSize

	if exists {
		cutoffTimestamp = compare.MinInt64(cutoffTimestamp, currentStateData.firstPointTimestamp)
	}

	dataToSet := types.MetricData{
		TimeToLive: data.TimeToLive,
	}
	dataToWrite := types.MetricData{
		TimeToLive: data.TimeToLive,
	}

pointsLoop:
	for _, point := range data.Points {
		if point.Timestamp >= cutoffTimestamp {
			dataToSet.Points = append(dataToSet.Points, point)
		}

		for _, stateData := range statesData {
			if (point.Timestamp >= stateData.firstPointTimestamp) && (point.Timestamp <= stateData.lastPointTimestamp) {
				dataToWrite.Points = append(dataToWrite.Points, point)

				continue pointsLoop
			}
		}
	}

	var expectedPointsCount int

	for _, stateData := range statesData {
		expectedPointsCount += stateData.pointCount
	}

	gotPointsCount := len(dataToWrite.Points)

	if gotPointsCount < expectedPointsCount {
		logger.Printf("Warning: Metric %v expected at least %d point(s), got %d point(s)",
			uuid, expectedPointsCount, gotPointsCount)
	}

	return dataToWrite, dataToSet
}

// Returns the deduplicated and sorted points read from the temporary and persistent storage according to the request
func (b *Batch) read(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	if len(request.UUIDs) == 0 {
		return nil, nil
	}

	metrics := make(map[types.MetricUUID]types.MetricData, len(request.UUIDs))

	for _, uuid := range request.UUIDs {
		uuidRequest := types.MetricRequest{
			UUIDs:         []types.MetricUUID{uuid},
			FromTimestamp: request.FromTimestamp,
			ToTimestamp:   request.ToTimestamp,
			Step:          request.Step,
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

		if uuidRequest.ToTimestamp < uuidRequest.FromTimestamp {
			continue
		}

		persistentMetrics, err := b.reader.Read(uuidRequest)
		if err != nil {
			return nil, err
		}

		persistentData := persistentMetrics[uuid]
		data := types.MetricData{
			Points: append(persistentData.Points, temporaryData.Points...),
		}

		if len(data.Points) > 0 {
			metrics[uuid] = data
		}
	}

	return metrics, nil
}

// Returns the deduplicated and sorted points read from the temporary storage according to the request
func (b *Batch) readTemporary(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	start := time.Now()

	metrics, err := b.memoryStore.Get(request.UUIDs)

	requestsSecondsRead.Observe(time.Since(start).Seconds())

	if err != nil {
		return nil, err
	}

	temporaryMetrics := make(map[types.MetricUUID]types.MetricData, len(request.UUIDs))

	var readPointsCount int

	for uuid, data := range metrics {
		temporaryData := types.MetricData{
			TimeToLive: data.TimeToLive,
		}

		for _, point := range data.Points {
			if (point.Timestamp >= request.FromTimestamp) && (point.Timestamp <= request.ToTimestamp) {
				temporaryData.Points = append(temporaryData.Points, point)
			}
		}

		temporaryData.Points = types.DeduplicatePoints(temporaryData.Points)
		temporaryMetrics[uuid] = temporaryData

		readPointsCount += len(data.Points)
	}

	requestsPointsTotalRead.Add(float64(readPointsCount))

	return temporaryMetrics, nil
}

// Writes metrics in the temporary storage
// Each metric has a state, which will allow you to know if the size of a batch, or the flush date, is reached
// If this is the case, the state is added to the list of states to flush
func (b *Batch) write(metrics map[types.MetricUUID]types.MetricData, now time.Time) error {
	if len(metrics) == 0 {
		return nil
	}

	var (
		addDuration      time.Duration
		addedPointsCount int
	)

	states := make(map[types.MetricUUID][]stateData)
	newMetrics := make(map[types.MetricUUID]types.MetricData)
	existingMetrics := make(map[types.MetricUUID]types.MetricData)

	for uuid, data := range metrics {
		newData := types.MetricData{
			TimeToLive: data.TimeToLive,
		}
		existingData := types.MetricData{
			TimeToLive: data.TimeToLive,
		}

		for _, point := range data.Points {
			currentState, exists := b.states[uuid]

			if !exists {
				currentState = stateData{
					pointCount:          1,
					firstPointTimestamp: point.Timestamp,
					lastPointTimestamp:  point.Timestamp,
					flushTimestamp:      flushTimestamp(uuid, now, b.batchSize),
				}
			} else {
				nextFirstPointTimestamp := compare.MinInt64(currentState.firstPointTimestamp, point.Timestamp)
				nextLastPointTimestamp := compare.MaxInt64(currentState.lastPointTimestamp, point.Timestamp)
				nextDelta := nextLastPointTimestamp - nextFirstPointTimestamp

				if (currentState.flushTimestamp < now.Unix()) || (nextDelta >= b.batchSize) {
					states[uuid] = append(states[uuid], currentState)
					delete(b.states, uuid)
					exists = false

					currentState = stateData{
						pointCount:          1,
						firstPointTimestamp: point.Timestamp,
						lastPointTimestamp:  point.Timestamp,
						flushTimestamp:      flushTimestamp(uuid, now, b.batchSize),
					}
				} else {
					currentState.pointCount++
					currentState.firstPointTimestamp = nextFirstPointTimestamp
					currentState.lastPointTimestamp = nextLastPointTimestamp
				}
			}

			b.states[uuid] = currentState

			if !exists {
				newData.Points = append(newData.Points, point)
			} else {
				existingData.Points = append(existingData.Points, point)
			}
		}

		newMetrics[uuid] = newData
		existingMetrics[uuid] = existingData

		addedPointsCount += len(newData.Points) + len(existingData.Points)
	}

	requestsPointsTotalWrite.Add(float64(addedPointsCount))

	start := time.Now()

	if err := b.memoryStore.Append(newMetrics, existingMetrics, b.memoryStoreTimeToLive); err != nil {
		return err
	}

	addDuration += time.Since(start)

	requestsSecondsWrite.Observe(addDuration.Seconds())

	if len(states) != 0 {
		b.flush(states, now)
	}

	return nil
}

// Returns a flush date
// It follows the formula:
// timestamp = (now + batchSize) - (now + batchSize + (uuid.int % batchSize)) % batchSize
func flushTimestamp(uuid types.MetricUUID, now time.Time, batchSize int64) int64 {
	timestamp := now.Unix() + batchSize
	offset := int64(uuid.Uint64() % uint64(batchSize))

	timestamp -= (timestamp + offset) % batchSize

	return timestamp
}

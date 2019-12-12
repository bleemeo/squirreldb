package batch

import (
	"context"
	"log"
	"os"
	"squirreldb/compare"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

const checkerInterval = 60

const flushLimit = 1000

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[batch] ", log.LstdFlags)

type Storer interface {
	Append(newMetrics, existingMetrics map[types.MetricUUID]types.MetricData, timeToLive int64) error
	Get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error)
	Set(metrics map[types.MetricUUID]types.MetricData, timeToLive int64) error
}

type stateData struct {
	pointCount          int
	firstPointTimestamp int64
	lastPointTimestamp  int64
	flushTimestamp      int64
}

type Batch struct {
	batchSize int64

	states map[types.MetricUUID]stateData
	mutex  sync.Mutex

	storer Storer
	reader types.MetricReader
	writer types.MetricWriter
}

// New creates a new Batch object
func New(batchSize int64, storer Storer, reader types.MetricReader, writer types.MetricWriter) *Batch {
	batch := &Batch{
		batchSize: batchSize,
		states:    make(map[types.MetricUUID]stateData),
		storer:    storer,
		reader:    reader,
		writer:    writer,
	}

	return batch
}

// Read is the public method of read
func (b *Batch) Read(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	return b.read(request)
}

// Write is the public method of write
func (b *Batch) Write(metrics map[types.MetricUUID]types.MetricData) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.write(metrics, time.Now())
}

// Run starts all Batch services
func (b *Batch) Run(ctx context.Context) {
	b.runChecker(ctx)
}

// Starts the checker service
// If a stop signal is received, the service is stopped
func (b *Batch) runChecker(ctx context.Context) {
	interval := checkerInterval * time.Second
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			b.check(time.Now(), false)
		case <-ctx.Done():
			b.check(time.Now(), true)
			logger.Println("Checker service stopped")

			return
		}
	}
}

// Checks the current states and flush those whose flush date has expired
// If force is true, each state is flushed
// If the number of states to be flushed is equal to the flushLimit, they will be flushed
func (b *Batch) check(now time.Time, force bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	states := make(map[types.MetricUUID][]stateData)

	for uuid, data := range b.states {
		if (data.flushTimestamp <= now.Unix()) || force {
			states[uuid] = append(states[uuid], data)

			delete(b.states, uuid)

			if (len(states) % flushLimit) == 0 {
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
		metrics, err = b.storer.Get(uuids)

		readDuration += time.Since(start)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't get metrics from the temporary storage",
		"Resolved: Get metrics from the temporary storage")

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
	requestsSecondsRead.Observe(readDuration.Seconds())

	retry.Print(func() error {
		return b.writer.Write(metricsToWrite)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't write metrics in the persistent storage",
		"Resolved: Write metrics in the persistent storage")

	timeToLive := (b.batchSize * 2) + 60

	retry.Print(func() error {
		start := time.Now()

		err := b.storer.Set(metricsToSet, timeToLive)

		deleteDuration += time.Since(start)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't set metrics in the temporary storage",
		"Resolved: Set metrics in the temporary storage")

	requestsPointsTotalDelete.Add(float64(deletedPointsCount))
	requestsSecondsDelete.Observe(deleteDuration.Seconds())
}

// Flushes the points corresponding to the status list of the specified metric
// The points corresponding to the specified state list will be written in the persistent storage
// The points corresponding to the current metric state and points longer than 5 minutes will be written in the temporary storage
func (b *Batch) flushData(uuid types.MetricUUID, data types.MetricData, statesData []stateData, now time.Time) (types.MetricData, types.MetricData) {
	dataToWrite := types.MetricData{
		TimeToLive: data.TimeToLive,
	}
	dataToSet := types.MetricData{
		TimeToLive: data.TimeToLive,
	}

	if len(data.Points) == 0 {
		return dataToWrite, dataToSet
	}

	currentStateData, exists := b.states[uuid]
	cutoffTimestamp := now.Unix() - b.batchSize

	if exists {
		cutoffTimestamp = compare.MinInt64(cutoffTimestamp, currentStateData.firstPointTimestamp)
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

		var temporaryMetrics map[types.MetricUUID]types.MetricData

		retry.Print(func() error {
			var err error
			temporaryMetrics, err = b.readTemporary(uuidRequest)

			return err
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"Error: Can't get metrics from the temporary storage",
			"Resolved: Get metrics from the temporary storage")

		temporaryData := temporaryMetrics[uuid]

		if len(temporaryData.Points) > 0 {
			uuidRequest.ToTimestamp = temporaryData.Points[0].Timestamp - 1
		}

		if uuidRequest.ToTimestamp <= uuidRequest.FromTimestamp {
			continue
		}

		var persistentMetrics map[types.MetricUUID]types.MetricData

		retry.Print(func() error {
			var err error
			persistentMetrics, err = b.reader.Read(uuidRequest)

			return err
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"Error: Can't get metrics from the persistent storage",
			"Resolved: Get metrics from the persistent storage")

		persistentData := persistentMetrics[uuid]
		data := types.MetricData{
			Points: append(persistentData.Points, temporaryData.Points...),
		}

		metrics[uuid] = data
	}

	return metrics, nil
}

// Returns the deduplicated and sorted points read from the temporary storage according to the request
func (b *Batch) readTemporary(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	var (
		readDuration    time.Duration
		readPointsCount int
	)

	start := time.Now()

	metrics, err := b.storer.Get(request.UUIDs)

	readDuration += time.Since(start)

	if err != nil {
		return nil, err
	}

	temporaryMetrics := make(map[types.MetricUUID]types.MetricData, len(request.UUIDs))

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
	requestsSecondsRead.Observe(readDuration.Seconds())

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

	retry.Print(func() error {
		timeToLive := (b.batchSize * 2) + 60 // Add 60 seconds as safety margin

		start := time.Now()

		err := b.storer.Append(newMetrics, existingMetrics, timeToLive)

		addDuration += time.Since(start)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't append metrics points in the temporary storage",
		"Resolved: Append metrics points in the temporary storage")

	requestsPointsTotalWrite.Add(float64(addedPointsCount))
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

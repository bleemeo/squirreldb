package batch

import (
	"context"
	"log"
	"math/big"
	"os"
	"squirreldb/compare"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

const (
	checkerInterval = 60
)

var (
	logger = log.New(os.Stdout, "[batch] ", log.LstdFlags)
)

type Storer interface {
	Append(newMetrics, actualMetrics types.Metrics, timeToLive int64) error
	Get(uuids types.MetricUUIDs) (types.Metrics, error)
	Set(newMetrics, actualMetrics types.Metrics, timeToLive int64) error
}

type state struct {
	pointCount          int
	firstPointTimestamp int64
	lastPointTimestamp  int64
	flushTimestamp      int64
}

type Batch struct {
	batchSize int64

	store  Storer
	reader types.MetricReader
	writer types.MetricWriter

	states map[types.MetricUUID]state
	mutex  sync.Mutex
}

// New creates a new Batch object
func New(batchSize int64, temporaryStorer Storer, persistentReader types.MetricReader, persistentWriter types.MetricWriter) *Batch {
	return &Batch{
		batchSize: batchSize,
		store:     temporaryStorer,
		reader:    persistentReader,
		writer:    persistentWriter,
		states:    make(map[types.MetricUUID]state),
	}
}

// Read() is the public function of read()
func (b *Batch) Read(request types.MetricRequest) (types.Metrics, error) {
	return b.read(request)
}

// Write is the public function of write()
func (b *Batch) Write(metrics types.Metrics) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.write(metrics, time.Now())
}

// Run calls check() every checker interval seconds
// If the context receives a stop signal, a last check is made before stopping the service
func (b *Batch) Run(ctx context.Context) {
	ticker := time.NewTicker(checkerInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.check(time.Now(), false)
		case <-ctx.Done():
			b.check(time.Now(), true)
			logger.Println("Run: Stopped")

			return
		}
	}
}

// Checks all current states and adds states, whose deadlines are exceeded, in the flush queue
func (b *Batch) check(now time.Time, flushAll bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	nowUnix := now.Unix()
	flushQueue := make(map[types.MetricUUID][]state)

	for uuid, state := range b.states {
		if (state.flushTimestamp < nowUnix) || flushAll {
			flushQueue[uuid] = append(flushQueue[uuid], state)

			delete(b.states, uuid)
		}
	}

	if len(flushQueue) != 0 {
		b.flush(flushQueue, now)
	}
}

// Transfers all metrics according to the states in the flush queue from the temporary storage to
// the persistent storage
func (b *Batch) flush(flushQueue map[types.MetricUUID][]state, now time.Time) {
	uuids := make(types.MetricUUIDs, 0, len(flushQueue))

	for uuid := range flushQueue {
		uuids = append(uuids, uuid)
	}

	var temporaryMetrics types.Metrics

	retry.Do(func() error {
		var err error
		temporaryMetrics, err = b.store.Get(uuids)

		return err
	}, "batch", "flush",
		"Can't get metrics from the temporary storage",
		"Resolved: Get metrics from the temporary storage",
		retry.NewBackOff(30*time.Second))

	cutoff := now.Unix() + b.batchSize
	metricsToSet := make(types.Metrics)
	metricsToWrite := make(types.Metrics)

	for uuid, states := range flushQueue {
		currentState := b.states[uuid]
		temporaryMetricData := temporaryMetrics[uuid]

		// Points to set
		var pointsToSet types.MetricPoints

		for _, point := range temporaryMetricData.Points {
			if (point.Timestamp >= cutoff) || (point.Timestamp >= currentState.firstPointTimestamp) {
				pointsToSet = append(pointsToSet, point)
			}
		}

		data := types.MetricData{
			Points:     pointsToSet,
			TimeToLive: temporaryMetricData.TimeToLive,
		}
		metricsToSet[uuid] = data

		// Points to write
		var pointsToWrite types.MetricPoints

		for _, state := range states {
			for _, point := range temporaryMetricData.Points {
				if (point.Timestamp >= state.firstPointTimestamp) && (point.Timestamp <= state.lastPointTimestamp) {
					pointsToWrite = append(pointsToWrite, point)
				}
			}
		}

		data = types.MetricData{
			Points:     pointsToWrite,
			TimeToLive: temporaryMetricData.TimeToLive,
		}
		metricsToWrite[uuid] = data
	}

	retry.Do(func() error {
		timeToLive := (b.batchSize * 2) + 60 // Add 60 seconds as safety margin

		return b.store.Set(nil, metricsToSet, timeToLive)
	}, "batch", "flush",
		"Can't set metrics in the temporary storage",
		"Resolved: Set metrics in the temporary storage",
		retry.NewBackOff(30*time.Second))

	retry.Do(func() error {
		return b.writer.Write(metricsToWrite)
	}, "batch", "flush",
		"Can't write metrics in the persistent storage",
		"Resolved: Write metrics in the persistent storage",
		retry.NewBackOff(30*time.Second))
}

// Returns metrics from the temporary and permanent storages according to the request
func (b *Batch) read(request types.MetricRequest) (types.Metrics, error) {
	metrics := make(types.Metrics)

	// Retrieves metrics from the temporary storage according to the request
	var temporaryMetrics types.Metrics

	retry.Do(func() error {
		var err error
		temporaryMetrics, err = b.store.Get(request.UUIDs)

		return err
	}, "batch", "read",
		"Can't get metrics from the temporary storage",
		"Resolved: Get metrics from the temporary storage",
		retry.NewBackOff(30*time.Second))

	for uuid, temporaryMetricData := range temporaryMetrics {
		var points types.MetricPoints

		for _, point := range temporaryMetricData.Points {
			if point.Timestamp >= request.FromTimestamp && point.Timestamp <= request.ToTimestamp {
				points = append(points, point)
			}
		}

		data := types.MetricData{
			Points: points.Deduplicate(),
		}
		metrics[uuid] = data
	}

	// Retrieves metrics from the persistent storage
	var persistentMetrics types.Metrics

	retry.Do(func() error {
		var err error
		persistentMetrics, err = b.reader.Read(request)

		return err
	}, "batch", "read",
		"Can't read metrics from the persistent storage",
		"Resolved: Read metrics from the persistent storage",
		retry.NewBackOff(30*time.Second))

	for uuid, persistentMetricData := range persistentMetrics {
		data := types.MetricData{
			Points: append(persistentMetricData.Points, metrics[uuid].Points...),
		}
		metrics[uuid] = data
	}

	return metrics, nil
}

// Writes metrics in the temporary storage
// Each metric will have a current state that will allow to know if the size of a batch, or the deadline is reached
// When one of these cases occurs, the current state is added to the flush queue
func (b *Batch) write(metrics types.Metrics, now time.Time) error {
	nowUnix := now.Unix()
	flushQueue := make(map[types.MetricUUID][]state)
	newMetrics := make(types.Metrics)
	actualMetrics := make(types.Metrics)

	for uuid, metricData := range metrics {
		var newMetricPoints, actualMetricPoints types.MetricPoints

		for _, point := range metricData.Points {
			currentState, exists := b.states[uuid]

			if !exists {
				currentState = state{
					pointCount:          1,
					firstPointTimestamp: point.Timestamp,
					lastPointTimestamp:  point.Timestamp,
					flushTimestamp:      flushTimestamp(uuid, now, b.batchSize),
				}
			} else {
				nextFirstPointTimestamp := compare.Int64Min(currentState.firstPointTimestamp, point.Timestamp)
				nextLastPointTimestamp := compare.Int64Max(currentState.lastPointTimestamp, point.Timestamp)
				nextDelta := nextLastPointTimestamp - nextFirstPointTimestamp

				if (currentState.flushTimestamp < nowUnix) || (nextDelta >= b.batchSize) {
					flushQueue[uuid] = append(flushQueue[uuid], currentState)
					delete(b.states, uuid)
					exists = false

					currentState = state{
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
				newMetricPoints = append(newMetricPoints, point)
			} else {
				actualMetricPoints = append(actualMetricPoints, point)
			}
		}

		data := types.MetricData{
			TimeToLive: metricData.TimeToLive,
			Points:     newMetricPoints,
		}

		newMetrics[uuid] = data

		data = types.MetricData{
			TimeToLive: metricData.TimeToLive,
			Points:     actualMetricPoints,
		}

		actualMetrics[uuid] = data
	}

	retry.Do(func() error {
		timeToLive := (b.batchSize * 2) + 60 // Add 60 seconds as safety margin

		return b.store.Append(newMetrics, actualMetrics, timeToLive)
	}, "batch", "write",
		"Can't append metrics points in the temporary storage",
		"Resolved: Append metrics points in the temporary storage",
		retry.NewBackOff(30*time.Second))

	if len(flushQueue) != 0 {
		b.flush(flushQueue, now)
	}

	return nil
}

// Returns a flush deadline
// It generates a flush date for each metric state every batchSize seconds and shift them among themselves
//
// It follows the formula:
// deadline = (now + batchSize) - (now + batchSize + (uuid.int % batchSize)) % batchSize
func flushTimestamp(uuid types.MetricUUID, now time.Time, batchSize int64) int64 {
	uuidBigInt := big.NewInt(0).SetBytes(uuid.Bytes())
	deadline := now.Unix() + batchSize
	offset := int64(uuidBigInt.Uint64() % uint64(batchSize))

	deadline -= (deadline + offset) % batchSize

	return deadline
}

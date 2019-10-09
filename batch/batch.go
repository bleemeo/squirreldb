package batch

import (
	"github.com/cenkalti/backoff"
	"log"
	"os"
	"squirreldb/types"
	"sync"
	"time"
)

var (
	logger  = log.New(os.Stdout, "[batch] ", log.LstdFlags)
	backOff = backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: 0.5,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
)

type Storer interface {
	Append(newMetrics, actualMetrics map[types.MetricUUID]types.MetricData) error
	Get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error)
	Set(newMetrics, actualMetrics map[types.MetricUUID]types.MetricData) error
}

type state struct {
	pointCount          int
	firstPointTimestamp int64
	lastPointTimestamp  int64
	flushDeadline       int64
}

type Batch struct {
	storer Storer
	reader types.MetricReader
	writer types.MetricWriter

	states map[types.MetricUUID]state
	mutex  sync.Mutex
}

// NewBatch creates a new Batch object
func NewBatch(temporaryStorer Storer, persistentReader types.MetricReader, persistentWriter types.MetricWriter) *Batch {
	return &Batch{
		storer: temporaryStorer,
		reader: persistentReader,
		writer: persistentWriter,
		states: make(map[types.MetricUUID]state),
	}
}

// Read() is the public function of read()
func (b *Batch) Read(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	return b.read(request)
}

// Write is the public function of write()
func (b *Batch) Write(metrics map[types.MetricUUID]types.MetricData) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.write(metrics, time.Now(), 300) // TODO: Batch size from config
}

// Checks all current states and adds states whose deadlines are passed in the flush queue
func (b *Batch) check(now time.Time, batchSize int64, flushAll bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	nowUnix := now.Unix()
	flushQueue := make(map[types.MetricUUID][]state)

	for uuid, state := range b.states {
		if state.flushDeadline < nowUnix {
			flushQueue[uuid] = append(flushQueue[uuid], state)
		}
	}

	if len(flushQueue) != 0 {
		b.flush(flushQueue, now, batchSize)
	}
}

// Transfers all metrics that comply with the states in the flush queue from the temporary storage to
// the persistent storage
func (b *Batch) flush(flushQueue map[types.MetricUUID][]state, now time.Time, batchSize int64) {
	uuids := make([]types.MetricUUID, 0, len(flushQueue))

	for uuid := range flushQueue {
		uuids = append(uuids, uuid)
	}

	var temporaryMetrics map[types.MetricUUID]types.MetricData

	_ = backoff.Retry(func() error {
		var err error
		temporaryMetrics, err = b.storer.Get(uuids)

		if err != nil {
			logger.Println("flush: Can't get points from the temporary storage (", err, ")")
		}

		return err
	}, &backOff)

	cutoff := now.Unix() + batchSize
	metricsToSet := make(map[types.MetricUUID]types.MetricData)
	metricsToWrite := make(map[types.MetricUUID]types.MetricData)

	for uuid, states := range flushQueue {
		currentState := b.states[uuid]
		var data types.MetricData

		for _, point := range temporaryMetrics[uuid].Points {
			if (point.Timestamp >= cutoff) && (point.Timestamp <= currentState.lastPointTimestamp) {
				data.Points = append(data.Points, point)
			}
		}

		metricsToSet[uuid] = data

		for _, state := range states {
			var data types.MetricData

			for _, point := range temporaryMetrics[uuid].Points {
				if (point.Timestamp >= state.firstPointTimestamp) && (point.Timestamp <= state.lastPointTimestamp) {
					data.Points = append(data.Points, point)
				}
			}

			metricsToWrite[uuid] = data
		}
	}

	_ = backoff.Retry(func() error {
		err := b.storer.Set(nil, metricsToSet)

		if err != nil {
			logger.Println("flush: Can't set points to the temporary storage (", err, ")")
		}

		return err
	}, &backOff)

	_ = backoff.Retry(func() error {
		err := b.writer.Write(metricsToWrite)

		if err != nil {
			logger.Println("flush: Can't write points to the persistent storage (", err, ")")
		}

		return err
	}, &backOff)
}

// Returns metrics from temporary and permanent storage that comply with the request
func (b *Batch) read(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	metrics := make(map[types.MetricUUID]types.MetricData)

	// Retrieves metrics from the temporary storage that comply with the request
	var temporaryMetrics map[types.MetricUUID]types.MetricData

	_ = backoff.Retry(func() error {
		var err error
		temporaryMetrics, err = b.storer.Get(request.UUIDs)

		if err != nil {
			logger.Println("flush: Can't get points from the temporary storage (", err, ")")
		}

		return err
	}, &backOff)

	for uuid, tData := range temporaryMetrics {
		var data types.MetricData

		for _, point := range tData.Points {
			if point.Timestamp >= request.FromTimestamp && point.Timestamp <= request.ToTimestamp {
				data.Points = append(data.Points, point)
			}
		}

		metrics[uuid] = data
	}

	// Retrieves pre-filtered metrics from the persistent storage
	var persistentMetrics map[types.MetricUUID]types.MetricData

	_ = backoff.Retry(func() error {
		var err error
		persistentMetrics, err = b.reader.Read(request)

		if err != nil {
			logger.Println("read: Can't read points from the persistent storage (", err, ")")
		}

		return err
	}, &backOff)

	for uuid, tData := range persistentMetrics {
		var data types.MetricData
		item := metrics[uuid]

		for _, point := range tData.Points {
			if point.Timestamp >= request.FromTimestamp && point.Timestamp <= request.ToTimestamp {
				data.Points = append(data.Points, point)
			}
		}

		item.Points = append(item.Points, data.Points...)

		metrics[uuid] = item
	}

	return metrics, nil
}

// Writes metrics in the temporary storage
// Each metric will have a current state that will allow to know if the size of a batch, or the deadline is reached
// When one of these cases occurs, the current state is added to the flush queue
func (b *Batch) write(metrics map[types.MetricUUID]types.MetricData, now time.Time, batchSize int64) error {
	nowUnix := now.Unix()
	newMetrics := make(map[types.MetricUUID]types.MetricData)
	actualMetrics := make(map[types.MetricUUID]types.MetricData)
	flushQueue := make(map[types.MetricUUID][]state)

	for uuid, data := range metrics {
		for _, point := range data.Points {
			currentState, exists := b.states[uuid]

			if !exists {
				currentState = state{
					pointCount:          1,
					firstPointTimestamp: point.Timestamp,
					lastPointTimestamp:  point.Timestamp,
					flushDeadline:       flushDeadline(uuid, now, batchSize),
				}
			} else {
				nextFirstPointTimestamp := currentState.firstPointTimestamp
				nextLastPointTimestamp := currentState.lastPointTimestamp

				if point.Timestamp < currentState.firstPointTimestamp {
					nextFirstPointTimestamp = point.Timestamp
				}

				if point.Timestamp > currentState.lastPointTimestamp {
					nextLastPointTimestamp = point.Timestamp
				}

				nextDelta := nextLastPointTimestamp - nextFirstPointTimestamp

				if (currentState.flushDeadline < nowUnix) || (nextDelta >= batchSize) {
					flushQueue[uuid] = append(flushQueue[uuid], currentState)
					delete(b.states, uuid)
					exists = false

					currentState = state{
						pointCount:          1,
						firstPointTimestamp: point.Timestamp,
						lastPointTimestamp:  point.Timestamp,
						flushDeadline:       flushDeadline(uuid, now, batchSize),
					}
				} else {
					currentState.pointCount++
					currentState.firstPointTimestamp = nextFirstPointTimestamp
					currentState.lastPointTimestamp = nextLastPointTimestamp
				}
			}

			b.states[uuid] = currentState

			if !exists {
				item := newMetrics[uuid]

				item.Points = append(item.Points, point)

				newMetrics[uuid] = item
			} else {
				item := actualMetrics[uuid]

				item.Points = append(item.Points, point)

				actualMetrics[uuid] = item
			}
		}
	}

	_ = backoff.Retry(func() error {
		err := b.storer.Append(newMetrics, actualMetrics)

		if err != nil {
			logger.Println("write: Can't append points in the temporary storage (", err, ")")
		}

		return err
	}, &backOff)

	if len(flushQueue) != 0 {
		b.flush(flushQueue, now, batchSize)
	}

	return nil
}

// Returns a flush deadline
// It generates a flush period for each metric state every batchSize seconds and shift them among themselves
//
// It follows the formula:
// deadline = (now + batchSize) - (now + batchSize + (uuid.int % batchSize)) % batchSize
func flushDeadline(uuid types.MetricUUID, now time.Time, batchSize int64) int64 {
	deadline := now.Unix() + batchSize
	offset := int64(uuid.Uint64() % uint64(batchSize))

	deadline = deadline - ((deadline + offset) % batchSize)

	return deadline
}

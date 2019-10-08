package batch

import (
	"context"
	"github.com/cenkalti/backoff"
	"log"
	"os"
	"squirreldb/config"
	"squirreldb/types"
	"sync"
	"time"
)

var (
	logger             = log.New(os.Stdout, "[batch] ", log.LstdFlags)
	exponentialBackOff = backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: 0.5,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
)

type MetricStorer interface {
	Append(newPoints, existingPoints map[string][]types.Point) error
	Get(keys []string) (map[string][]types.Point, error)
	Set(newPoints, existingPoints map[string][]types.Point) error
}

type state struct {
	types.Metric
	pointCount     int
	firstPointTime time.Time
	lastPointTime  time.Time
	flushDeadline  time.Time
}

type Batch struct {
	temporaryStorage   MetricStorer
	persistentStorageR types.MetricReader
	persistentStorageW types.MetricWriter

	states map[string]state
	mutex  sync.Mutex
}

// NewBatch creates a new Batch object
func NewBatch(temporaryStorage MetricStorer, persistentStorageR types.MetricReader, persistentStorageW types.MetricWriter) *Batch {
	return &Batch{
		temporaryStorage:   temporaryStorage,
		persistentStorageR: persistentStorageR,
		persistentStorageW: persistentStorageW,
		states:             make(map[string]state),
	}
}

// Read() is the public function of read()
func (b *Batch) Read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	return b.read(mRequest)
}

// RunChecker calls check() every BatchCheckerInterval seconds
// If the context receives a stop signal, a last check is made before stopping the service
func (b *Batch) RunChecker(ctx context.Context, wg *sync.WaitGroup) {
	batchSize := config.C.Duration("batch.size") * time.Second
	ticker := time.NewTicker(config.C.Duration("batch.checker_interval") * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.check(time.Now(), batchSize, false)
		case <-ctx.Done():
			b.check(time.Now(), batchSize, true)
			logger.Println("RunChecker: Stopped")
			wg.Done()
			return
		}
	}
}

// Write is the public function of write()
func (b *Batch) Write(msPoints []types.MetricPoints) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.write(msPoints, time.Now(), config.C.Duration("batch.size")*time.Second)
}

// Checks all current states and adds states whose deadlines are passed in a waiting list in order to flush them
func (b *Batch) check(now time.Time, batchSize time.Duration, flushAll bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	stateQueue := make(map[string][]state)

	for key, state := range b.states {
		if state.flushDeadline.Before(now) || flushAll {
			stateQueue[key] = append(stateQueue[key], state)
		}
	}

	if len(stateQueue) != 0 {
		b.flush(stateQueue, now, batchSize)
	}
}

// Transfers all points that comply with the properties of the state queue from the temporary storage to
// the persistent storage
func (b *Batch) flush(stateQueue map[string][]state, now time.Time, batchSize time.Duration) {
	keysPointsToSet := make(map[string][]types.Point)

	keys := make([]string, 0, len(stateQueue))

	for key := range stateQueue {
		keys = append(keys, key)
	}

	var temporaryKeysPoints map[string][]types.Point

	_ = backoff.Retry(func() error {
		var err error
		temporaryKeysPoints, err = b.temporaryStorage.Get(keys)

		if err != nil {
			logger.Println("flush: Can't get points from the temporary storage (", err, ")")
		}

		return err
	}, &exponentialBackOff)

	var msPointsToWrite []types.MetricPoints

	for key, metricStates := range stateQueue {
		for _, state := range metricStates {
			mPointsToWrite := types.MetricPoints{
				Metric: state.Metric,
				Points: nil,
			}

			for _, point := range temporaryKeysPoints[key] {
				if !point.Time.Before(state.firstPointTime) && !point.Time.After(state.lastPointTime) {
					mPointsToWrite.Points = append(mPointsToWrite.Points, point)
				}
			}

			msPointsToWrite = append(msPointsToWrite, mPointsToWrite)
		}

		currentState := b.states[key]
		cutoff := now.Add(batchSize)
		var pointsToSet []types.Point

		for _, point := range temporaryKeysPoints[key] {
			if !point.Time.Before(cutoff) || !point.Time.Before(currentState.firstPointTime) {
				pointsToSet = append(pointsToSet, point)
			}
		}

		keysPointsToSet[key] = pointsToSet
	}

	_ = backoff.Retry(func() error {
		err := b.persistentStorageW.Write(msPointsToWrite)

		if err != nil {
			logger.Println("flush: Can't write points to the persistent storage (", err, ")")
		}

		return err
	}, &exponentialBackOff)

	_ = backoff.Retry(func() error {
		err := b.temporaryStorage.Set(nil, keysPointsToSet)

		if err != nil {
			logger.Println("flush: Can't set points to the temporary storage (", err, ")")
		}

		return err
	}, &exponentialBackOff)
}

// Retrieves points from temporary and permanent storage that comply with the request and returns
// a list of MetricPoints
func (b *Batch) read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	results := make(map[string]types.MetricPoints)
	var mUUID types.MetricUUID

	_ = backoff.Retry(func() error {
		var err error
		mUUID, err = mRequest.UUID()

		if err != nil {
			logger.Println("read: Can't generate UUID (", err, ")")
		}

		return err
	}, &exponentialBackOff)

	keys := []string{mUUID.String()}

	// Retrieves the points from the temporary storage, filters them according to the request and
	// generates a MetricPoints
	var temporaryKeysPoints map[string][]types.Point

	_ = backoff.Retry(func() error {
		var err error
		temporaryKeysPoints, err = b.temporaryStorage.Get(keys)

		if err != nil {
			logger.Println("read: Can't get points from the temporary storage (", err, ")")
		}

		return err
	}, &exponentialBackOff)

	for key, points := range temporaryKeysPoints {
		var temporaryPoints []types.Point

		for _, point := range points {
			if !point.Time.Before(mRequest.FromTime) && !point.Time.After(mRequest.ToTime) {
				temporaryPoints = append(temporaryPoints, point)
			}
		}

		temporaryMPoints := types.MetricPoints{
			Metric: mRequest.Metric,
			Points: temporaryPoints,
		}

		results[key] = temporaryMPoints
	}

	// Retrieves the pre-filtered MetricPoints from the persistent storage, and add them to the result map or
	// appends their points if a MetricPoints already exists for the metric in the result map
	var persistentMsPoints []types.MetricPoints

	_ = backoff.Retry(func() error {
		var err error
		persistentMsPoints, err = b.persistentStorageR.Read(mRequest)

		if err != nil {
			logger.Println("read: Can't read points from the persistent storage (", err, ")")
		}

		return err
	}, &exponentialBackOff)

	for _, persistentMPoint := range persistentMsPoints {
		var mUUID types.MetricUUID

		_ = backoff.Retry(func() error {
			var err error
			mUUID, err = mRequest.UUID()

			if err != nil {
				logger.Println("read: Can't generate UUID (", err, ")")
			}

			return err
		}, &exponentialBackOff)

		key := mUUID.String()
		mPoints, exists := results[key]

		if !exists {
			mPoints = persistentMPoint
		} else {
			mPoints.Points = append(mPoints.Points, persistentMPoint.Points...)
		}

		results[key] = mPoints
	}

	var msPoints []types.MetricPoints

	for _, mPoint := range results {
		msPoints = append(msPoints, mPoint)
	}

	return msPoints, nil
}

// Retrieves the points to be written and stores them in the temporary storage
// Each metric will have a current state that will allow to know if the size of a batch, or the deadline is reached.
// When one of these cases occurs, the current state is added to the waiting list and flush() is called
func (b *Batch) write(msPoints []types.MetricPoints, now time.Time, batchSize time.Duration) error {
	newPoints := make(map[string][]types.Point)
	existingPoints := make(map[string][]types.Point)
	stateQueue := make(map[string][]state)

	for _, mPoints := range msPoints {
		var mUUID types.MetricUUID

		_ = backoff.Retry(func() error {
			var err error
			mUUID, err = mPoints.UUID()

			if err != nil {
				logger.Println("write: Can't generate UUID (", err, ")")
			}

			return err
		}, &exponentialBackOff)

		key := mUUID.String()

		for _, point := range mPoints.Points {
			currentState, exists := b.states[key]

			if !exists {
				currentState = state{
					Metric:         mPoints.Metric,
					pointCount:     1,
					firstPointTime: point.Time,
					lastPointTime:  point.Time,
					flushDeadline:  flushDeadline(mPoints.Metric, now, batchSize),
				}
			} else {
				nextFirstPointTime := currentState.firstPointTime
				nextLastPointTime := currentState.lastPointTime

				if point.Time.Before(nextFirstPointTime) {
					nextFirstPointTime = point.Time
				}

				if point.Time.After(nextLastPointTime) {
					nextLastPointTime = point.Time
				}

				nextDelta := nextLastPointTime.Sub(nextFirstPointTime)

				// If the flush date of the current state is exceeded or the time between the smallest and
				// largest time exceeds the size of a batch, then we add the current state to the list of states
				// from metrical to flush and recreate a current state for the iterated point.
				// If this is not the case, the times of the first and last point of the current state
				// are redefined and the point counter is increased
				if currentState.flushDeadline.Before(now) || (nextDelta.Seconds() >= batchSize.Seconds()) {
					stateQueue[key] = append(stateQueue[key], currentState)
					exists = false
					delete(b.states, key)

					currentState = state{
						Metric:         mPoints.Metric,
						pointCount:     1,
						firstPointTime: point.Time,
						lastPointTime:  point.Time,
						flushDeadline:  now.Add(batchSize),
					}
				} else {
					currentState.pointCount++
					currentState.firstPointTime = nextFirstPointTime
					currentState.lastPointTime = nextLastPointTime
				}
			}

			b.states[key] = currentState

			if !exists {
				newPoints[key] = append(newPoints[key], point)
			} else {
				existingPoints[key] = append(existingPoints[key], point)
			}
		}
	}

	_ = backoff.Retry(func() error {
		err := b.temporaryStorage.Append(newPoints, existingPoints)

		if err != nil {
			logger.Println("write: Can't append points in the temporary storage (", err, ")")
		}

		return err
	}, &exponentialBackOff)

	if len(stateQueue) != 0 {
		b.flush(stateQueue, now, batchSize)
	}

	return nil
}

// Generates and returns a flush deadline.
// It allows to generate a flush period for each metric state every batchSize seconds and to shift them
// among themselves thanks to the value of the uuid of the metrics
//
// It follows the formula:
// deadline = (now + batchSize) - (now + batchSize + (uuid.int % batchSize)) % batchSize
func flushDeadline(metric types.Metric, now time.Time, batchSize time.Duration) time.Time {
	var mUUID types.MetricUUID

	_ = backoff.Retry(func() error {
		var err error
		mUUID, err = metric.UUID()

		if err != nil {
			logger.Println("flushDeadline: Can't generate UUID (", err, ")")
		}

		return err
	}, &exponentialBackOff)

	flush := now.Add(batchSize).Unix()
	offset := mUUID.Int64() % int64(batchSize.Seconds())

	flush = flush - (flush+offset)%int64(batchSize.Seconds())

	flushDeadline := time.Unix(flush, 0)

	return flushDeadline
}

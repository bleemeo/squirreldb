package batch

import (
	"context"
	"log"
	"os"
	"squirreldb/config"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

var logger = log.New(os.Stdout, "[batch] ", log.LstdFlags)

type MetricStorer interface {
	Append(newPoints, existingPoints map[string][]types.Point) error
	Get(key string) ([]types.Point, error)
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

func NewBatch(temporaryStorage MetricStorer, persistentStorageR types.MetricReader, persistentStorageW types.MetricWriter) *Batch {
	return &Batch{
		temporaryStorage:   temporaryStorage,
		persistentStorageR: persistentStorageR,
		persistentStorageW: persistentStorageW,
		states:             make(map[string]state),
	}
}

func (b *Batch) RunChecker(ctx context.Context, wg *sync.WaitGroup) {
	ticker := time.NewTicker(config.BatchCheckerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = b.check(time.Now(), config.BatchLength, false)
		case <-ctx.Done():
			logger.Println("RunChecker: Stopping...")
			_ = b.check(time.Now(), config.BatchLength, true)
			logger.Println("RunChecker: Stopped")
			wg.Done()
			return
		}
	}
}

func (b *Batch) Read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	return b.read(mRequest)
}

func (b *Batch) Write(msPoints []types.MetricPoints) error {
	return b.write(msPoints, time.Now(), config.BatchLength)
}

func (b *Batch) check(now time.Time, batchLength time.Duration, flushAll bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	stateQueue := make(map[string][]state)

	for key, state := range b.states {
		if state.flushDeadline.Before(now) || flushAll {
			stateQueue[key] = append(stateQueue[key], state)
		}
	}

	if len(stateQueue) != 0 {
		_ = b.flush(stateQueue, now, batchLength)
	}

	return nil
}

func (b *Batch) flush(stateQueue map[string][]state, now time.Time, batchLength time.Duration) error {
	var msPointsToWrite []types.MetricPoints
	msPointsToSet := make(map[string][]types.Point)

	for key, states := range stateQueue {
		var points []types.Point

		retry.Endlessly(config.BatchRetryDelay, func() error {
			var err error
			points, err = b.temporaryStorage.Get(key)

			if err != nil {
				logger.Printf("flush: Can't get points from temporaryStorage (%v)"+"\n", err)
			}

			return err
		}, logger)

		for _, state := range states {
			mPointsToWrite := types.MetricPoints{
				Metric: state.Metric,
				Points: nil,
			}

			for _, point := range points {
				if !point.Time.Before(state.firstPointTime) && !point.Time.After(state.lastPointTime) {
					mPointsToWrite.Points = append(mPointsToWrite.Points, point)
				}
			}

			msPointsToWrite = append(msPointsToWrite, mPointsToWrite)
		}

		currentState := b.states[key]
		cutoff := now.Add(batchLength)
		var pointsToSet []types.Point

		for _, point := range points {
			if !point.Time.Before(cutoff) || !point.Time.Before(currentState.firstPointTime) {
				pointsToSet = append(pointsToSet, point)
			}
		}

		msPointsToSet[key] = pointsToSet
	}

	retry.Endlessly(config.BatchRetryDelay, func() error {
		err := b.persistentStorageW.Write(msPointsToWrite)

		if err != nil {
			logger.Printf("flush: Can't write metrics points to persistentStorage (%v)"+"\n", err)
		}

		return err
	}, logger)

	retry.Endlessly(config.BatchRetryDelay, func() error {
		err := b.temporaryStorage.Set(nil, msPointsToSet)

		if err != nil {
			logger.Printf("flush: Can't set metrics points to temporaryStorage (%v)"+"\n", err)
		}

		return err
	}, logger)

	return nil
}

func (b *Batch) read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	// TODO: Handle error
	mUUID, _ := mRequest.UUID()
	key := mUUID.String()
	points, _ := b.temporaryStorage.Get(key)
	var temporaryPoints []types.Point

	for _, point := range points {
		if !point.Time.Before(mRequest.FromTime) && !point.Time.After(mRequest.ToTime) {
			temporaryPoints = append(temporaryPoints, point)
		}
	}

	results := make(map[string]types.MetricPoints)

	temporaryMPoints := types.MetricPoints{
		Metric: mRequest.Metric,
		Points: temporaryPoints,
	}

	results[key] = temporaryMPoints

	// TODO: Handle error
	persistentMsPoints, _ := b.persistentStorageR.Read(mRequest)

	for _, persistentMPoint := range persistentMsPoints {
		// TODO: Handle error
		mUUID, _ := persistentMPoint.UUID()
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

func (b *Batch) write(msPoints []types.MetricPoints, now time.Time, batchLength time.Duration) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	newPoints := make(map[string][]types.Point)
	existingPoints := make(map[string][]types.Point)
	stateQueue := make(map[string][]state)

	for _, mPoints := range msPoints {
		// TODO: Handle error
		mUUID, _ := mPoints.UUID()
		key := mUUID.String()

		for _, point := range mPoints.Points {
			currentState, exists := b.states[key]

			if !exists {
				currentState = state{
					Metric:         mPoints.Metric,
					pointCount:     1,
					firstPointTime: point.Time,
					lastPointTime:  point.Time,
					flushDeadline:  flushDeadline(mPoints.Metric, now, batchLength),
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

				nextDeltaDuration := nextLastPointTime.Sub(nextFirstPointTime)

				if currentState.flushDeadline.Before(now) || (nextDeltaDuration.Seconds() >= batchLength.Seconds()) {
					stateQueue[key] = append(stateQueue[key], currentState)
					exists = false
					delete(b.states, key)

					currentState = state{
						Metric:         mPoints.Metric,
						pointCount:     1,
						firstPointTime: point.Time,
						lastPointTime:  point.Time,
						flushDeadline:  now.Add(batchLength),
					}
				} else {
					currentState.pointCount += 1
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

	retry.Endlessly(config.BatchRetryDelay, func() error {
		err := b.temporaryStorage.Append(newPoints, existingPoints)

		if err != nil {
			logger.Printf("write: Can't append metrics points to temporaryStorage (%v)"+"\n", err)
		}

		return err
	}, logger)

	if len(stateQueue) != 0 {
		_ = b.flush(stateQueue, now, config.BatchLength)
	}

	return nil
}

func flushDeadline(metric types.Metric, now time.Time, batchLength time.Duration) time.Time {
	// TODO: Handle error
	mUUID, _ := metric.UUID()
	flush := now.Add(batchLength).Unix()

	offset := mUUID.Int64() % int64(batchLength.Seconds())

	flush = flush - ((flush + offset) % int64(batchLength.Seconds()))

	flushDeadline := time.Unix(flush, 0)

	return flushDeadline
}

package batch

import (
	"context"
	"hamsterdb/cassandra"
	"hamsterdb/config"
	"hamsterdb/retry"
	"hamsterdb/types"
	"log"
	"math/big"
	"os"
	"strings"
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
	temporaryStorage  MetricStorer
	persistentStorage types.MetricWriter

	states map[string]state
	mutex  sync.Mutex
}

func NewBatch(temporaryStorage MetricStorer, persistentStorage types.MetricWriter) *Batch {
	return &Batch{
		temporaryStorage:  temporaryStorage,
		persistentStorage: persistentStorage,
		states:            make(map[string]state),
	}
}

func (b *Batch) RunChecker(ctx context.Context, wg *sync.WaitGroup) {
	ticker := time.NewTicker(config.BatchCheckerInterval * time.Second)
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

	batchLength *= time.Second

	for key, states := range stateQueue {
		var points []types.Point

		retry.Endlessly(config.BatchRetryDelay*time.Second, func() error {
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

	retry.Endlessly(config.BatchRetryDelay*time.Second, func() error {
		err := b.persistentStorage.Write(msPointsToWrite)

		if err != nil {
			logger.Printf("flush: Can't write metrics points to persistentStorage (%v)"+"\n", err)
		}

		return err
	}, logger)

	retry.Endlessly(config.BatchRetryDelay*time.Second, func() error {
		err := b.temporaryStorage.Set(nil, msPointsToSet)

		if err != nil {
			logger.Printf("flush: Can't set metrics points to temporaryStorage (%v)"+"\n", err)
		}

		return err
	}, logger)

	return nil
}

func (b *Batch) write(msPoints []types.MetricPoints, now time.Time, batchLength time.Duration) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	newPoints := make(map[string][]types.Point)
	existingPoints := make(map[string][]types.Point)
	stateQueue := make(map[string][]state)

	batchLength *= time.Second

	for _, mPoints := range msPoints {
		key := mPoints.CanonicalLabels()

		for _, point := range mPoints.Points {
			currentState, exists := b.states[key]

			if !exists {
				currentState = state{
					Metric:         mPoints.Metric,
					pointCount:     1,
					firstPointTime: point.Time,
					lastPointTime:  point.Time,
					flushDeadline:  flushDeadline(mPoints, now, batchLength/time.Second),
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

	retry.Endlessly(config.BatchRetryDelay*time.Second, func() error {
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

func flushDeadline(mPoints types.MetricPoints, now time.Time, batchLength time.Duration) time.Time {
	uuidString := cassandra.MetricUUID(mPoints.Metric).String()
	uuidParsed := strings.Replace(uuidString, "-", "", -1)
	uuidBigInt, _ := big.NewInt(0).SetString(uuidParsed, 16)

	batchLength *= time.Second

	batchBigInt := big.NewInt(int64(batchLength.Seconds()))
	offsetBigInt := uuidBigInt.Mod(uuidBigInt, batchBigInt)

	flushBatch := now.Add(batchLength).Unix()

	flushBatch = flushBatch - ((flushBatch + offsetBigInt.Int64()) % batchBigInt.Int64())

	flushDeadline := time.Unix(flushBatch, 0)

	return flushDeadline
}

package batch

import (
	"hamsterdb/config"
	"hamsterdb/types"
	"hamsterdb/util"
	"log"
	"sync"
	"time"
)

type Storer interface {
	Append(newMsPoints map[string]types.MetricPoints, currentMsPoints map[string]types.MetricPoints) error
	Get(key string) (types.MetricPoints, error)
	Set(newMsPoints map[string]types.MetricPoints, currentMsPoints map[string]types.MetricPoints) error
}

type state struct {
	pointCount     int
	firstPointTime time.Time
	lastPointTime  time.Time
	flushDeadline  time.Time
}

type batch struct {
	temporaryStorage  Storer
	persistentStorage types.Writer

	states map[string]state
	mutex  sync.Mutex
}

func NewBatch(temporaryStorage Storer, persistentStorage types.Writer) *batch {
	return &batch{
		temporaryStorage:  temporaryStorage,
		persistentStorage: persistentStorage,
		states:            make(map[string]state),
	}
}

func (b *batch) RunFlushChecker(stopChn chan bool) {
	ticker := time.NewTicker(config.BatchCheckerDelay * time.Second)

	for {
		select {
		case <-ticker.C:
			_ = b.flushCheck(time.Now())
		case <-stopChn:
			ticker.Stop()
			return
		}
	}
}

func (b *batch) flush(flushQueue map[string][]state, currentTime time.Time, batchTimeLength float64) error {
	// Create the list of metrics to be written
	var msPointsToWrite []types.MetricPoints
	// Create the list of metrics to be set
	msPointsToSet := make(map[string]types.MetricPoints)

	// Iteration on each metric state
	for key, states := range flushQueue {
		var mPoints types.MetricPoints

		if retryErr := util.Retry(config.BatchGetAttempts, config.BatchGetTimeout*time.Second, func() error {
			var err error
			mPoints, err = b.temporaryStorage.Get(key)

			if err != nil {
				log.Printf("[batch] Flush: Can't get metric points from temporaryStorage (%v)", err)
			}

			return err
		}); retryErr != nil {
			return retryErr
		}

		// Iteration on each state of a metric
		for _, state := range states {
			// Created a metric with the points to be written
			mPointsToWrite := types.MetricPoints{
				Metric: mPoints.Metric,
			}

			// Iteration on each point of a metric
			for _, point := range mPoints.Points {
				//  Checks if the point is between the time of the first point and the last point of the state
				if util.TimeBetween(point.Time, state.firstPointTime, state.lastPointTime) {
					mPointsToWrite.Points = append(mPointsToWrite.Points, point)
				}
			}

			// Adds the metric to be written to the list of metrics to be written
			msPointsToWrite = append(msPointsToWrite, mPointsToWrite)
		}

		// Lists the points to be deleted
		cutoff := currentTime.Add(time.Duration(-batchTimeLength) * time.Second)
		currentState := b.states[key]
		var pointsTimeToDelete []time.Time

		// Iteration on each point of a metric
		for _, point := range mPoints.Points {
			// Checks whether or not the point time is before the cut-off time or
			// whether or not it is before the time of the first point of the metric ongoing state
			if point.Time.Before(cutoff) && point.Time.Before(currentState.firstPointTime) {
				pointsTimeToDelete = append(pointsTimeToDelete, point.Time)
			}
		}

		// Deletes expired points from their time
		mPoints.RemovePoints(pointsTimeToDelete)

		// Adds the metric to the list of metrics to be set
		msPointsToSet[key] = mPoints
	}

	// Write the list of metrics to be written
	if retryErr := util.Retry(config.BatchWriteAttempts, config.BatchWriteTimeout*time.Second, func() error {
		err := b.persistentStorage.Write(msPointsToWrite)

		if err != nil {
			log.Printf("[batch] Flush: Can't write in persistentStorage (%v)", err)
		}

		return err
	}); retryErr != nil {
		return retryErr
	}

	// Define the list of metrics to be defined
	if retryErr := util.Retry(config.BatchSetAttempts, config.BatchSetTimeout*time.Second, func() error {
		err := b.temporaryStorage.Set(nil, msPointsToSet)

		if err != nil {
			log.Printf("[batch] Flush: Can't set metrics points in temporaryStorage (%v)", err)
		}

		return err
	}); retryErr != nil {
		return retryErr
	}

	return nil
}

func (b *batch) flushCheck(currentTime time.Time) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	flushQueue := make(map[string][]state)

	// Iteration on each ongoing state
	for key, state := range b.states {
		// Checks if the deadline time is before or not the current time
		if state.flushDeadline.Before(currentTime) {
			// Adds the state to the flush queue and remove it from ongoing states
			flushQueue[key] = append(flushQueue[key], state)
			delete(b.states, key)
		}
	}

	// Check if there are any points to flush and do it if this is the case
	if len(flushQueue) != 0 {
		if err := b.flush(flushQueue, time.Now(), config.BatchTimeLength); err != nil {
			return err
		}
	}

	return nil
}

func (b *batch) Write(msPoints []types.MetricPoints) error {
	return b.write(msPoints, time.Now(), config.BatchTimeLength)
}

func (b *batch) write(msPoints []types.MetricPoints, currentTime time.Time, batchTimeLength float64) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	currentMsPoints := make(map[string]types.MetricPoints)
	newMsPoints := make(map[string]types.MetricPoints)
	flushQueue := make(map[string][]state)

	// Iterate on each MetricPoints
	for _, mPoints := range msPoints {
		key := mPoints.CanonicalLabels()

		// Iterate on each point of a MetricPoint
		for _, point := range mPoints.Points {
			currentState, stateExists := b.states[key]

			// Checks whether the state exists or not
			if !stateExists {
				// Create a new state
				currentState = state{
					pointCount:     1,
					firstPointTime: point.Time,
					lastPointTime:  point.Time,
					flushDeadline:  point.Time.Add(time.Duration(batchTimeLength) * time.Second),
				}

				// Adds the point in the new points map
				newMPoints := newMsPoints[key]
				newMPoints.Labels = mPoints.Labels
				newMPoints.Points = append(newMPoints.Points, point)
				newMsPoints[key] = newMPoints
			} else {
				// Search for a time before the first point or after the last point
				nextFirstPointTime, nextLastPointTime := currentState.firstPointTime, currentState.lastPointTime

				if point.Time.Before(nextFirstPointTime) {
					nextFirstPointTime = point.Time
				}

				if point.Time.After(nextLastPointTime) {
					nextLastPointTime = point.Time
				}

				// Checks the delta duration between the first and last point
				nextDeltaDuration := nextLastPointTime.Sub(nextFirstPointTime)

				// Checks whether the deadline date has expired or not and
				// whether the delta duration calculated is either less or more than the duration of a batch
				if currentState.flushDeadline.Before(currentTime) || (nextDeltaDuration.Seconds() >= batchTimeLength) {
					// Adds the state to the flush queue and remove it from ongoing states
					flushQueue[key] = append(flushQueue[key], currentState)
					delete(b.states, key)

					currentState = state{
						pointCount:     1,
						firstPointTime: point.Time,
						lastPointTime:  point.Time,
						flushDeadline:  point.Time.Add(time.Duration(batchTimeLength) * time.Second),
					}
				} else {
					// Increases the point count and assigns the new times
					currentState.pointCount += 1
					currentState.firstPointTime = nextFirstPointTime
					currentState.lastPointTime = nextLastPointTime

					// Adds the point to the current point map
					currentMPoints := currentMsPoints[key]
					currentMPoints.Labels = mPoints.Labels
					currentMPoints.Points = append(currentMPoints.Points, point)
					currentMsPoints[key] = currentMPoints
				}
			}

			// Adds it to the state map
			b.states[key] = currentState
		}
	}

	// Append the points in the temporary store
	if retryErr := util.Retry(config.BatchAppendAttempts, config.BatchAppendTimeout*time.Second, func() error {
		err := b.temporaryStorage.Append(newMsPoints, currentMsPoints)

		if err != nil {
			log.Printf("[batch] Write: Can't append metrics points in temporaryStorage (%v)", err)
		}

		return err
	}); retryErr != nil {
		return retryErr
	}

	// Check if there are any points to flush and do it if this is the case
	if len(flushQueue) != 0 {
		if err := b.flush(flushQueue, currentTime, batchTimeLength); err != nil {
			return err
		}
	}

	return nil
}

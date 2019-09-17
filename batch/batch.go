package batch

import (
	"hamsterdb/config"
	"hamsterdb/types"
	"hamsterdb/util"
	"sync"
	"time"
)

type State struct {
	pointCount     int
	firstPointTime time.Time
	lastPointTime  time.Time
	flushDeadline  time.Time
}

type Batch struct {
	temporaryStorage  types.Storer
	persistentStorage types.Writer
	states            map[string]State
	mutex             sync.Mutex
}

func NewBatch(temporaryStorage types.Storer, persistentStorage types.Writer) *Batch {
	return &Batch{
		temporaryStorage:  temporaryStorage,
		persistentStorage: persistentStorage,
		states:            make(map[string]State),
	}
}

func (batch *Batch) InitChecker() (chan bool, chan error) {
	stopChn := make(chan bool)
	errorChn := make(chan error)

	go func() {
		for {
			errorChn <- batch.FlushChecker()
			select {
			case <-time.After(config.BatchCheckerDuration):
			case <-stopChn:
				return
			}
		}
	}()

	return stopChn, errorChn
}

func (batch *Batch) Flush(flushQueue map[string][]State) error {
	return batch.flush(flushQueue, time.Now(), config.BatchDuration)
}

func (batch *Batch) flush(flushQueue map[string][]State, currentTime time.Time, batchDuration float64) error {
	// Create the list of metrics to be written
	var msPointsToWrite []types.MetricPoints
	// Create the list of metrics to be set
	msPointsToSet := make(map[string]types.MetricPoints)

	// Iteration on each metric state
	for key, states := range flushQueue {
		mPoints, err := batch.temporaryStorage.Get(key)

		if err != nil {
			return err
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
		cutoff := currentTime.Add(time.Duration(-batchDuration) * time.Second)
		ongoingState := batch.states[key]
		var pointsTimeToDelete []time.Time

		// Iteration on each point of a metric
		for _, point := range mPoints.Points {
			// Checks whether or not the point time is before the cut-off time or
			// whether or not it is before the time of the first point of the metric ongoing state
			if point.Time.Before(cutoff) && point.Time.Before(ongoingState.firstPointTime) {
				pointsTimeToDelete = append(pointsTimeToDelete, point.Time)
			}
		}

		// Deletes expired points from their time
		mPoints.RemovePoints(pointsTimeToDelete)

		// Adds the metric to the list of metrics to be set
		msPointsToSet[key] = mPoints
	}

	// Write the list of metrics to be written
	if err := batch.persistentStorage.Write(msPointsToWrite); err != nil {
		return err
	}

	// Define the list of metrics to be defined
	if err := batch.temporaryStorage.Set(nil, msPointsToSet); err != nil {
		return err
	}

	return nil
}

func (batch *Batch) FlushChecker() error {
	return batch.flushChecker(time.Now())
}

func (batch *Batch) flushChecker(currentTime time.Time) error {
	flushQueue := make(map[string][]State)

	// Iteration on each ongoing state
	for key, state := range batch.states {
		// Checks if the deadline time is before or not the current time
		if state.flushDeadline.Before(currentTime) {
			// Adds the state to the flush queue and remove it from ongoing states
			flushQueue[key] = append(flushQueue[key], state)
			delete(batch.states, key)
		}
	}

	// Check if there are any points to flush and do it if this is the case
	if len(flushQueue) != 0 {
		if err := batch.Flush(flushQueue); err != nil {
			return err
		}
	}

	return nil
}

func (batch *Batch) Write(msPoints []types.MetricPoints) error {
	return batch.write(msPoints, time.Now(), config.BatchDuration)
}

func (batch *Batch) write(msPoints []types.MetricPoints, currentTime time.Time, batchDuration float64) error {
	currentMsPoints := make(map[string]types.MetricPoints)
	newMsPoints := make(map[string]types.MetricPoints)
	flushQueue := make(map[string][]State)

	// Iterate on each MetricPoints
	for _, mPoints := range msPoints {
		key := mPoints.CanonicalLabels()

		// Iterate on each point of a MetricPoint
		for i, length := 0, len(mPoints.Points); i < length; i++ {
			point := mPoints.Points[i]
			state, stateExists := batch.states[key]

			// Checks whether the state exists or not
			if !stateExists {
				// Create a new state
				state = State{
					pointCount:     1,
					firstPointTime: point.Time,
					lastPointTime:  point.Time,
					flushDeadline:  point.Time.Add(time.Duration(batchDuration) * time.Second),
				}

				// Adds it to the state map
				batch.states[key] = state

				// Adds the point in the new points map
				newMPoints, newMPointsExists := newMsPoints[key]

				if !newMPointsExists {
					newMPoints.Labels = mPoints.Labels
				}

				newMPoints.Points = append(newMPoints.Points, point)
				newMsPoints[key] = newMPoints
			} else {
				// Search for a time before the first point or after the last point
				nextFirstPointTime, nextLastPointTime := state.firstPointTime, state.lastPointTime

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
				if state.flushDeadline.Before(currentTime) || (nextDeltaDuration.Seconds() > batchDuration) {
					// Adds the state to the flush queue and remove it from ongoing states
					flushQueue[key] = append(flushQueue[key], state)
					delete(batch.states, key)
					i -= 1
				} else {
					// Increases the point count and assigns the new times
					state.pointCount += 1
					state.firstPointTime = nextFirstPointTime
					state.lastPointTime = nextLastPointTime

					// Assigns the state to its key in the state map
					batch.states[key] = state

					// Adds the point to the current point map
					currentMPoints := currentMsPoints[key]
					currentMPoints.Labels = mPoints.Labels
					currentMPoints.Points = append(currentMPoints.Points, point)
					currentMsPoints[key] = currentMPoints
				}
			}
		}
	}

	// Append the points in the temporary store
	if err := batch.temporaryStorage.Append(newMsPoints, currentMsPoints); err != nil {
		return err
	}

	// Check if there are any points to flush and do it if this is the case
	if len(flushQueue) != 0 {
		if err := batch.Flush(flushQueue); err != nil {
			return err
		}
	}

	return nil
}

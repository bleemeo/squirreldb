package batch

import (
	"hamsterdb/config"
	"hamsterdb/types"
	"hamsterdb/util"
	"sync"
	"time"
)

type State struct {
	pointNumbers   int
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

func (batch *Batch) Flush(flushQueue map[string][]State) error {
	var sendingMsPoints []types.MetricPoints

	for key, states := range flushQueue {
		mPoints, err := batch.temporaryStorage.Get(key)

		if err != nil {
			return err
		}

		for _, state := range states {
			sendingMPoints := types.MetricPoints{
				Metric: mPoints.Metric,
			}

			for _, point := range mPoints.Points {
				if util.TimeBetween(point.Time, state.firstPointTime, state.lastPointTime) {
					sendingMPoints.Points = append(sendingMPoints.Points, point)
				}
			}

			sendingMsPoints = append(sendingMsPoints, sendingMPoints)
		}
	}

	if err := batch.persistentStorage.Write(sendingMsPoints); err != nil {
		return err
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

	for i := 0; i < len(msPoints); i++ {
		mPoints := msPoints[i]
		key := mPoints.CanonicalLabels()

		for i, length := 0, len(mPoints.Points); i < length; i++ {
			point := mPoints.Points[i]
			state, stateExists := batch.states[key]

			if !stateExists {
				state = State{
					pointNumbers:   1,
					firstPointTime: point.Time,
					lastPointTime:  point.Time,
					flushDeadline:  point.Time.Add(time.Duration(batchDuration) * time.Second),
				}

				batch.states[key] = state

				newMPoints := newMsPoints[key]
				newMPoints.Labels = mPoints.Labels
				newMPoints.Points = append(newMPoints.Points, point)
				newMsPoints[key] = newMPoints
			} else {
				nextFirstPointTime, nextLastPointTime := state.firstPointTime, state.lastPointTime

				if point.Time.Before(nextFirstPointTime) {
					nextFirstPointTime = point.Time
				}

				if point.Time.After(nextLastPointTime) {
					nextLastPointTime = point.Time
				}

				deltaDuration := nextLastPointTime.Sub(nextFirstPointTime)

				if state.flushDeadline.Before(currentTime) || (deltaDuration.Seconds() > batchDuration) {
					flushQueue[key] = append(flushQueue[key], state)
					delete(batch.states, key)
					i -= 1
				} else {
					state.pointNumbers += 1
					state.firstPointTime = nextFirstPointTime
					state.lastPointTime = nextLastPointTime

					batch.states[key] = state

					currentMPoints := currentMsPoints[key]
					currentMPoints.Labels = mPoints.Labels
					currentMPoints.Points = append(currentMPoints.Points, point)
					currentMsPoints[key] = currentMPoints
				}
			}
		}
	}

	if err := batch.temporaryStorage.Set(newMsPoints, currentMsPoints); err != nil {
		return err
	}

	if len(flushQueue) != 0 {
		if err := batch.Flush(flushQueue); err != nil {
			return err
		}
	}

	return nil
}

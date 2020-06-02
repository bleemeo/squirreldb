package batch

import (
	"squirreldb/types"
	"time"
)

type walReadIter struct {
	w       *WalBatcher
	request types.MetricRequest
	offset  int
	err     error
	current types.MetricData
}

func (i *walReadIter) Next() bool {
	start := time.Now()

	defer func() {
		requestsSecondsRead.Observe(time.Since(start).Seconds())
	}()

	idRequest := types.MetricRequest{
		IDs:           []types.MetricID{0}, // value 0 will be changed later in this function
		FromTimestamp: i.request.FromTimestamp,
		ToTimestamp:   i.request.ToTimestamp,
		StepMs:        i.request.StepMs,
		Function:      i.request.Function,
	}

	for {
		if i.offset >= len(i.request.IDs) {
			return false
		}

		id := i.request.IDs[i.offset]
		i.offset++

		idRequest.IDs[0] = id

		i.w.writeLock.L.Lock()
		data := i.w.dirty[id]
		data2 := i.w.writing[id]
		i.w.writeLock.L.Unlock()

		result := filterPoints(idRequest, data2, data)

		if result.TimeToLive < data2.TimeToLive {
			result.TimeToLive = data2.TimeToLive
		}

		peristentToTimestamp := idRequest.ToTimestamp
		if len(result.Points) > 0 {
			peristentToTimestamp = result.Points[0].Timestamp - 1
		}

		resultPeristent, err := i.onePersistentMetric(idRequest, peristentToTimestamp)
		if err != nil {
			i.err = err
			return false
		}

		if len(resultPeristent.Points) > 0 {
			result.Points = append(resultPeristent.Points, result.Points...)
		}

		if result.TimeToLive < resultPeristent.TimeToLive {
			result.TimeToLive = resultPeristent.TimeToLive
		}

		if len(result.Points) > 0 {
			i.current = result
			requestsPointsTotalRead.Add(float64(len(result.Points)))

			return true
		}
	}
}

func (i *walReadIter) onePersistentMetric(idRequest types.MetricRequest, toTimestamp int64) (types.MetricData, error) {
	idRequest.ToTimestamp = toTimestamp

	persistentMetrics, err := i.w.PersitentStore.ReadIter(idRequest)
	if err != nil {
		return types.MetricData{}, err
	}

	for persistentMetrics.Next() {
		persistentData := persistentMetrics.At()
		if persistentData.ID == idRequest.IDs[0] {
			return persistentData, nil
		}
	}

	return types.MetricData{}, nil
}

func (i *walReadIter) Err() error {
	return i.err
}

func (i *walReadIter) At() types.MetricData {
	return i.current
}

// req must contains only one ID and datas assume be be for this ID or nil.
func filterPoints(req types.MetricRequest, datas ...types.MetricData) types.MetricData {
	result := types.MetricData{
		ID: req.IDs[0],
	}

	var (
		needSort bool
		lastTS   int64
	)

	for _, data := range datas {
		for _, point := range data.Points {
			if (point.Timestamp >= req.FromTimestamp) && (point.Timestamp <= req.ToTimestamp) {
				if len(result.Points) == 0 || lastTS < point.Timestamp {
					result.Points = append(result.Points, point)
				} else if lastTS > point.Timestamp {
					result.Points = append(result.Points, point)
					needSort = true
				}
			}
		}

		if result.TimeToLive < data.TimeToLive {
			result.TimeToLive = data.TimeToLive
		}
	}

	if needSort {
		result.Points = types.DeduplicatePoints(result.Points)
	}

	return result
}

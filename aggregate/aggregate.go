package aggregate

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
	logger             = log.New(os.Stdout, "[aggregate] ", log.LstdFlags)
	exponentialBackOff = &backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: 0.5,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
)

type Aggregate struct {
	persistentStorageR types.MetricReader
	persistentStorageW types.MetricWriter
}

// * OK
// TODO: Add documentation
func NewAggregate(persistentStorageR types.MetricReader, persistentStorageW types.MetricWriter) *Aggregate {
	return &Aggregate{
		persistentStorageR: persistentStorageR,
		persistentStorageW: persistentStorageW,
	}
}

func (a *Aggregate) RunAggregator(ctx context.Context, wg *sync.WaitGroup) {
	now := time.Now()
	startOffset := config.C.Int64("aggregate.aggregator_start_offset")
	dayTimestamp := now.Unix() - (now.Unix() % 86400)
	dayStartTimestamp := dayTimestamp + startOffset
	waitTimestamp := dayStartTimestamp - now.Unix()

	if waitTimestamp < 0 {
		waitTimestamp += 86400
	}

	select {
	case <-time.After(time.Duration(waitTimestamp) * time.Second):
	case <-ctx.Done():
		logger.Println("RunAggregator: Stopped")
		wg.Done()
		return
	}

	aggregatePeriod := config.C.Int64("aggregate.period")
	aggregateSize := config.C.Int64("aggregate.size")
	aggregateEndOffset := config.C.Int64("aggregate.end_offset")
	tickerInit := false
	ticker := time.NewTicker(10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !tickerInit {
				ticker.Stop()
				ticker = time.NewTicker(config.C.Duration("aggregate.aggregator_interval") * time.Second)
				tickerInit = true
			}

			a.aggregate(time.Now(), aggregatePeriod, aggregateSize, aggregateEndOffset)
		case <-ctx.Done():
			logger.Println("RunAggregator: Stopped")
			wg.Done()
			return
		}
	}
}

func (a *Aggregate) aggregate(now time.Time, aggregatePeriod, aggregateSize, aggregateEndOffset int64) {
	toTimestamp := now.Unix() - (now.Unix() % aggregateSize) + aggregateEndOffset
	fromTimestamp := toTimestamp - aggregateSize

	metricUUIDS := []string{
		"0057b0d4-8f93-16a8-450e-fd76d060fae1",
		"008fb79d-0f6c-23aa-dcfc-8b68b60bc4d9",
		"009d66c1-c206-10b4-1e6d-c53ef48e80d1",
		"0464bc67-86db-d491-886b-5f0a8978af53",
	}
	results := make(map[string]types.MetricPoints)

	for i := fromTimestamp; i < toTimestamp; i += aggregatePeriod {
		for _, metricUUID := range metricUUIDS {
			aggregatedMPoints := a.aggregateMetric(metricUUID, i, i+aggregatePeriod)
			item, exists := results[metricUUID]

			if !exists {
				item = aggregatedMPoints
			} else {
				item.Points = append(item.Points, aggregatedMPoints.Points...)
			}

			results[metricUUID] = item
		}
	}

	var msPoints []types.MetricPoints

	for _, mPoint := range results {
		msPoints = append(msPoints, mPoint)
	}

	_ = backoff.Retry(func() error {
		err := a.persistentStorageW.Write(msPoints)

		if err != nil {
			logger.Println("aggregate: Can't write to persistentStorage")
		}

		return err
	}, exponentialBackOff)
}

func (a *Aggregate) aggregateMetric(metricUUID string, fromTimestamp, toTimestamp int64) types.MetricPoints {
	mRequest := types.MetricRequest{
		Metric: types.Metric{Labels: map[string]string{
			"__uuid__": metricUUID,
		}},
		FromTime: time.Unix(fromTimestamp, 0),
		ToTime:   time.Unix(toTimestamp, 0),
	}

	var msPoints []types.MetricPoints

	aggregatedMPoints := types.MetricPoints{
		Metric: mRequest.Metric,
	}

	aggregatedMPoints.Labels["__aggregated__"] = "true"

	_ = backoff.Retry(func() error {
		var err error
		msPoints, err = a.persistentStorageR.Read(mRequest)

		if err != nil {
			logger.Println("aggregateMetric: Can't read from persistentStorage (", err, ")")
		}

		return err
	}, exponentialBackOff)

	for _, mPoints := range msPoints {
		values := calculate(mPoints.Points)

		for _, value := range values {
			point := types.Point{
				Time:  time.Unix(fromTimestamp, 0),
				Value: value,
			}

			aggregatedMPoints.Points = append(mPoints.Points, point)
		}
	}

	return aggregatedMPoints
}

func calculate(points []types.Point) []float64 {
	values := []float64{0, 0, 0, float64(len(points))}

	for i, point := range points {
		if i == 0 {
			values[0] = point.Value
			values[1] = point.Value
		} else {
			if point.Value < values[0] {
				values[0] = point.Value
			} else if point.Value > values[1] {
				values[1] = point.Value
			}
		}

		values[2] += point.Value
	}

	return values
}

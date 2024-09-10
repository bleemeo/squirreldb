package tsdb

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/aggregate"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/retry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	shardStatePrefix = "aggregate_until_shard_"
	shardNumber      = 60
)

const (
	lockTimeToLive = 10 * time.Minute
)

// MaxPastDelay is the maximum delay in the past a points could be write without aggregated data issue.
// Any point wrote with a timestamp more than MaxPastDelay in the past is likely to be ignored in aggregated data.
// This delay will also force the aggregation to run after 0h00 UTC + MaxPastDelay
// (since we check often, it will start at this time).
const MaxPastDelay = 8 * time.Hour

// run starts all CassandraTSDB pre-aggregations.
func (c *CassandraTSDB) run(ctx context.Context) {
	shard := rand.Intn(shardNumber) //nolint:gosec
	aggregateShardIntended := c.options.AggregateIntendedDuration / time.Duration(shardNumber)
	ticker := time.NewTicker(aggregateShardIntended)
	consecutiveNothingDone := 0 // number of time aggregateShard did nothing in a row

	var (
		lastNotifiedAggretedFrom  time.Time
		lastNotifiedAggretedUntil time.Time
	)

	aggregatedUntil := make(map[int]time.Time, shardNumber)

	defer ticker.Stop()

	for ctx.Err() == nil {
		start := time.Now()
		workDone, tmp := c.aggregateShard(ctx, shard, &lastNotifiedAggretedFrom)

		if workDone {
			c.metrics.AggregationSeconds.Observe(time.Since(start).Seconds())
		}

		if !tmp.IsZero() {
			aggregatedUntil[shard] = tmp
		}

		shard = (shard + 1) % shardNumber

		if !workDone {
			consecutiveNothingDone++
		} else {
			consecutiveNothingDone = 0
		}

		if !workDone && consecutiveNothingDone < shardNumber {
			continue
		}

		if len(aggregatedUntil) == shardNumber {
			var minTime time.Time

			for _, t := range aggregatedUntil {
				if minTime.IsZero() || t.Before(minTime) {
					minTime = t
				}
			}

			c.metrics.AggregatdUntilSeconds.Set(float64(minTime.Unix()))

			if minTime.After(lastNotifiedAggretedUntil) {
				c.logger.Info().Msgf("All shard are aggregated until %s", minTime)
				lastNotifiedAggretedUntil = minTime

				c.l.Lock()
				c.fullyAggregatedAt = minTime
				c.l.Unlock()
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			c.logger.Debug().Msg("Cassandra TSDB service stopped")

			return
		}
	}
}

// InternalWriteAggregated writes all specified metrics as aggregated data
// This method should only by used for benchmark/tests or bulk import.
// This will replace data for given aggregated row (based on timestamp of first time).
// Points in each AggregatedData MUST be sorted in ascending order.
// Also if writingTimestamp is not 0, it's the timestamp used to write in Cassandra (in microseconds since epoc).
func (c *CassandraTSDB) InternalWriteAggregated(
	ctx context.Context,
	metrics []aggregate.AggregatedData,
	writingTimestamp int64,
) error {
	if len(metrics) == 0 {
		return nil
	}

	var wg sync.WaitGroup

	wg.Add(concurrentWriterCount)

	step := len(metrics) / concurrentWriterCount

	for i := range concurrentWriterCount {
		startIndex := i * step
		endIndex := (i + 1) * step

		if endIndex > len(metrics) || i == concurrentWriterCount-1 {
			endIndex = len(metrics)
		}

		go func() {
			defer logger.ProcessPanic()
			defer wg.Done()

			for _, data := range metrics[startIndex:endIndex] {
				retry.Print(func() error {
					return c.writeAggregateData(ctx, data, writingTimestamp) //nolint:scopelint
				}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
					c.logger,
					"write aggregated points to Cassandra",
				)

				if ctx.Err() != nil {
					break
				}
			}
		}()
	}

	wg.Wait()

	return nil
}

// ForcePreAggregation will pre-aggregate all metrics between two given time
// The forced pre-aggregation will run even if data are already pre-aggregated and
// will not change the stored position where pre-aggregation think it stopped.
// The from & to will be extended to be aligned with the aggregation size.
// Metrics will be filtered by labels if `matchers` is not nil.
//
// Forcing pre-aggregation should only serve when:
//  1. a bug caused the normal pre-aggregation to have anormal result
//  2. data were inserted with too many delay and normal pre-aggregation already
//     processed the time range.
func (c *CassandraTSDB) ForcePreAggregation(
	ctx context.Context,
	threadCount int,
	from time.Time, to time.Time,
	matchers []*labels.Matcher,
) error {
	var (
		wg         sync.WaitGroup
		rangeCount int

		l           sync.Mutex
		pointsCount int
	)

	start := time.Now()
	workChan := make(chan time.Time)

	c.logger.Info().Msgf("Forced pre-aggregation requested between %v and %v", from, to)

	currentFrom := from
	currentFrom = currentFrom.Truncate(aggregateSize)

	wg.Add(threadCount)

	for range threadCount {
		go func() {
			defer logger.ProcessPanic()
			defer wg.Done()

			var ids []types.MetricID

			for currentFrom := range workChan {
				var rangePointsCount int

				rangeStart := time.Now()
				currentTo := currentFrom.Add(aggregateSize)

				retry.Print(func() error {
					var err error

					if matchers != nil {
						ids, err = c.findMatchingMetrics(ctx, currentFrom, currentTo, matchers)
					} else {
						ids, err = c.index.AllIDs(ctx, currentFrom, currentTo)
					}

					return err //nolint:wrapcheck
				}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
					c.logger,
					"get IDs from the index",
				)

				if ctx.Err() != nil {
					break
				}

				retry.Print(func() error {
					var err error

					rangePointsCount, err = c.doAggregation(
						ctx,
						ids,
						currentFrom.UnixNano()/1000000,
						currentTo.UnixNano()/1000000,
						aggregateResolution.Milliseconds(),
					)

					return err
				}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
					c.logger,
					fmt.Sprintf("forced pre-aggregation from %v to %v", currentFrom, currentTo),
				)

				if ctx.Err() != nil {
					break
				}

				rangeCount++

				l.Lock()
				pointsCount += rangePointsCount
				l.Unlock()

				delta := time.Since(rangeStart)

				c.logger.Info().Msgf(
					"Forced pre-aggregation from %v to %v completed in %v (read %d pts, %.0fk pts/s)",
					currentFrom,
					currentTo,
					delta.Truncate(time.Millisecond),
					rangePointsCount,
					float64(rangePointsCount)/delta.Seconds()/1000,
				)
			}
		}()
	}

outter:
	for !to.Before(currentFrom) {
		select {
		case workChan <- currentFrom:
		case <-ctx.Done():
			break outter
		}

		currentFrom = currentFrom.Add(aggregateSize)
	}

	close(workChan)
	wg.Wait()

	delta := time.Since(start)
	c.logger.Info().Msgf(
		"Aggregated %d range and %d points in %v (%.2fk points/s)",
		rangeCount,
		pointsCount,
		delta.Truncate(time.Second),
		float64(pointsCount)/delta.Seconds()/1000,
	)

	return nil
}

// aggregateShard aggregate one shard. It take the lock and run aggregation for the next period to aggregate.
func (c *CassandraTSDB) aggregateShard(
	ctx context.Context,
	shard int,
	lastNotifiedAggretedFrom *time.Time,
) (bool, time.Time) {
	name := shardStatePrefix + strconv.Itoa(shard)
	now := time.Now()
	maxTime := now.Truncate(aggregateSize)

	if c.options.ReadOnly {
		if maxTime.After(*lastNotifiedAggretedFrom) {
			c.logger.Debug().Str("shard-name", name).Msg("read-only is enabled, skip aggregation")

			*lastNotifiedAggretedFrom = maxTime
		}

		return false, time.Time{}
	}

	if c.options.DisablePreAggregation {
		if maxTime.After(*lastNotifiedAggretedFrom) {
			c.logger.Debug().Str("shard-name", name).Msg("pre-aggregation is disabled, skipping it")

			*lastNotifiedAggretedFrom = maxTime
		}

		return false, time.Time{}
	}

	lock := c.lockFactory.CreateLock(name, lockTimeToLive)
	if acquired := lock.TryLock(ctx, 0); !acquired {
		return false, time.Time{}
	}
	defer lock.Unlock()

	var fromTime time.Time

	{
		var fromTimeStr string

		retry.Print(func() error {
			_, err := c.state.Read(ctx, name, &fromTimeStr)

			return err //nolint:wrapcheck
		}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
			c.logger,
			"get state for shard "+name,
		)

		if ctx.Err() != nil {
			return false, time.Time{}
		}

		if fromTimeStr != "" {
			fromTime, _ = time.Parse(time.RFC3339, fromTimeStr)
		}
	}

	if fromTime.IsZero() {
		fromTime = maxTime

		retry.Print(func() error {
			return c.state.Write(ctx, name, fromTime.Format(time.RFC3339))
		}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
			c.logger,
			"update state for shard "+name,
		)
	}

	if ctx.Err() != nil {
		return false, time.Time{}
	}

	toTime := fromTime.Add(aggregateSize)
	isSafeMargin := toTime.Before(now.Add(-MaxPastDelay))

	if toTime.After(maxTime) || !isSafeMargin {
		return false, fromTime
	}

	if fromTime.After(*lastNotifiedAggretedFrom) {
		c.logger.Info().Msgf("Start aggregating from %s to %s", fromTime, toTime)
		*lastNotifiedAggretedFrom = fromTime
	}

	var ids []types.MetricID

	retry.Print(func() error {
		var err error
		ids, err = c.index.AllIDs(ctx, fromTime, toTime)

		return err //nolint:wrapcheck
	}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
		c.logger,
		"get IDs from the index",
	)

	if ctx.Err() != nil {
		return false, time.Time{}
	}

	var shardIDs []types.MetricID

	for _, id := range ids {
		idShard := id % shardNumber

		if idShard == types.MetricID(shard) {
			shardIDs = append(shardIDs, id)
		}
	}

	start := time.Now()

	if count, err := c.doAggregation(
		ctx, shardIDs, fromTime.UnixNano()/1000000, toTime.UnixNano()/1000000, aggregateResolution.Milliseconds(),
	); err == nil {
		c.logger.Debug().Msgf("Aggregated shard %d from [%v] to [%v] and read %d points in %v",
			shard, fromTime, toTime, count, time.Since(start))

		retry.Print(func() error {
			return c.state.Write(ctx, name, toTime.Format(time.RFC3339))
		}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
			c.logger,
			"update state for shard "+name,
		)
	} else {
		c.logger.Err(err).Msgf("Can't aggregate shard %d from [%v] to [%v]", shard, fromTime, toTime)
	}

	return true, toTime
}

// doAggregation perform the aggregation for given parameter.
func (c *CassandraTSDB) doAggregation(ctx context.Context,
	ids []types.MetricID,
	fromTimestamp,
	toTimestamp,
	resolution int64,
) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	pointsRead := 0

	var (
		metric types.MetricData
		err    error
	)

	tmp := c.getPointsBuffer()

	defer func() {
		c.putPointsBuffer(tmp)
	}()

	for _, id := range ids {
		metric.ID = id
		metric.TimeToLive = 0
		metric.Points = metric.Points[:0]

		metric, tmp, err = c.readRawData(ctx, id, metric, fromTimestamp, toTimestamp, tmp, "")
		if err != nil {
			return pointsRead, err
		}

		// Sort points in ascending order.
		reversePoints(metric.Points)

		aggregatedMetric := aggregate.Aggregate(metric, resolution)

		pointsRead += len(metric.Points)

		if len(aggregatedMetric.Points) == 0 {
			continue
		}

		err := c.writeAggregateData(ctx, aggregatedMetric, 0)
		if err != nil {
			return pointsRead, err
		}
	}

	return pointsRead, nil
}

func (c *CassandraTSDB) findMatchingMetrics(
	ctx context.Context,
	from time.Time, to time.Time,
	matchers []*labels.Matcher,
) ([]types.MetricID, error) {
	metricSet, err := c.index.Search(ctx, from, to, matchers)
	if err != nil {
		return nil, err
	}

	ids := make([]types.MetricID, 0, metricSet.Count())

	for metricSet.Next() {
		ids = append(ids, metricSet.At().ID)
	}

	return ids, metricSet.Err()
}

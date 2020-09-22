package tsdb

import (
	"context"
	"fmt"
	"math/rand"
	"squirreldb/aggregate"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"strconv"
	"time"
)

const (
	shardStatePrefix = "aggregate_until_shard_"
	shardNumber      = 60
)

const (
	lockTimeToLive = 10 * time.Minute
)

// Processed with aggregation for data older than backlogMargin seconds. If data older than this delay are received,
// they won't be aggregated.
const backlogMargin = time.Hour

// Run starts all CassandraTSDB services.
func (c *CassandraTSDB) Run(ctx context.Context) {
	shard := rand.Intn(shardNumber)
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
		workDone, tmp := c.aggregateShard(shard, &lastNotifiedAggretedFrom)

		if workDone {
			aggregationSeconds.Observe(time.Since(start).Seconds())
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

			aggregatdUntilSeconds.Set(float64(minTime.Unix()))

			if minTime.After(lastNotifiedAggretedUntil) {
				logger.Printf("All shard are aggregated until %s", minTime)
				lastNotifiedAggretedUntil = minTime

				c.l.Lock()
				c.fullyAggregatedAt = minTime
				c.l.Unlock()
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			debug.Print(2, logger, "Cassandra TSDB service stopped")
			return
		}
	}
}

// ForcePreAggregation will pre-aggregate all metrics between two given time
// The forced pre-aggregation will run even if data are already pre-aggregated and
// will no change the stored position where pre-aggregation think it stopped.
// The from & to will be extended to be aligned with the aggregation size.
//
// Forcing pre-aggregation should only serve when:
// 1) a bug caused the normal pre-aggregation to have anormal result
// 2) data were inserted with too many delay and normal pre-aggregation already
//    processed the time range.
func (c *CassandraTSDB) ForcePreAggregation(from time.Time, to time.Time) error {
	var ids []types.MetricID

	logger.Printf("Forced pre-aggregation requested between %v and %v", from, to)

	retry.Print(func() error {
		var err error
		ids, err = c.index.AllIDs()

		return err
	}, retry.NewExponentialBackOff(context.Background(), retryMaxDelay), logger,
		"get IDs from the index",
	)

	currentFrom := from
	currentFrom = currentFrom.Truncate(c.options.AggregateSize)

	for !to.Before(currentFrom) {
		currentTo := currentFrom.Add(c.options.AggregateSize)

		retry.Print(func() error {
			return c.doAggregation(ids, currentFrom.UnixNano()/1000000, currentTo.UnixNano()/1000000, c.options.AggregateResolution.Milliseconds())
		}, retry.NewExponentialBackOff(context.Background(), retryMaxDelay), logger,
			fmt.Sprintf("forced pre-aggregation from %v to %v", currentFrom, currentTo),
		)
		logger.Printf("Forced pre-aggregation from %v to %v completed", currentFrom, currentTo)

		currentFrom = currentFrom.Add(c.options.AggregateSize)
	}

	return nil
}

// aggregateShard aggregate one shard. It take the lock and run aggregation for the next period to aggregate.
func (c *CassandraTSDB) aggregateShard(shard int, lastNotifiedAggretedFrom *time.Time) (bool, time.Time) {
	name := shardStatePrefix + strconv.Itoa(shard)

	lock := c.lockFactory.CreateLock(name, lockTimeToLive)
	if acquired := lock.TryLock(context.Background(), 0); !acquired {
		return false, time.Time{}
	}
	defer lock.Unlock()

	var fromTime time.Time

	{
		var fromTimeStr string
		retry.Print(func() error {
			_, err := c.state.Read(name, &fromTimeStr)
			return err
		}, retry.NewExponentialBackOff(context.Background(), retryMaxDelay), logger,
			"get state for shard "+name,
		)

		if fromTimeStr != "" {
			fromTime, _ = time.Parse(time.RFC3339, fromTimeStr)
		}
	}

	now := time.Now()
	maxTime := now.Truncate(c.options.AggregateSize)

	if fromTime.IsZero() {
		fromTime = maxTime

		retry.Print(func() error {
			return c.state.Write(name, fromTime.Format(time.RFC3339))
		}, retry.NewExponentialBackOff(context.Background(), retryMaxDelay), logger,
			"update state for shard "+name,
		)
	}

	toTime := fromTime.Add(c.options.AggregateSize)
	isSafeMargin := toTime.Before(now.Add(-backlogMargin))

	if toTime.After(maxTime) || !isSafeMargin {
		return false, fromTime
	}

	if fromTime.After(*lastNotifiedAggretedFrom) {
		logger.Printf("Start aggregating from %s to %s", fromTime, toTime)
		*lastNotifiedAggretedFrom = fromTime
	}

	var ids []types.MetricID

	retry.Print(func() error {
		var err error
		ids, err = c.index.AllIDs()

		return err
	}, retry.NewExponentialBackOff(context.Background(), retryMaxDelay), logger,
		"get IDs from the index",
	)

	var shardIDs []types.MetricID

	for _, id := range ids {
		idShard := id % shardNumber

		if idShard == types.MetricID(shard) {
			shardIDs = append(shardIDs, id)
		}
	}

	start := time.Now()

	if err := c.doAggregation(shardIDs, fromTime.UnixNano()/1000000, toTime.UnixNano()/1000000, c.options.AggregateResolution.Milliseconds()); err == nil {
		debug.Print(debug.Level1, logger, "Aggregated shard %d from [%v] to [%v] in %v",
			shard, fromTime, toTime, time.Since(start))

		retry.Print(func() error {
			return c.state.Write(name, toTime.Format(time.RFC3339))
		}, retry.NewExponentialBackOff(context.Background(), retryMaxDelay), logger,
			"update state for shard "+name,
		)
	} else {
		logger.Printf("Error: Can't aggregate shard %d from [%v] to [%v] (%v)",
			shard, fromTime, toTime, err)
	}

	return true, toTime
}

// doAggregation perform the aggregation for given parameter.
func (c *CassandraTSDB) doAggregation(ids []types.MetricID, fromTimestamp, toTimestamp, resolution int64) error {
	if len(ids) == 0 {
		return nil
	}

	request := types.MetricRequest{
		IDs:           ids,
		FromTimestamp: fromTimestamp,
		ToTimestamp:   toTimestamp,
	}
	metrics, err := c.ReadIter(context.Background(), request)

	if err != nil {
		return err
	}

	for metrics.Next() {
		metric := metrics.At()
		aggregatedMetric := aggregate.Aggregate(metric, resolution)

		err := c.writeAggregateData(aggregatedMetric)
		if err != nil {
			return err
		}
	}

	return metrics.Err()
}

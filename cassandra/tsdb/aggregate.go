package tsdb

import (
	"context"
	"math/rand"
	"squirreldb/aggregate"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"strconv"
	"time"

	gouuid "github.com/gofrs/uuid"
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

// Run starts all CassandraTSDB services
func (c *CassandraTSDB) Run(ctx context.Context) {
	shard := rand.Intn(shardNumber)
	aggregateShardIntended := c.options.AggregateIntendedDuration / time.Duration(shardNumber)
	ticker := time.NewTicker(aggregateShardIntended)
	consecutiveNothingDone := 0 // number of time aggregateShard did nothing in a row

	defer ticker.Stop()

	for ctx.Err() == nil {
		workDone := c.aggregateShard(shard)

		shard = (shard + 1) % shardNumber

		if !workDone {
			consecutiveNothingDone++
		} else {
			consecutiveNothingDone = 0
		}

		if !workDone && consecutiveNothingDone < shardNumber {
			continue
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			debug.Print(2, logger, "Cassandra TSDB service stopped")
			return
		}
	}
}

// aggregateShard aggregate one shard. It take the lock and run aggregation for the next period to aggregate.
func (c *CassandraTSDB) aggregateShard(shard int) bool {
	name := shardStatePrefix + strconv.Itoa(shard)

	lock := c.lockFactory.CreateLock(name, lockTimeToLive)
	if acquired := lock.TryLock(); !acquired {
		return false
	}
	defer lock.Unlock()

	var fromTime time.Time

	{
		var fromTimeStr string
		retry.Print(func() error {
			_, err := c.state.Read(name, &fromTimeStr)
			return err
		}, retry.NewExponentialBackOff(retryMaxDelay), logger,
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
		}, retry.NewExponentialBackOff(retryMaxDelay), logger,
			"update state for shard "+name,
		)
	}

	toTime := fromTime.Add(c.options.AggregateSize)
	isSafeMargin := toTime.Before(now.Add(-backlogMargin))

	if toTime.After(maxTime) || !isSafeMargin {
		return false
	}

	var uuids []gouuid.UUID

	retry.Print(func() error {
		var err error
		uuids, err = c.index.AllUUIDs()

		return err
	}, retry.NewExponentialBackOff(retryMaxDelay), logger,
		"get UUIDs from the index",
	)

	var shardUUIDs []gouuid.UUID

	for _, uuid := range uuids {
		uuidShard := (int(types.UintFromUUID(uuid) % uint64(shardNumber)))

		if uuidShard == shard {
			shardUUIDs = append(shardUUIDs, uuid)
		}
	}

	if err := c.doAggregation(shardUUIDs, fromTime.UnixNano()/1000000, toTime.UnixNano()/1000000, c.options.AggregateResolution.Milliseconds()); err == nil {
		logger.Printf("Aggregate shard %d from [%v] to [%v]",
			shard, fromTime, toTime)

		retry.Print(func() error {
			return c.state.Write(name, toTime.Format(time.RFC3339))
		}, retry.NewExponentialBackOff(retryMaxDelay), logger,
			"update state for shard "+name,
		)
	} else {
		logger.Printf("Error: Can't aggregate shard %d from [%v] to [%v] (%v)",
			shard, fromTime, toTime, err)
	}

	return true
}

// doAggregation perform the aggregation for given parameter
func (c *CassandraTSDB) doAggregation(uuids []gouuid.UUID, fromTimestamp, toTimestamp, resolution int64) error {
	if len(uuids) == 0 {
		return nil
	}

	request := types.MetricRequest{
		UUIDs:         uuids,
		FromTimestamp: fromTimestamp,
		ToTimestamp:   toTimestamp,
	}
	metrics, err := c.Read(request)

	if err != nil {
		return err
	}

	aggregatedMetrics := aggregate.Aggregate(metrics, resolution)

	err = c.writeAggregate(aggregatedMetrics)

	return err
}

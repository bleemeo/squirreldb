package tsdb

import (
	"context"
	"math/rand"
	"squirreldb/aggregate"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"strconv"
	"sync"
	"time"
)

const (
	shardStatePrefix = "shard_"
	shardNumber      = 60
)

const (
	lockTimeToLive     = 600
	lockUpdateInterval = 300
)

// Processed with aggregation for data older than backlogMargin seconds. If data older than this delay are received,
// they won't be aggregated.
const backlogMargin = 3600

// Run starts all CassandraTSDB services
func (c *CassandraTSDB) Run(ctx context.Context) {
	shard := rand.Intn(shardNumber)
	aggregateShardIntended := float64(c.options.AggregateIntendedDuration) / float64(shardNumber)
	interval := (time.Duration(aggregateShardIntended)) * time.Second
	ticker := time.NewTicker(interval)
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

	if applied := c.writeAggregateLock(name); !applied {
		return false
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		c.updateAggregateLock(ctx, name)
	}()

	var fromTimestamp int64

	retry.Print(func() error {
		_, err := c.state.Read(name, &fromTimestamp)
		return err
	}, retry.NewExponentialBackOff(retryMaxDelay), logger,
		"get state for shard "+name,
	)

	if fromTimestamp == 0 {
		now := time.Now()
		fromTimestamp = now.Unix() - (now.Unix() % c.options.AggregateSize)

		retry.Print(func() error {
			return c.state.Update(name, fromTimestamp)
		}, retry.NewExponentialBackOff(retryMaxDelay), logger,
			"update state for shard "+name,
		)
	}

	now := time.Now()
	maxTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize)
	toTimestamp := fromTimestamp + c.options.AggregateSize
	isSafeMargin := (now.Unix() % 86400) >= backlogMargin

	if (toTimestamp > maxTimestamp) || !isSafeMargin {
		cancel()
		wg.Wait()
		c.deleteAggregateLock(name)

		return false
	}

	var uuids []types.MetricUUID

	retry.Print(func() error {
		var err error
		uuids, err = c.index.AllUUIDs()

		return err
	}, retry.NewExponentialBackOff(retryMaxDelay), logger,
		"get UUIDs from the index",
	)

	var shardUUIDs []types.MetricUUID

	for _, uuid := range uuids {
		uuidShard := (int(uuid.Uint64() % uint64(shardNumber)))

		if uuidShard == shard {
			shardUUIDs = append(shardUUIDs, uuid)
		}
	}

	if err := c.doAggregation(shardUUIDs, fromTimestamp, toTimestamp, c.options.AggregateResolution); err == nil {
		logger.Printf("Aggregate shard %d from [%v] to [%v]",
			shard, time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0))

		retry.Print(func() error {
			return c.state.Update(name, toTimestamp)
		}, retry.NewExponentialBackOff(retryMaxDelay), logger,
			"update state for shard "+name,
		)
	} else {
		logger.Printf("Error: Can't aggregate shard %d from [%v] to [%v] (%v)",
			shard, time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0), err)
	}

	cancel()
	wg.Wait()
	c.deleteAggregateLock(name)

	return true
}

// doAggregation perform the aggregation for given parameter
func (c *CassandraTSDB) doAggregation(uuids []types.MetricUUID, fromTimestamp, toTimestamp, resolution int64) error {
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

// Deletes the specified lock
func (c *CassandraTSDB) deleteAggregateLock(name string) {
	retry.Print(func() error {
		return c.locker.Delete(name)
	}, retry.NewExponentialBackOff(retryMaxDelay), logger,
		"delete lock for shard "+name,
	)
}

// Returns a boolean if the specified lock was written or not
func (c *CassandraTSDB) writeAggregateLock(name string) bool {
	var applied bool

	retry.Print(func() error {
		var err error
		applied, err = c.locker.Write(name, lockTimeToLive)

		return err
	}, retry.NewExponentialBackOff(retryMaxDelay), logger,
		"acquire lock for shard "+name,
	)

	return applied
}

// Updates the specified lock until a signal is received
func (c *CassandraTSDB) updateAggregateLock(ctx context.Context, name string) {
	interval := lockUpdateInterval * time.Second
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			retry.Print(func() error {
				return c.locker.Update(name, lockTimeToLive)
			}, retry.NewExponentialBackOff(retryMaxDelay), logger,
				"refresh lock for shard "+name,
			)
		case <-ctx.Done():
			return
		}
	}
}
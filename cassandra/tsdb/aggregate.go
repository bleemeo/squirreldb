package tsdb

import (
	"context"
	"squirreldb/aggregate"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
)

const (
	shards       = 60.
	shardsPeriod = 60.
)

const (
	fromTimestampStateName = "aggregate_from_timestamp"
	lastShardStateName     = "aggregate_last_shard"
)

const safeMargin = 3600

// Run starts all CassandraTSDB services
func (c *CassandraTSDB) Run(ctx context.Context) {
	c.runAggregator(ctx)
}

// Starts the aggregator service
// If a stop signal is received, the service is stopped
func (c *CassandraTSDB) runAggregator(ctx context.Context) {
	c.aggregateInit()

	delay := (shardsPeriod / shards) * time.Second
	ticker := time.NewTicker(delay)

	for {
		c.aggregate(c.index.UUIDs(nil, true))

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("Aggregator service stopped")
			return
		}
	}
}

// Initializes the aggregate states
func (c *CassandraTSDB) aggregateInit() {
	now := time.Now()
	fromTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize)

	if c.debugOptions.AggregateForce {
		fromTimestamp -= c.debugOptions.AggregateSize
	}

	retry.Print(func() error {
		return c.states.Write(fromTimestampStateName, fromTimestamp)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't write "+fromTimestampStateName+" state",
		"Resolved: Write "+fromTimestampStateName+" state")

	lastShard := 0

	retry.Print(func() error {
		return c.states.Write(lastShardStateName, lastShard)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't write "+lastShardStateName+" state",
		"Resolved: Write "+lastShardStateName+" state")
}

// Aggregates all metrics contained in the index by aggregation batch size and by shard
func (c *CassandraTSDB) aggregate(uuids []types.MetricUUID) {
	if len(uuids) == 0 {
		return
	}

	var fromTimestamp int64

	retry.Print(func() error {
		return c.states.Read(fromTimestampStateName, &fromTimestamp)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't read "+fromTimestampStateName+" state",
		"Resolved: Read "+fromTimestampStateName+" state")

	now := time.Now()
	maxTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize)
	toTimestamp := fromTimestamp + c.options.AggregateSize
	isSafeMargin := (now.Unix() % 86400) >= safeMargin // Authorizes aggregation if it is more than 1 a.m.

	if (toTimestamp > maxTimestamp) || !isSafeMargin {
		return
	}

	shard, err := c.aggregateSize(uuids, fromTimestamp, toTimestamp, c.options.AggregateResolution)

	if err != nil {
		logger.Printf("Error: Can't aggregate from [%v] to [%v] (%v)",
			time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0), err)
	}

	if shard == shards {
		logger.Printf("Successfully aggregate from [%v] to [%v]",
			time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0))

		retry.Print(func() error {
			return c.states.Update(fromTimestampStateName, toTimestamp)
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"Error: Can't update "+fromTimestampStateName+" state",
			"Resolved: Update "+fromTimestampStateName+" state")
	}
}

// Aggregates all metrics by aggregation batch size
func (c *CassandraTSDB) aggregateSize(uuids []types.MetricUUID, fromTimestamp, toTimestamp, resolution int64) (int, error) {
	var lastShard int

	retry.Print(func() error {
		return c.states.Read(lastShardStateName, &lastShard)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't read "+lastShardStateName+" state",
		"Resolved: Read "+lastShardStateName+" state")

	shard := lastShard + 1
	err := c.aggregateShard(shard, uuids, fromTimestamp, toTimestamp, resolution)

	if err != nil {
		return shard, err
	}

	retry.Print(func() error {
		return c.states.Update(lastShardStateName, shard%60)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't update "+lastShardStateName+" state",
		"Resolved: Update "+lastShardStateName+" state")

	return shard, nil
}

// Aggregates all metrics associated with the shard
func (c *CassandraTSDB) aggregateShard(shard int, uuids []types.MetricUUID, fromTimestamp, toTimestamp, resolution int64) error {
	var uuidsShard []types.MetricUUID

	for _, uuid := range uuids {
		shardUUID := int(uuid.Uint64() % (uint64(shards) + 1))

		if shardUUID == shard {
			uuidsShard = append(uuidsShard, uuid)
		}
	}

	err := c.readAggregateWrite(uuidsShard, fromTimestamp, toTimestamp, resolution)

	return err
}

// Reads all metrics contained between timestamps, generates aggregate data and writes it
func (c *CassandraTSDB) readAggregateWrite(uuids []types.MetricUUID, fromTimestamp, toTimestamp, resolution int64) error {
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

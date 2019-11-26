package tsdb

import (
	"context"
	"fmt"
	"math/rand"
	"squirreldb/aggregate"
	"squirreldb/retry"
	"squirreldb/types"
	"strconv"
	"time"
)

const (
	shards       = 60.
	shardsPeriod = 60.
)

const (
	shardStatePrefix = "shard_"
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
	shard := rand.Intn(shards) + 1

	for {
		c.aggregateShard(shard)

		shard = (shard % shards) + 1

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("Aggregator service stopped")
			return
		}
	}
}

// Initializes the aggregate shard states
func (c *CassandraTSDB) aggregateInit() {
	now := time.Now()
	fromTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize)

	if c.debugOptions.AggregateForce {
		fromTimestamp -= c.debugOptions.AggregateSize
	}

	for i := 1; i <= shards; i++ {
		name := shardStatePrefix + strconv.Itoa(i)

		if c.debugOptions.AggregateForce {
			retry.Print(func() error {
				return c.states.Update(name, fromTimestamp)
			}, retry.NewExponentialBackOff(30*time.Second), logger,
				"Error: Can't update "+name+" state",
				"Resolved: Update "+name+" state")
		} else {
			retry.Print(func() error {
				return c.states.Write(name, fromTimestamp)
			}, retry.NewExponentialBackOff(30*time.Second), logger,
				"Error: Can't write "+name+" state",
				"Resolved: Write "+name+" state")
		}
	}
}

func (c *CassandraTSDB) aggregateShard(shard int) {
	name := shardStatePrefix + strconv.Itoa(shard)

	var fromTimestamp int64

	retry.Print(func() error {
		return c.states.Read(name, &fromTimestamp)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't read "+name+" state",
		"Resolved: Read "+name+" state")

	now := time.Now()
	maxTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize)
	toTimestamp := fromTimestamp + c.options.AggregateSize
	isSafeMargin := (now.Unix() % 86400) >= safeMargin // Authorizes aggregation if it is more than 1 a.m.

	if (toTimestamp > maxTimestamp) || !isSafeMargin {
		return
	}

	if err := c.aggregateShardSizeLock(shard, fromTimestamp, toTimestamp, c.options.AggregateResolution); err == nil {
		logger.Printf("Aggregate shard %d from [%v] to [%v]",
			shard, time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0))

		retry.Print(func() error {
			return c.states.Update(name, toTimestamp)
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"Error: Can't update "+name+" state",
			"Resolved: update "+name+" state")
	} else {
		logger.Printf("Error: Can't aggregate shard %d from [%v] to [%v] (%v)",
			shard, time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0), err)
	}
}

func (c *CassandraTSDB) aggregateShardSizeLock(shard int, fromTimestamp, toTimestamp, resolution int64) error {
	name := shardStatePrefix + strconv.Itoa(shard)

	var applied bool

	retry.Print(func() error {
		var err error
		applied, err = c.locks.Write(name, 30*time.Second)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't write "+name+" lock",
		"Resolved: Write "+name+" lock")

	if !applied {
		return fmt.Errorf("not the leader on the shard")
	}

	err := c.aggregateShardSize(shards, fromTimestamp, toTimestamp, resolution)

	retry.Print(func() error {
		return c.locks.Delete(name)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't delete "+name+" lock",
		"Resolved: Delete "+name+" lock")

	return err
}

func (c *CassandraTSDB) aggregateShardSize(shard int, fromTimestamp, toTimestamp, resolution int64) error {
	var shardUUIDs []types.MetricUUID

	for _, uuid := range c.index.UUIDs(nil, true) {
		uuidShard := (int(uuid.Uint64() % uint64(shards))) + 1

		if uuidShard == shard {
			shardUUIDs = append(shardUUIDs, uuid)
		}
	}

	err := c.readAggregateWrite(shardUUIDs, fromTimestamp, toTimestamp, resolution)

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

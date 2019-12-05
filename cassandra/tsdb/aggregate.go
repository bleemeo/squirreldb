package tsdb

import (
	"context"
	"math/rand"
	"squirreldb/aggregate"
	"squirreldb/retry"
	"squirreldb/types"
	"strconv"
	"sync"
	"time"
)

const (
	shardStatePrefix = "shard_"
	shards           = 60
)

const (
	lockTimeToLive     = 600
	lockUpdateInterval = 300
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

	shard := rand.Intn(shards) + 1
	aggregateShardIntended := float64(c.options.AggregateIntendedDuration) / float64(shards)
	interval := (time.Duration(aggregateShardIntended)) * time.Second
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		c.aggregate(shard)

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

	for i := 1; i <= shards; i++ {
		name := shardStatePrefix + strconv.Itoa(i)
		retry.Print(func() error {
			return c.stater.Write(name, fromTimestamp)
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"Error: Can't write "+name+" state",
			"Resolved: Write "+name+" state")
	}
}

// Aggregates metrics belonging to the shard
func (c *CassandraTSDB) aggregate(shard int) {
	name := shardStatePrefix + strconv.Itoa(shard)

	if applied := c.writeAggregateLock(name); !applied {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	runAggregateLockUpdate := func() {
		defer wg.Done()
		c.updateAggregateLock(ctx, name)
	}

	wg.Add(1)

	go runAggregateLockUpdate()

	var fromTimestamp int64

	retry.Print(func() error {
		return c.stater.Read(name, &fromTimestamp)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't read "+name+" state",
		"Resolved: Read "+name+" state")

	now := time.Now()
	maxTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize)
	toTimestamp := fromTimestamp + c.options.AggregateSize
	isSafeMargin := (now.Unix() % 86400) >= safeMargin // Authorizes aggregation if it is more than 1 a.m.

	if (toTimestamp > maxTimestamp) || !isSafeMargin {
		cancel()
		c.deleteAggregateLock(name)

		return
	}

	if err := c.aggregateSize(shard, fromTimestamp, toTimestamp, c.options.AggregateResolution); err == nil {
		logger.Printf("Aggregate shard %d from [%v] to [%v]",
			shard, time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0))

		retry.Print(func() error {
			return c.stater.Update(name, toTimestamp)
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"Error: Can't update "+name+" state",
			"Resolved: update "+name+" state")
	} else {
		logger.Printf("Error: Can't aggregate shard %d from [%v] to [%v] (%v)",
			shard, time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0), err)
	}

	cancel()
	c.deleteAggregateLock(name)
}

// Aggregates metrics belonging to the shard by aggregation batch size
func (c *CassandraTSDB) aggregateSize(shard int, fromTimestamp, toTimestamp, resolution int64) error {
	uuids := c.indexer.UUIDs(nil, true)

	var shardUUIDs []types.MetricUUID

	for _, uuid := range uuids {
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

// Deletes the specified lock
func (c *CassandraTSDB) deleteAggregateLock(name string) {
	retry.Print(func() error {
		return c.locker.Delete(name)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't delete "+name+" lock",
		"Resolved: Delete "+name+" lock")
}

// Returns a boolean if the specified lock was written or not
func (c *CassandraTSDB) writeAggregateLock(name string) bool {
	var applied bool

	retry.Print(func() error {
		var err error
		applied, err = c.locker.Write(name, lockTimeToLive)

		return err
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't write "+name+" lock",
		"Resolved: Write "+name+" lock")

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
			}, retry.NewExponentialBackOff(30*time.Second), logger,
				"Error: Can't update "+name+" lock",
				"Resolved: Update "+name+" lock")
		case <-ctx.Done():
			return
		}
	}
}

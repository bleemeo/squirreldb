package tsdb

import (
	"context"
	"github.com/gocql/gocql"
	"squirreldb/aggregate"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
)

const (
	AggregateShards       = 60
	AggregateShardsPeriod = 60
)

// Run calls aggregateShard() every period if the conditions are met
// The process starts from the last saved states (from timestamp and last shard)
// If a stop signal is received, the service is stopped
func (c *CassandraTSDB) Run(ctx context.Context) {
	if c.debug.AggregateForce {
		now := time.Now()
		fromTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize) - c.debug.AggregateSize

		_ = c.states.Write("aggregate_from_timestamp", fromTimestamp)
	}

	ticker := time.NewTicker(time.Duration(float64(AggregateShardsPeriod)/float64(AggregateShards)) * time.Second)
	started := false

	for {
		uuids := c.index.UUIDs()
		var fromTimestamp int64

		retry.Print(func() error {
			err := c.states.Read("aggregate_from_timestamp", &fromTimestamp)

			if err == gocql.ErrNotFound {
				return nil
			}

			return err
		}, retry.NewBackOff(30*time.Second), logger,
			"Error: Can't read 'aggregate_from_timestamp' state",
			"Resolved: Read 'aggregate_from_timestamp' state")

		now := time.Now()
		limitFromTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize) - c.options.AggregateSize

		if fromTimestamp == 0 {
			fromTimestamp = limitFromTimestamp
		}

		if (len(uuids) > 0) && (fromTimestamp <= limitFromTimestamp) {
			var lastShard int

			retry.Print(func() error {
				err := c.states.Read("aggregate_last_shard", &lastShard)

				if err == gocql.ErrNotFound {
					return nil
				}

				return err
			}, retry.NewBackOff(30*time.Second), logger,
				"Error: Can't read 'aggregate_last_shard' state",
				"Resolved: Read 'aggregate_last_shard' state")

			toTimestamp := fromTimestamp + c.options.AggregateSize
			shard := lastShard + 1

			if !started {
				logger.Printf("Aggregate from %v to %v",
					time.Unix(fromTimestamp, 0), time.Unix(toTimestamp, 0))

				if shard != 1 {
					logger.Printf("Aggregate from shard %d on %d", shard, AggregateShards)
				}

				started = true
			}

			err := c.aggregateShard(shard, uuids, fromTimestamp, toTimestamp)

			if err != nil {
				logger.Printf("Error: Can't aggregate shard (%v)", err)
			} else {
				retry.Print(func() error {
					return c.states.Write("aggregate_last_shard", shard%60)
				}, retry.NewBackOff(30*time.Second), logger,
					"Error: Can't write 'aggregate_last_shard' state",
					"Resolved: Write 'aggregate_last_shard' state")

				debug.Print(debug.Level1, logger, "Aggregate shard %d on %d", shard, AggregateShards)
			}

			if (err == nil) && (shard == AggregateShards) {
				retry.Print(func() error {
					return c.states.Write("aggregate_from_timestamp", toTimestamp)
				}, retry.NewBackOff(30*time.Second), logger,
					"Error: Can't write 'aggregate_from_timestamp' state",
					"Resolved: Write 'aggregate_from_timestamp' state")

				logger.Println("Aggregate completed")
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// Aggregate each metric corresponding to the shard
func (c *CassandraTSDB) aggregateShard(shard int, uuids types.MetricUUIDs, fromTimestamp, toTimestamp int64) error {
	var uuidsShard types.MetricUUIDs

	for _, uuid := range uuids {
		if (uuid.Uint64() % (uint64(AggregateShards) + 1)) == uint64(shard) {
			uuidsShard = append(uuidsShard, uuid)
		}
	}

	if err := c.aggregate(uuidsShard, fromTimestamp, toTimestamp); err != nil {
		return err
	}

	return nil
}

// Aggregate each specified metrics contained in the specified period
func (c *CassandraTSDB) aggregate(uuids types.MetricUUIDs, fromTimestamp, toTimestamp int64) error {
	startTime := time.Now()

	for _, uuid := range uuids {
		request := types.MetricRequest{
			UUIDs:         types.MetricUUIDs{uuid},
			FromTimestamp: fromTimestamp,
			ToTimestamp:   toTimestamp,
		}

		metrics, err := c.Read(request)

		aggregateProcessedPointsTotal.Add(float64(len(metrics[uuid].Points)))

		if err != nil {
			return err
		}

		for timestamp := fromTimestamp; timestamp < toTimestamp; timestamp += c.options.AggregateSize {
			aggregatedMetrics := aggregate.Metrics(metrics, timestamp, timestamp+c.options.AggregateSize, c.options.AggregateResolution)

			if err := c.writeAggregated(aggregatedMetrics); err != nil {
				return err
			}
		}
	}

	duration := time.Since(startTime)
	aggregateSeconds.Add(duration.Seconds())

	debug.Print(debug.Level1, logger,
		"Aggregate details:"+"\n"+
			"|__ fromTimestamp: %v (%d)"+"\n"+
			"|__ toTimestamp: %v (%d)"+"\n"+
			"|__ Process %d metric(s) in %v (%f metric(s)/s)",
		time.Unix(fromTimestamp, 0), fromTimestamp,
		time.Unix(toTimestamp, 0), toTimestamp,
		len(uuids), duration, float64(len(uuids))/duration.Seconds(),
	)

	return nil
}

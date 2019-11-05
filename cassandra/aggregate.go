package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"
	"log"
	"os"
	"squirreldb/aggregate"
	"squirreldb/types"
	"strings"
	"time"
)

const (
	AggregatePeriod       = 300
	AggregateShards       = 60
	AggregateShardsPeriod = 60
)

func (c *Cassandra) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(AggregatePeriod) * time.Second)

	for {
		uuids := c.readUUIDs()
		var fromTimestamp int64

		_ = c.readState("last_aggregation_from_timestamp", &fromTimestamp)

		now := time.Now()
		limitFromTimestamp := now.Unix() - (now.Unix() % c.options.AggregateSize) - c.options.AggregateSize

		if fromTimestamp == 0 {
			fromTimestamp = limitFromTimestamp
		}

		toTimestamp := fromTimestamp + c.options.AggregateSize

		if (len(uuids) == 0) || (fromTimestamp > limitFromTimestamp) {
			logger.Println("runAggregate: Nothing to aggregate")
		} else {
			logger.Println("runAggregate: Aggregate", len(uuids), "metric(s) from", fromTimestamp, "to", toTimestamp) // TODO: Debug

			complete := c.aggregateShards(ctx, AggregateShards, AggregateShardsPeriod, uuids, fromTimestamp, toTimestamp)

			if complete {
				logger.Println("runAggregate: Aggregate completed") // TODO: Debug

				fromTimestamp += c.options.AggregateSize
			} else {
				logger.Println("runAggregate: Aggregate not completed") // TODO: Debug
			}

			_ = c.writeState("last_aggregation_from_timestamp", fromTimestamp)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("runAggregate: Stopped")
			return
		}
	}
}

func (c *Cassandra) aggregateShards(ctx context.Context, shards, period int, uuids types.MetricUUIDs, fromTimestamp, toTimestamp int64) bool {
	var lastShard int

	_ = c.readState("last_aggregation_shard", &lastShard)

	ticker := time.NewTicker(time.Duration(float64(period)/float64(shards)) * time.Second)

	for currentShard := lastShard + 1; currentShard <= shards; currentShard++ {
		var uuidsShard types.MetricUUIDs

		for _, uuid := range uuids {
			if (uuid.Uint64() % uint64(shards)) == uint64(currentShard) {
				uuidsShard = append(uuidsShard, uuid)
			}
		}

		if err := c.aggregate(uuidsShard, fromTimestamp, toTimestamp); err != nil {
			logger.Println("aggregateShards: Can't aggregate (", err, ")")

			_ = c.writeState("last_aggregation_shard", currentShard-1)

			return false
		}

		logger.Println("aggregateShards: Aggregate", len(uuidsShard), "metric(s) (", currentShard, "/", shards, ")") // TODO: Debug

		select {
		case <-ticker.C:
		case <-ctx.Done():
			ctx.Done()
			_ = c.writeState("last_aggregation_shard", currentShard)

			return false
		}
	}

	_ = c.writeState("last_aggregation_shard", 0)

	return true
}

func (c *Cassandra) aggregate(uuids types.MetricUUIDs, fromTimestamp, toTimestamp int64) error {
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

	debugLog := log.New(os.Stdout, "[debug] ", log.LstdFlags) // TODO: Debug
	debugLog.Println("[cassandra] aggregate():")              // TODO: Debug
	debugLog.Printf("\t"+"|_ fromTimestamp: %d (%v), toTimestamp: %d (%v)"+"\n",
		fromTimestamp, time.Unix(fromTimestamp, 0), toTimestamp, time.Unix(toTimestamp, 0)) // TODO: Debug
	debugLog.Printf("\t"+"|_ Process %d metric(s) in %v (%f metric(s)/s)",
		len(uuids), duration, float64(len(uuids))/duration.Seconds()) // TODO: Debug

	return nil
}

func (c *Cassandra) readUUIDs() types.MetricUUIDs {
	var uuids types.MetricUUIDs

	iterator := c.readDatabaseUUIDs()
	var metricUUID string

	for iterator.Scan(&metricUUID) {
		uuidItem := types.MetricUUID{
			UUID: gouuid.FromStringOrNil(metricUUID),
		}

		uuids = append(uuids, uuidItem)
	}

	return uuids
}

// Returns an iterator of all metrics from the data table according to the parameters
func (c *Cassandra) readDatabaseUUIDs() *gocql.Iter {
	iteratorReplacer := strings.NewReplacer("$INDEX_TABLE", c.options.indexTable)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT metric_uuid FROM $INDEX_TABLE
		ALLOW FILTERING
	`)).Iter()

	return iterator
}

package cassandra

import (
	"context"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"github.com/gofrs/uuid"
	"squirreldb/aggregate"
	"squirreldb/compare"
	"squirreldb/retry"
	"squirreldb/types"
	"strings"
	"time"
)

func (c *Cassandra) Run(ctx context.Context) {
	nowUnix := time.Now().Unix()
	toTimestamp := nowUnix - (nowUnix % c.options.AggregateSize)
	fromTimestamp := toTimestamp - c.options.AggregateSize
	waitTimestamp := (nowUnix % c.options.AggregateSize) + c.options.AggregateStartOffset

	if c.options.DebugAggregateForce { // TODO: DEBUG
		fromTimestamp = toTimestamp - c.options.DebugAggregateSize
		waitTimestamp = 5
	}

	logger.Println("Run: Start in", waitTimestamp, "seconds...") // TODO: Debug

	select {
	case <-time.After(time.Duration(waitTimestamp) * time.Second):
	case <-ctx.Done():
		logger.Println("Run: Stopped")
		return
	}

	ticker := time.NewTicker(time.Duration(c.options.AggregateSize) * time.Second)
	defer ticker.Stop()

	for {
		_ = backoff.Retry(func() error {
			err := c.aggregate(fromTimestamp, toTimestamp)

			if err != nil {
				logger.Println("aggregate: Can't aggregate (", err, ")")
			}

			return err
		}, retry.NewBackOff(30*time.Second))

		toTimestamp += c.options.AggregateSize

		logger.Println("Run: Aggregate") // TODO: Debug

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("Run: Stopped")
			return
		}
	}
}

func (c *Cassandra) aggregate(fromTimestamp, toTimestamp int64) error {
	uuids := c.readUUIDs(fromTimestamp, toTimestamp)

	for _, mUUID := range uuids {
		request := types.MetricRequest{
			UUIDs:         []types.MetricUUID{mUUID},
			FromTimestamp: fromTimestamp,
			ToTimestamp:   toTimestamp,
		}

		metrics, err := c.Read(request)

		if err != nil {
			return err
		}

		metrics[mUUID] = metrics[mUUID].SortUnify()

		for timestamp := fromTimestamp; timestamp < toTimestamp; timestamp += c.options.AggregateSize {
			aggregatedMetrics := aggregate.Metrics(metrics, timestamp, timestamp+c.options.AggregateSize, c.options.AggregateResolution)

			if err := c.writeAggregated(aggregatedMetrics); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Cassandra) readUUIDs(fromTimestamp, toTimestamp int64) []types.MetricUUID {
	batchSize := c.options.BatchSize
	rawPartitionSize := c.options.RawPartitionSize
	fromBaseTimestamp := fromTimestamp - (fromTimestamp % rawPartitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % rawPartitionSize)

	uuidMap := make(map[types.MetricUUID]bool)

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += rawPartitionSize {
		fromOffsetTimestamp := compare.Int64Max(fromTimestamp-baseTimestamp-batchSize, 0)
		toOffsetTimestamp := compare.Int64Min(toTimestamp-baseTimestamp, rawPartitionSize)

		iterator := c.readDatabaseUUIDs(baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		var metricUUID string

		for iterator.Scan(&metricUUID) {
			uuidItem := types.MetricUUID{
				UUID: uuid.FromStringOrNil(metricUUID),
			}

			uuidMap[uuidItem] = true
		}
	}

	var uuids []types.MetricUUID

	for uuidItem := range uuidMap {
		uuids = append(uuids, uuidItem)
	}

	return uuids
}

// Returns an iterator of all metrics from the data table according to the parameters
func (c *Cassandra) readDatabaseUUIDs(baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iteratorReplacer := strings.NewReplacer("$DATA_TABLE", c.options.dataTable)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT metric_uuid FROM $DATA_TABLE
		WHERE base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
		ALLOW FILTERING
	`), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	return iterator
}

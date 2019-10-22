package cassandra

import (
	"context"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"github.com/gofrs/uuid"
	"log"
	"os"
	"squirreldb/aggregate"
	"squirreldb/compare"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"strings"
	"time"
)

func (c *Cassandra) Run(ctx context.Context) {
	nowUnix := time.Now().Unix()
	toTimestamp := nowUnix - (nowUnix % c.options.AggregateSize)
	fromTimestamp := toTimestamp - c.options.AggregateSize
	waitTimestamp := nowUnix - toTimestamp + c.options.AggregateStartOffset

	if c.options.DebugAggregateForce { // TODO: DEBUG
		toTimestamp = toTimestamp - c.options.AggregateStartOffset
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

		fromTimestamp = toTimestamp
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

	speedReadPoints := debug.NewSpeed()      // TODO: Speed
	speedAggregatePoints := debug.NewSpeed() // TODO: Speed
	speedWriteRows := debug.NewSpeed()       // TODO: Speed
	speedWritePoints := debug.NewSpeed()     // TODO: Speed

	for _, mUUID := range uuids {
		request := types.MetricRequest{
			UUIDs:         []types.MetricUUID{mUUID},
			FromTimestamp: fromTimestamp,
			ToTimestamp:   toTimestamp,
		}

		speedReadPoints.Start() // TODO: Speed

		metrics, err := c.Read(request)

		speedReadPoints.Stop(float64(len(metrics[mUUID]))) // TODO: Speed

		if err != nil {
			return err
		}

		for timestamp := fromTimestamp; timestamp < toTimestamp; timestamp += c.options.AggregateSize {
			speedAggregatePoints.Start() // TODO: Speed

			aggregatedMetrics := aggregate.Metrics(metrics, timestamp, timestamp+c.options.AggregateSize, c.options.AggregateResolution)

			speedAggregatePoints.Stop(0) // TODO: Speed

			speedWriteRows.Start()   // TODO: Speed
			speedWritePoints.Start() // TODO: Speed

			if err := c.writeAggregated(aggregatedMetrics); err != nil {
				return err
			}

			speedWriteRows.Stop(1)                                        // TODO: Speed
			speedWritePoints.Stop(float64(len(aggregatedMetrics[mUUID]))) // TODO: Speed
		}

		speedAggregatePoints.AddValue(float64(len(metrics[mUUID]))) // TODO: Speed
	}

	debugLog := log.New(os.Stdout, "[debug] ", log.LstdFlags)

	debugLog.Println("[cassandra] Aggregation debug:")
	debugLog.Printf("[cassandra] fromTimestamp: %d (%v), toTimestamp: %d (%v)"+"\n",
		fromTimestamp, time.Unix(fromTimestamp, 0), toTimestamp, time.Unix(toTimestamp, 0))
	speedReadPoints.Print("cassandra", "Read", "point")
	speedAggregatePoints.Print("cassandra", "Aggregate", "point")
	speedWritePoints.Print("cassandra", "Write", "point")
	speedWriteRows.Print("cassandra", "Write", "row")

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

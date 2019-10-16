package cassandra

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"squirreldb/aggregate"
	"squirreldb/config"
	"squirreldb/math"
	"squirreldb/retry"
	"squirreldb/types"
	"strings"
	"time"
)

// TODO: Comment
func (c *Cassandra) Run(ctx context.Context) {
	nowUnix := time.Now().Unix()
	aggregateSize := config.C.Int64("cassandra.aggregate.size")
	aggregateTimestamp := nowUnix - (nowUnix % aggregateSize)
	waitTimestamp := aggregateTimestamp - nowUnix

	if waitTimestamp < 0 {
		waitTimestamp += aggregateSize
	}

	startOffset := config.C.Int64("cassandra.aggregate.start_offset")

	waitTimestamp += startOffset

	logger.Println("Run: Start in", waitTimestamp, "seconds...") // TODO: Debug

	select {
	case <-time.After(time.Duration(waitTimestamp) * time.Second):
	case <-ctx.Done():
		logger.Println("Run: Stopped")
		return
	}

	aggregateTime := time.Unix(aggregateTimestamp, 0)
	aggregateStep := config.C.Int64("cassandra.aggregate.step")
	ticker := time.NewTicker(time.Duration(aggregateSize) * time.Second)
	defer ticker.Stop()

	for {
		c.aggregate(aggregateTime, aggregateStep, aggregateSize)
		aggregateTime = aggregateTime.Add(time.Duration(aggregateSize) * time.Second)

		logger.Println("Run: Aggregate") // TODO: Debug

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("Run: Stopped")
			return
		}
	}
}

func (c *Cassandra) aggregate(now time.Time, aggregateStep, aggregateSize int64) {
	nowUnix := now.Unix()
	toTimestamp := nowUnix - (nowUnix % aggregateSize)
	fromTimestamp := toTimestamp - aggregateSize

	rowSize := config.C.Int64("batch.size")
	partitionSize := config.C.Int64("cassandra.partition_size.raw")
	fromBaseTimestamp := fromTimestamp - (fromTimestamp % partitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % partitionSize)
	var uuids []types.MetricUUID

	perfGettingUUIDsTime := time.Now() // TODO: Performance

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += partitionSize {
		fromOffsetTimestamp := math.Int64Max(fromTimestamp-baseTimestamp-rowSize, 0)
		toOffsetTimestamp := math.Int64Min(toTimestamp-baseTimestamp, partitionSize)

		iterator := c.uuids(baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		var metricUUID string

		for iterator.Scan(&metricUUID) {
			cqlUUID, _ := gocql.ParseUUID(metricUUID)
			uuid := types.MetricUUID{UUID: [16]byte(cqlUUID)}

			uuids = append(uuids, uuid)
		}

		_ = backoff.Retry(func() error {
			err := iterator.Close()

			if err != nil {
				logger.Println("aggregate: Can't close iterator (", err, ")")
			}

			return err
		}, retry.NewBackOff(30*time.Second))
	}

	fmt.Println("Gettings all UUIDs in:", time.Now().Sub(perfGettingUUIDsTime)) // TODO: Performance

	perfAggregateAllMetricsTime := time.Now() // TODO: Performance

	for _, uuid := range uuids {
		perfAggregateMetricTime := time.Now() // TODO: Performance

		request := types.MetricRequest{
			UUIDs:         []types.MetricUUID{uuid},
			FromTimestamp: nowUnix - (nowUnix % aggregateSize) - aggregateSize,
			ToTimestamp:   nowUnix - (nowUnix % aggregateSize),
		}

		var metrics types.Metrics

		_ = backoff.Retry(func() error {
			var err error
			metrics, err = c.Read(request)

			if err != nil {
				logger.Println("read: Can't read metrics (", err, ")")
			}

			return err
		}, retry.NewBackOff(30*time.Second))

		aggregatedMetrics := aggregate.Metrics(metrics, request.FromTimestamp, request.ToTimestamp, aggregateStep)

		_ = backoff.Retry(func() error {
			err := c.writeAggregated(aggregatedMetrics)

			if err != nil {
				logger.Println("aggregate: Can't write aggregated metrics (", err, ")")
			}

			return err
		}, retry.NewBackOff(30*time.Second))

		fmt.Println("Read, aggregate and write one metric in:", time.Now().Sub(perfAggregateMetricTime)) // TODO: Performance
	}

	fmt.Println("Read, aggregate and write all metrics in:", time.Now().Sub(perfAggregateAllMetricsTime)) // TODO: Performance
}

// Returns an iterator of all metrics from the data table according to the parameters
func (c *Cassandra) uuids(baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iteratorReplacer := strings.NewReplacer("$DATA_TABLE", dataTable)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT metric_uuid FROM $DATA_TABLE
		WHERE base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
		ALLOW FILTERING
	`), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	return iterator
}

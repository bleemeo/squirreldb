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
func (c *Cassandra) RunAggregator(ctx context.Context) {
	nowUnix := time.Now().Unix()
	aggregateSize := config.C.Int64("cassandra.aggregate.size")
	startOffset := config.C.Int64("cassandra.aggregate.start_offset")
	startTimestamp := nowUnix - (nowUnix % aggregateSize) + startOffset
	waitTimestamp := startTimestamp - nowUnix

	if waitTimestamp < 0 {
		waitTimestamp += aggregateSize
	}

	logger.Println("RunAggregator: Start in", waitTimestamp, "seconds...") // TODO: Debug

	select {
	case <-time.After(time.Duration(waitTimestamp) * time.Second):
	case <-ctx.Done():
		logger.Println("RunAggregator: Stopped")
		return
	}

	aggregateStep := config.C.Int64("cassandra.aggregate.step")
	ticker := time.NewTicker(time.Duration(aggregateSize) * time.Second)
	defer ticker.Stop()

	for {
		c.aggregate(time.Now(), aggregateStep, aggregateSize)

		logger.Println("RunAggregator: Aggregated") // TODO: Debug

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("RunAggregator: Stopped")
			return
		}
	}
}

func (c *Cassandra) aggregate(now time.Time, aggregateStep, aggregateSize int64) {
	nowUnix := now.Unix()
	request := types.MetricRequest{
		FromTimestamp: nowUnix - (nowUnix % aggregateSize) - aggregateSize,
		ToTimestamp:   nowUnix - (nowUnix % aggregateSize),
	}

	rowSize := config.C.Int64("batch.size")
	partitionSize := config.C.Int64("cassandra.partition_size.raw")
	fromBaseTimestamp := request.FromTimestamp - (request.FromTimestamp % partitionSize)
	toBaseTimestamp := request.ToTimestamp - (request.ToTimestamp % partitionSize)

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += partitionSize {
		fromOffsetTimestamp := math.Int64Max(request.FromTimestamp-baseTimestamp-rowSize, 0)
		toOffsetTimestamp := math.Int64Min(request.ToTimestamp-baseTimestamp, partitionSize)

		iterator := c.uuids(baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		var metricUUID string

		for iterator.Scan(&metricUUID) {
			cqlUUID, _ := gocql.ParseUUID(metricUUID)
			uuid := types.MetricUUID{UUID: [16]byte(cqlUUID)}

			request.UUIDs = append(request.UUIDs, uuid)
		}

		_ = backoff.Retry(func() error {
			err := iterator.Close()

			if err != nil {
				logger.Println("aggregate: Can't close iterator (", err, ")")
			}

			return err
		}, retry.NewBackOff(30*time.Second))
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

	fmt.Println("aggregatedMetrics:", aggregatedMetrics) // TODO: Debug

	_ = backoff.Retry(func() error {
		err := c.writeAggregated(aggregatedMetrics)

		if err != nil {
			logger.Println("aggregate: Can't write aggregated metrics (", err, ")")
		}

		return err
	}, retry.NewBackOff(30*time.Second))
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

package cassandra

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"squirreldb/aggregate"
	"squirreldb/compare"
	"squirreldb/retry"
	"squirreldb/types"
	"strings"
	"time"
)

// Run calls aggregate() every aggregate size seconds
// If the context receives a stop signal, the service is stopped
func (c *Cassandra) Run(ctx context.Context) {
	nowUnix := time.Now().Unix()
	aggregateTimestamp := nowUnix - (nowUnix % c.options.AggregateSize)
	waitTimestamp := aggregateTimestamp - nowUnix

	if waitTimestamp < 0 {
		waitTimestamp += c.options.AggregateSize
	}

	waitTimestamp += c.options.AggregateStartOffset

	logger.Println("Run: Start in", waitTimestamp, "seconds...") // TODO: Debug

	select {
	case <-time.After(time.Duration(waitTimestamp) * time.Second):
	case <-ctx.Done():
		logger.Println("Run: Stopped")
		return
	}

	aggregateTime := time.Unix(aggregateTimestamp, 0)
	ticker := time.NewTicker(time.Duration(c.options.AggregateSize) * time.Second)
	defer ticker.Stop()

	for {
		_ = backoff.Retry(func() error {
			err := c.aggregate(aggregateTime)

			if err != nil {
				logger.Println("aggregate: Can't aggregate (", err, ")")
			}

			return err
		}, retry.NewBackOff(30*time.Second))

		aggregateTime = aggregateTime.Add(time.Duration(c.options.AggregateSize) * time.Second)

		logger.Println("Run: Aggregate") // TODO: Debug

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("Run: Stopped")
			return
		}
	}
}

func (c *Cassandra) aggregate(now time.Time) error {
	nowUnix := now.Unix()
	toTimestamp := nowUnix - (nowUnix % c.options.AggregateSize)
	fromTimestamp := toTimestamp - c.options.AggregateSize

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % c.options.RawPartitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % c.options.RawPartitionSize)
	var uuids []types.MetricUUID

	perfGettingUUIDsTime := time.Now() // TODO: Performance

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += c.options.RawPartitionSize {
		fromOffsetTimestamp := compare.Int64Max(fromTimestamp-baseTimestamp-c.options.BatchSize, 0)
		toOffsetTimestamp := compare.Int64Min(toTimestamp-baseTimestamp, c.options.RawPartitionSize)

		iterator := c.uuids(baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		var metricUUID string

		for iterator.Scan(&metricUUID) {
			cqlUUID, _ := gocql.ParseUUID(metricUUID)
			uuid := types.MetricUUID{UUID: [16]byte(cqlUUID)}

			uuids = append(uuids, uuid)
		}

		if err := iterator.Close(); err != nil {
			return err
		}
	}

	fmt.Println("Gettings all UUIDs in:", time.Since(perfGettingUUIDsTime)) // TODO: Performance

	perfAggregateAllMetricsTime := time.Now() // TODO: Performance

	for _, uuid := range uuids {
		perfAggregateMetricTime := time.Now() // TODO: Performance

		request := types.MetricRequest{
			UUIDs:         []types.MetricUUID{uuid},
			FromTimestamp: nowUnix - (nowUnix % c.options.AggregateSize) - c.options.AggregateSize,
			ToTimestamp:   nowUnix - (nowUnix % c.options.AggregateSize),
		}

		metrics, err := c.Read(request)

		if err != nil {
			return err
		}

		aggregatedMetrics := aggregate.Metrics(metrics, request.FromTimestamp, request.ToTimestamp, c.options.AggregateResolution)

		if err := c.writeAggregated(aggregatedMetrics); err != nil {
			return err
		}

		fmt.Println("Read, aggregate and write one metric in:", time.Since(perfAggregateMetricTime)) // TODO: Performance
	}

	fmt.Println("Read, aggregate and write all metrics in:", time.Since(perfAggregateAllMetricsTime)) // TODO: Performance

	return nil
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

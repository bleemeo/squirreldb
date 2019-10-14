package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"log"
	"os"
	"squirreldb/aggregate"
	"squirreldb/config"
	"squirreldb/math"
	"squirreldb/types"
	"strings"
	"time"
)

var (
	logger = log.New(os.Stdout, "[cassandra] ", log.LstdFlags)
)

// TODO: Comment
func (c *Cassandra) RunAggregator(ctx context.Context) {
	nowUnix := time.Now().Unix()
	startOffset := config.C.Int64("cassandra.aggregate.start_offset")
	dayStartTimestamp := nowUnix - (nowUnix % 86400) + startOffset
	waitTimestamp := dayStartTimestamp - nowUnix

	if waitTimestamp < 0 {
		waitTimestamp += 86400
	}

	logger.Println("RunAggregator: Start in", waitTimestamp, "seconds...") // TODO: Debug

	select {
	case <-time.After(time.Duration(waitTimestamp) * time.Second):
	case <-ctx.Done():
		logger.Println("RunAggregator: Stopped")
		return
	}

	logger.Println("RunAggregator: Start") // TODO: Debug

	aggregateStep := config.C.Int64("cassandra.aggregate.step")
	aggregateSize := config.C.Int64("cassandra.aggregate.size")
	ticker := time.NewTicker(time.Duration(aggregateSize) * time.Second)
	defer ticker.Stop()

	for {
		c.aggregate(aggregateStep, aggregateSize, time.Now())

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("RunAggregator: Stopped")
			return
		}
	}
}

func (c *Cassandra) aggregate(aggregateStep, aggregateSize int64, now time.Time) {
	nowUnix := now.Unix()
	request := types.MetricRequest{
		FromTimestamp: nowUnix - (nowUnix % aggregateSize),
		ToTimestamp:   nowUnix - (nowUnix % aggregateSize) - aggregateSize,
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

		_ = iterator.Close() // TODO: Handle error
	}

	metrics, _ := c.Read(request) // TODO: Handle error

	aggregatedMetrics := aggregate.Metrics(metrics, request.FromTimestamp, request.ToTimestamp, aggregateStep)

	_ = c.writeAggregated(aggregatedMetrics) // TODO: Handle error
}

// Returns an iterator of all metrics from the data table according to the parameters
func (c *Cassandra) uuids(baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iteratorReplacer := strings.NewReplacer("$DATA_TABLE", dataTable)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT base_ts, offset_ts, values FROM $DATA_TABLE
		WHERE metric_uuid = ? AND base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
	`), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	return iterator
}

package cassandra

import (
	"context"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"github.com/gofrs/uuid"
	"log"
	"os"
	"squirreldb/aggregate"
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

	if c.options.DebugAggregateForce { // TODO: Debug
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

	uuids := c.readUUIDs()
	ticker := time.NewTicker(time.Duration(c.options.AggregateSize) * time.Second)
	defer ticker.Stop()

	for {
		nowUnix = time.Now().Unix()
		aggregateNextTimestamp.Set(float64(nowUnix+c.options.AggregateSize) * 1000)

		c.aggregateInParts(ctx, uuids, fromTimestamp, toTimestamp, 20, 4)

		fromTimestamp = toTimestamp
		toTimestamp += c.options.AggregateSize

		nowUnix = time.Now().Unix()
		aggregateLastTimestamp.Set(float64(nowUnix) * 1000)

		logger.Println("Run: Aggregate all parts") // TODO: Debug

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("Run: Stopped")
			return
		}
	}
}

func (c *Cassandra) aggregateInParts(ctx context.Context, uuids types.MetricUUIDs, fromTimestamp, toTimestamp, size int64, parts int) {
	ticker := time.NewTicker(time.Duration(size/int64(parts)) * time.Second)
	uuidsLen := len(uuids)
	var fromIndex, toIndex int
	var force bool

	for i := 0; i < parts; i++ {
		fromIndex = int(float64(uuidsLen) * (float64(i) / float64(parts)))
		toIndex = int(float64(uuidsLen) * (float64(i+1) / float64(parts)))
		uuidsPart := uuids[fromIndex:toIndex]

		_ = backoff.Retry(func() error {
			err := c.aggregate(uuidsPart, fromTimestamp, toTimestamp)

			if err != nil {
				logger.Println("aggregateInParts: Can't aggregate (", err, ")")
			}

			return err
		}, retry.NewBackOff(30*time.Second))

		logger.Println("aggregateInParts: Aggregate from", fromIndex, "to", toIndex, "on", uuidsLen, "metrics") // TODO: Debug

		if force {
			continue
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			force = true
		}
	}
}

func (c *Cassandra) aggregate(uuids types.MetricUUIDs, fromTimestamp, toTimestamp int64) error {
	startTime := time.Now()

	for _, mUUID := range uuids {
		request := types.MetricRequest{
			UUIDs:         types.MetricUUIDs{mUUID},
			FromTimestamp: fromTimestamp,
			ToTimestamp:   toTimestamp,
		}

		metrics, err := c.Read(request)

		aggregateProcessedPointsTotal.Add(float64(len(metrics[mUUID].Points)))

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
	debugLog.Println("[cassandra] aggregate():")
	debugLog.Printf("\t"+"|_ fromTimestamp: %d (%v), toTimestamp: %d (%v)"+"\n",
		fromTimestamp, time.Unix(fromTimestamp, 0), toTimestamp, time.Unix(toTimestamp, 0))
	debugLog.Printf("\t"+"|_ Process %d metric(s) in %v (%f metric(s)/s)",
		len(uuids), duration, float64(len(uuids))/duration.Seconds())

	return nil
}

func (c *Cassandra) readUUIDs() types.MetricUUIDs {
	var uuids types.MetricUUIDs

	iterator := c.readDatabaseUUIDs()
	var metricUUID string

	for iterator.Scan(&metricUUID) {
		uuidItem := types.MetricUUID{
			UUID: uuid.FromStringOrNil(metricUUID),
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

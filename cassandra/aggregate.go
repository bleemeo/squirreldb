package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"
	"log"
	"os"
	"squirreldb/aggregate"
	"squirreldb/compare"
	"squirreldb/types"
	"strings"
	"time"
)

// Run calls aggregateInParts() every aggregate size interval seconds
// If the context receives a stop signal, the service is stopped
func (c *Cassandra) Run(ctx context.Context) {
	nowUnix := time.Now().Unix()
	toTimestamp := nowUnix - (nowUnix % c.options.AggregateSize)
	fromTimestamp := compare.Int64Max(toTimestamp-c.options.AggregateSize, c.states.lastAggregationFromTimestamp)
	waitTimestamp := nowUnix - toTimestamp + c.options.AggregateStartOffset

	if c.options.DebugAggregateForce { // TODO: Debug
		fromTimestamp = toTimestamp - c.options.DebugAggregateSize
		waitTimestamp = 5
	}

	aggregateNextTimestamp.Set(float64(nowUnix+waitTimestamp) * 1000)

	logger.Println("Run: Start in", waitTimestamp, "seconds...") // TODO: Debug

	select {
	case <-time.After(time.Duration(waitTimestamp) * time.Second):
	case <-ctx.Done():
		logger.Println("Run: Stopped")
		return
	}

	ticker := time.NewTicker(time.Duration(c.options.AggregateSize) * time.Second)
	defer ticker.Stop()

	for uuids := c.readUUIDs(); ; {
		if (len(uuids) == 0) || ((fromTimestamp == c.states.lastAggregationFromTimestamp) && (c.states.lastAggregatedPart == 60)) {
			logger.Println("Run: Nothing to aggregate") // TODO: Debug
		} else {
			nowUnix = time.Now().Unix()

			c.states.lastAggregatedPart %= 60

			lastAggregatedPart, err := c.aggregateInParts(ctx, uuids, fromTimestamp, toTimestamp, 60, 60)

			c.states.lastAggregatedPart = lastAggregatedPart

			if err != nil {
				logger.Println("Run: Can't aggregate (", err, ")")
			} else {
				logger.Println("Run: Aggregate all parts") // TODO: Debug
			}
		}

		c.states.lastAggregationFromTimestamp = fromTimestamp

		if c.states.lastAggregatedPart == 0 {
			fromTimestamp = toTimestamp
			toTimestamp += c.options.AggregateSize
		}

		aggregateNextTimestamp.Set(float64(nowUnix+c.options.AggregateSize) * 1000)
		nowUnix = time.Now().Unix()
		aggregateLastTimestamp.Set(float64(nowUnix) * 1000)

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("Run: Stopped")
			return
		}
	}
}

func (c *Cassandra) aggregateInParts(ctx context.Context, uuids types.MetricUUIDs, fromTimestamp, toTimestamp, size int64, parts int) (int, error) {
	ticker := time.NewTicker(time.Duration(size/int64(parts)) * time.Second)
	uuidsLen := len(uuids)
	var fromIndex, toIndex int

	for currentPart := c.states.lastAggregatedPart + 1; currentPart <= parts; currentPart++ {
		fromIndex = int(float64(uuidsLen) * (float64(currentPart-1) / float64(parts)))
		toIndex = int(float64(uuidsLen) * (float64(currentPart) / float64(parts)))
		uuidsPart := uuids[fromIndex:toIndex]

		if err := c.aggregate(uuidsPart, fromTimestamp, toTimestamp); err != nil {
			return currentPart - 1, err
		}

		logger.Println("aggregateInParts: Aggregate from", fromIndex, "to", toIndex, "on", uuidsLen, "metrics") // TODO: Debug

		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Println("aggregateInParts: Stopped")
			return currentPart, nil
		}
	}

	return parts, nil
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

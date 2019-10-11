package cassandra

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"io"
	"log"
	"os"
	"squirreldb/aggregate"
	"squirreldb/config"
	"squirreldb/types"
	"sync"
	"time"
)

var (
	logger  = log.New(os.Stdout, "[cassandra] ", log.LstdFlags)
	backOff = backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: 0.5,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
)

func (c *Cassandra) RunAggregator(ctx context.Context, wg *sync.WaitGroup) {
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
		wg.Done()
		return
	}

	logger.Println("RunAggregator: Start") // TODO: Debug

	aggregateStep := config.C.Int64("cassandra.aggregate.step")
	aggregateSize := config.C.Int64("cassandra.aggregate.size")

	tickerInit := false
	ticker := time.NewTicker(10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !tickerInit {
				ticker.Stop()
				ticker = time.NewTicker(time.Duration(aggregateSize) * time.Second)
				tickerInit = true
			}

			c.aggregate(aggregateStep, aggregateSize, time.Now())
		case <-ctx.Done():
			logger.Println("RunAggregator: Stopped")
			wg.Done()
			return
		}
	}
}

func (c *Cassandra) aggregate(aggregateStep, aggregateSize int64, now time.Time) {
	nowUnix := now.Unix()
	toTimestamp := nowUnix - (nowUnix % aggregateSize)
	fromTimestamp := toTimestamp - aggregateSize

	logger.Println("aggregate: fromTimestamp:", fromTimestamp, "| toTimestamp:", toTimestamp) // TODO: Debug

	batchSize := config.C.Int64("batch.size")
	partitionSize := config.C.Int64("cassandra.partition_size.raw")
	fromBaseTimestamp := fromTimestamp - (fromTimestamp % partitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % partitionSize)
	fromOffsetTimestamp := fromTimestamp - fromBaseTimestamp - batchSize
	toOffsetTimestamp := toTimestamp - toBaseTimestamp

	logger.Println("aggregate: fromBaseTimestamp:", fromBaseTimestamp, "| toBaseTimestamp:", toBaseTimestamp,
		"| fromOffsetTimestamp:", fromOffsetTimestamp, "| toOffsetTimestamp:", toOffsetTimestamp) // TODO: Debug

	iterator := c.readRangeSQL(fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
	var metrics types.Metrics

	_ = backoff.Retry(func() error {
		var err error
		metrics, err = readRangeData(iterator, fromTimestamp, toTimestamp)

		if err != nil {
			logger.Println("aggregate: Can't read metrics (", err, ")") // TODO: Debug
		}

		return err
	}, &backOff)

	logger.Println("aggregate: metrics:", metrics) // TODO: Debug

	aggregatedMetrics := toAggregated(metrics, fromTimestamp, toTimestamp, aggregateStep)

	logger.Println("aggregate: aggregatedMetrics:", aggregatedMetrics)

	_ = backoff.Retry(func() error {
		err := c.writeAggregated(aggregatedMetrics)

		if err != nil {
			logger.Println("aggregate: Can't read metrics (", err, ")") // TODO: Debug
		}

		return err
	}, &backOff)
}

// Returns an iterator of all metrics from the data table according to the parameters
func (c *Cassandra) readRangeSQL(fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iterator := c.session.Query(
		"SELECT metric_uuid, base_ts, offset_ts, values FROM "+dataTable+" "+
			"WHERE base_ts >= ? AND base_ts <= ? AND offset_ts >= ? AND offset_ts <= ? "+
			"ALLOW FILTERING", // ! For development only
		fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	return iterator
}

// Writes aggregated metrics in the data aggregated table
func (c *Cassandra) writeAggregated(aggregatedMetrics map[types.MetricUUID][]aggregate.AggregatedPoint) error {
	partitionSize := config.C.Int64("cassandra.partition_size.aggregated")
	nowUnix := time.Now().Unix()
	timestampToLive := config.C.Int64("cassandra.default_time_to_live")

	for uuid, aggregatedPoints := range aggregatedMetrics {
		baseTimestampPoints := make(map[int64][]aggregate.AggregatedPoint)

		for _, point := range aggregatedPoints {
			baseTimestamp := point.Timestamp - (point.Timestamp % partitionSize)

			baseTimestampPoints[baseTimestamp] = append(baseTimestampPoints[baseTimestamp], point)
		}

		for baseTimestamp, points := range baseTimestampPoints {
			var smallestTimestamp, biggestTimestamp int64

			for i, point := range points {
				if i == 0 {
					smallestTimestamp = point.Timestamp
					biggestTimestamp = point.Timestamp
				} else if point.Timestamp < smallestTimestamp {
					smallestTimestamp = point.Timestamp
				} else if point.Timestamp > biggestTimestamp {
					biggestTimestamp = point.Timestamp
				}
			}

			age := nowUnix - biggestTimestamp

			if age < timestampToLive {
				offsetTimestamp := smallestTimestamp - baseTimestamp
				buffer := new(bytes.Buffer)

				for _, point := range points {
					pointData := []interface{}{
						uint16(point.Timestamp - baseTimestamp - offsetTimestamp),
						point.Min,
						point.Max,
						point.Average,
						point.Count,
					}

					for _, element := range pointData {
						if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
							return err
						}
					}
				}

				if err := c.writeSQL(aggregatedDataTable, gocql.UUID(uuid.UUID), baseTimestamp, offsetTimestamp, timestampToLive, buffer.Bytes()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Returns metrics
func readRangeData(iterator *gocql.Iter, fromTimestamp, toTimestamp int64) (types.Metrics, error) {
	var metricUUID string
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	metrics := make(types.Metrics)

	for iterator.Scan(&metricUUID, &baseTimestamp, &offsetTimestamp, &values) {
		cassandraUUID, _ := gocql.ParseUUID(metricUUID)
		uuid := types.MetricUUID{UUID: [16]byte(cassandraUUID)}
		buffer := bytes.NewReader(values)

	forLoop:
		for {
			var pointData struct {
				Timestamp uint16
				Value     float64
			}

			err := binary.Read(buffer, binary.BigEndian, &pointData)

			switch err {
			case nil:
				timestamp := baseTimestamp + offsetTimestamp + int64(pointData.Timestamp)

				if (timestamp >= fromTimestamp) && (timestamp <= toTimestamp) {
					point := types.MetricPoint{
						Timestamp: timestamp,
						Value:     pointData.Value,
					}

					metrics[uuid] = append(metrics[uuid], point)
				}
			case io.EOF:
				break forLoop
			default:
				return nil, err
			}
		}
	}

	return metrics, nil
}

// Convert Metrics to AggregatedMetrics
func toAggregated(metrics types.Metrics, fromTimestamp, toTimestamp, step int64) map[types.MetricUUID][]aggregate.AggregatedPoint {
	aggregatedMetrics := make(map[types.MetricUUID][]aggregate.AggregatedPoint)

	for i := fromTimestamp; i < toTimestamp; i += step {
		for uuid, points := range metrics {
			var pointsToAggregate types.MetricPoints

			for _, point := range points {
				if (point.Timestamp >= i) && (point.Timestamp <= i+step) {
					pointsToAggregate = append(pointsToAggregate, point)
				}
			}

			aggregatedPoints := aggregate.Calculate(i, pointsToAggregate)

			aggregatedMetrics[uuid] = append(aggregatedMetrics[uuid], aggregatedPoints)
		}
	}

	return aggregatedMetrics
}

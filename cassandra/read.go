package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/gocql/gocql"
	"io"
	"squirreldb/compare"
	"squirreldb/types"
	"strings"
	"time"
)

// Read returns metrics according to the request
func (c *Cassandra) Read(request types.MetricRequest) (types.Metrics, error) {
	aggregated := request.Step >= c.options.AggregateResolution

	metrics := make(types.Metrics)

	for _, uuid := range request.UUIDs {
		fromTimestamp := request.FromTimestamp
		toTimestamp := request.ToTimestamp
		var points types.MetricPoints

		if aggregated {
			aggregatedPoints, err := c.readAggregatedData(uuid, fromTimestamp, toTimestamp, request.Function)

			if err != nil {
				return nil, err
			}

			points = append(points, aggregatedPoints...)

			if len(points) != 0 {
				fromTimestamp = points[len(points)-1].Timestamp + c.options.AggregateResolution
			}
		}

		rawPoints, err := c.readRawData(uuid, fromTimestamp, toTimestamp)

		if err != nil {
			return nil, err
		}

		points = append(points, rawPoints...)

		metrics[uuid] = points
	}

	return metrics, nil
}

// Reads raw data
func (c *Cassandra) readRawData(uuid types.MetricUUID, fromTimestamp int64, toTimestamp int64) (types.MetricPoints, error) {
	startTime := time.Now()

	batchSize := c.options.BatchSize
	rawPartitionSize := c.options.RawPartitionSize
	fromBaseTimestamp := fromTimestamp - (fromTimestamp % rawPartitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % rawPartitionSize)
	dataTable := c.options.dataTable
	var points types.MetricPoints

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += rawPartitionSize {
		fromOffsetTimestamp := compare.Int64Max(fromTimestamp-baseTimestamp-batchSize, 0)
		toOffsetTimestamp := compare.Int64Min(toTimestamp-baseTimestamp, rawPartitionSize)

		iterator := c.readDatabase(dataTable, gocql.UUID(uuid.UUID), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		partitionPoints, err := iterateRawData(iterator, fromTimestamp, toTimestamp)

		if err != nil {
			return nil, err
		}

		points = append(points, partitionPoints...)
	}

	points = points.SortUnify()

	duration := time.Since(startTime)
	readRawSecondsTotal.Observe(duration.Seconds())
	readRawPointsTotal.Add(float64(len(points)))

	return points, nil
}

// Reads aggregated data
func (c *Cassandra) readAggregatedData(uuid types.MetricUUID, fromTimestamp int64, toTimestamp int64, function string) (types.MetricPoints, error) {
	startTime := time.Now()

	aggregateRowSize := c.options.AggregateSize
	aggregatePartitionSize := c.options.AggregatePartitionSize
	fromBaseTimestamp := fromTimestamp - (fromTimestamp % aggregatePartitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % aggregatePartitionSize)
	aggregateDataTable := c.options.aggregateDataTable
	var points types.MetricPoints

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += aggregatePartitionSize {
		fromOffsetTimestamp := compare.Int64Max(fromTimestamp-baseTimestamp-aggregateRowSize, 0)
		toOffsetTimestamp := compare.Int64Min(toTimestamp-baseTimestamp, aggregatePartitionSize)

		iterator := c.readDatabase(aggregateDataTable, gocql.UUID(uuid.UUID), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		partitionPoints, err := iterateAggregatedData(iterator, fromTimestamp, toTimestamp, function)

		if err != nil {
			return nil, err
		}

		points = append(points, partitionPoints...)
	}

	points = points.SortUnify()

	duration := time.Since(startTime)
	readAggregatedSecondsTotal.Observe(duration.Seconds())
	readAggregatedPointsTotal.Add(float64(len(points)))

	return points, nil
}

// Returns an iterator from the specified table according to the parameters
func (c *Cassandra) readDatabase(table string, uuid gocql.UUID, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	startTime := time.Now()

	iteratorReplacer := strings.NewReplacer("$TABLE", table)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT base_ts, offset_ts, values FROM $TABLE
		WHERE metric_uuid = ? AND base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
	`), uuid, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	duration := time.Since(startTime)
	readQueriesTotal.Observe(duration.Seconds())

	return iterator
}

// Returns metrics
func iterateRawData(iterator *gocql.Iter, fromTimestamp int64, toTimestamp int64) (types.MetricPoints, error) {
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	var points types.MetricPoints

	for iterator.Scan(&baseTimestamp, &offsetTimestamp, &values) {
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

					points = append(points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return types.MetricPoints{}, err
			}
		}
	}

	return points, nil
}

// Returns aggregated metrics
func iterateAggregatedData(iterator *gocql.Iter, fromTimestamp int64, toTimestamp int64, function string) (types.MetricPoints, error) {
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	var points types.MetricPoints

	for iterator.Scan(&baseTimestamp, &offsetTimestamp, &values) {
		buffer := bytes.NewReader(values)

	forLoop:
		for {
			var pointData struct {
				Timestamp uint16
				Min       float64
				Max       float64
				Average   float64
				Count     float64
			}

			err := binary.Read(buffer, binary.BigEndian, &pointData)

			switch err {
			case nil:
				timestamp := baseTimestamp + offsetTimestamp + int64(pointData.Timestamp)

				if (timestamp >= fromTimestamp) && (timestamp <= toTimestamp) {
					point := types.MetricPoint{
						Timestamp: timestamp,
					}

					switch function {
					case "min":
						point.Value = pointData.Min
					case "max":
						point.Value = pointData.Max
					case "avg":
						point.Value = pointData.Average
					case "count":
						point.Value = pointData.Count
					default:
						point.Value = pointData.Average
					}

					points = append(points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return types.MetricPoints{}, err
			}
		}
	}

	return points, nil
}

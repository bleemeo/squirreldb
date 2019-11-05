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
		var metricData types.MetricData

		if aggregated {
			aggregatedMetricData, err := c.readAggregatedData(uuid, request.FromTimestamp, request.ToTimestamp, request.Function)

			if err != nil {
				return nil, err
			}

			metricData = aggregatedMetricData

			length := len(metricData.Points)

			if length != 0 {
				lastPoint := metricData.Points[length-1]
				request.FromTimestamp = lastPoint.Timestamp + c.options.AggregateResolution
			}
		}

		rawMetricData, err := c.readRawData(uuid, request.FromTimestamp, request.ToTimestamp)

		if err != nil {
			return nil, err
		}

		metricData.Points = append(metricData.Points, rawMetricData.Points...)
		metricData.TimeToLive = compare.Int64Max(metricData.TimeToLive, rawMetricData.TimeToLive)

		metrics[uuid] = metricData
	}

	return metrics, nil
}

// Reads raw data
func (c *Cassandra) readRawData(uuid types.MetricUUID, fromTimestamp int64, toTimestamp int64) (types.MetricData, error) {
	startTime := time.Now()

	batchSize := c.options.BatchSize
	rawPartitionSize := c.options.RawPartitionSize
	fromBaseTimestamp := fromTimestamp - (fromTimestamp % rawPartitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % rawPartitionSize)
	dataTable := c.options.dataTable
	var metricData types.MetricData

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += rawPartitionSize {
		fromOffsetTimestamp := compare.Int64Max(fromTimestamp-baseTimestamp-batchSize, 0)
		toOffsetTimestamp := compare.Int64Min(toTimestamp-baseTimestamp, rawPartitionSize)

		iterator := c.readDatabase(dataTable, gocql.UUID(uuid.UUID), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		partitionMetricData, err := iterateRawData(iterator, fromTimestamp, toTimestamp)

		if err != nil {
			return types.MetricData{}, err
		}

		metricData.Points = append(metricData.Points, partitionMetricData.Points...)
		metricData.TimeToLive = compare.Int64Max(metricData.TimeToLive, partitionMetricData.TimeToLive)
	}

	metricData.Points = metricData.Points.Deduplicate()

	duration := time.Since(startTime)
	readRawSeconds.Observe(duration.Seconds())
	readRawPointsTotal.Add(float64(len(metricData.Points)))

	return metricData, nil
}

// Reads aggregated data
func (c *Cassandra) readAggregatedData(uuid types.MetricUUID, fromTimestamp int64, toTimestamp int64, function string) (types.MetricData, error) {
	startTime := time.Now()

	aggregateRowSize := c.options.AggregateSize
	aggregatePartitionSize := c.options.AggregatePartitionSize
	fromBaseTimestamp := fromTimestamp - (fromTimestamp % aggregatePartitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % aggregatePartitionSize)
	aggregateDataTable := c.options.aggregateDataTable
	var metricData types.MetricData

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += aggregatePartitionSize {
		fromOffsetTimestamp := compare.Int64Max(fromTimestamp-baseTimestamp-aggregateRowSize, 0)
		toOffsetTimestamp := compare.Int64Min(toTimestamp-baseTimestamp, aggregatePartitionSize)

		iterator := c.readDatabase(aggregateDataTable, gocql.UUID(uuid.UUID), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		partitionMetricData, err := iterateAggregatedData(iterator, fromTimestamp, toTimestamp, function)

		if err != nil {
			return types.MetricData{}, err
		}

		metricData.Points = append(metricData.Points, partitionMetricData.Points...)
		metricData.TimeToLive = compare.Int64Max(metricData.TimeToLive, partitionMetricData.TimeToLive)
	}

	metricData.Points = metricData.Points.Deduplicate()

	duration := time.Since(startTime)
	readAggregatedSeconds.Observe(duration.Seconds())
	readAggregatedPointsTotal.Add(float64(len(metricData.Points)))

	return metricData, nil
}

// Returns an iterator from the specified table according to the parameters
func (c *Cassandra) readDatabase(table string, uuid gocql.UUID, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	startTime := time.Now()

	iteratorReplacer := strings.NewReplacer("$TABLE", table)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT base_ts, offset_ts, TTL(values), values FROM $TABLE
		WHERE metric_uuid = ? AND base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
	`), uuid, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	duration := time.Since(startTime)
	readQueriesSeconds.Observe(duration.Seconds())

	return iterator
}

// Returns metrics
func iterateRawData(iterator *gocql.Iter, fromTimestamp int64, toTimestamp int64) (types.MetricData, error) {
	var baseTimestamp, offsetTimestamp, timeToLive int64
	var values []byte
	var metricData types.MetricData

	for iterator.Scan(&baseTimestamp, &offsetTimestamp, &timeToLive, &values) {
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

					metricData.Points = append(metricData.Points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return types.MetricData{}, err
			}
		}

		metricData.TimeToLive = compare.Int64Max(metricData.TimeToLive, timeToLive)
	}

	return metricData, nil
}

// Returns aggregated metrics
func iterateAggregatedData(iterator *gocql.Iter, fromTimestamp int64, toTimestamp int64, function string) (types.MetricData, error) {
	var baseTimestamp, offsetTimestamp, timeToLive int64
	var values []byte
	var metricData types.MetricData

	for iterator.Scan(&baseTimestamp, &offsetTimestamp, &timeToLive, &values) {
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

					metricData.Points = append(metricData.Points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return types.MetricData{}, err
			}
		}

		metricData.TimeToLive = compare.Int64Max(metricData.TimeToLive, timeToLive)
	}

	return metricData, nil
}

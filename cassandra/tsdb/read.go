package tsdb

import (
	"github.com/gocql/gocql"

	"bytes"
	"encoding/binary"
	"io"
	"squirreldb/compare"
	"squirreldb/types"
	"strings"
	"time"
)

// Read returns metrics according to the request made
func (c *CassandraTSDB) Read(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	if len(request.UUIDs) == 0 {
		return nil, nil
	}

	readAggregate := request.Step >= c.options.AggregateResolution
	metrics := make(map[types.MetricUUID]types.MetricData, len(request.UUIDs))

	for _, uuid := range request.UUIDs {
		data := types.MetricData{}
		fromTimestamp := request.FromTimestamp

		if readAggregate {
			aggregateData, err := c.readAggregateData(uuid, fromTimestamp, request.ToTimestamp, request.Function)

			if err != nil {
				return nil, err
			}

			data = aggregateData

			if len(data.Points) != 0 {
				lastPoint := data.Points[len(data.Points)-1]
				fromTimestamp = lastPoint.Timestamp + c.options.AggregateResolution
			}
		}

		if fromTimestamp > request.ToTimestamp {
			return metrics, nil
		}

		rawData, err := c.readRawData(uuid, fromTimestamp, request.ToTimestamp)

		if err != nil {
			return nil, err
		}

		data.Points = append(data.Points, rawData.Points...)
		data.TimeToLive = compare.MaxInt64(data.TimeToLive, rawData.TimeToLive)
		metrics[uuid] = data
	}

	return metrics, nil
}

// Returns aggregated data between the specified timestamps of the requested metric
func (c *CassandraTSDB) readAggregateData(uuid types.MetricUUID, fromTimestamp, toTimestamp int64, function string) (types.MetricData, error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % c.options.AggregatePartitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % c.options.AggregatePartitionSize)
	aggregateData := types.MetricData{}

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += c.options.AggregatePartitionSize {
		aggregatePartitionData, err := c.readAggregatePartitionData(uuid, fromTimestamp, toTimestamp, baseTimestamp, function)

		if err != nil {
			requestsSecondsReadAggregated.Observe(time.Since(start).Seconds())
			requestsPointsTotalReadAggregated.Add(float64(len(aggregateData.Points)))

			return types.MetricData{}, err
		}

		aggregateData.Points = append(aggregateData.Points, aggregatePartitionData.Points...)
		aggregateData.TimeToLive = compare.MaxInt64(aggregateData.TimeToLive, aggregatePartitionData.TimeToLive)
	}

	requestsPointsTotalReadAggregated.Add(float64(len(aggregateData.Points)))

	aggregateData.Points = types.DeduplicatePoints(aggregateData.Points)

	requestsSecondsReadAggregated.Observe(time.Since(start).Seconds())

	return aggregateData, nil
}

// Returns aggregated partition data between the specified timestamps of the requested metric
func (c *CassandraTSDB) readAggregatePartitionData(uuid types.MetricUUID, fromTimestamp, toTimestamp, baseTimestamp int64, function string) (types.MetricData, error) {
	fromOffsetTimestamp := fromTimestamp - baseTimestamp - c.options.AggregateSize
	toOffsetTimestamp := toTimestamp - baseTimestamp

	fromOffsetTimestamp = compare.MaxInt64(fromOffsetTimestamp, 0)

	start := time.Now()

	tableSelectDataIter := c.tableSelectDataIter(c.options.aggregateDataTable, uuid.String(), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)

	queryDuration := time.Since(start)

	aggregatePartitionData := types.MetricData{}

	var (
		offsetTimestamp int64
		timeToLive      int64
		values          []byte
	)

	start = time.Now()

	for tableSelectDataIter.Scan(&offsetTimestamp, &timeToLive, &values) {
		queryDuration += time.Since(start)

		points, err := pointsFromAggregateValues(values, fromTimestamp, toTimestamp, baseTimestamp, offsetTimestamp, c.options.AggregateResolution, function)

		if err != nil {
			cassandraQueriesSecondsRead.Observe(queryDuration.Seconds())

			return types.MetricData{}, err
		}

		aggregatePartitionData.Points = append(aggregatePartitionData.Points, points...)
		aggregatePartitionData.TimeToLive = compare.MaxInt64(aggregatePartitionData.TimeToLive, timeToLive)

		start = time.Now()
	}

	cassandraQueriesSecondsRead.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return types.MetricData{}, err
	}

	return aggregatePartitionData, nil
}

// Returns raw data between the specified timestamps of the requested metric
func (c *CassandraTSDB) readRawData(uuid types.MetricUUID, fromTimestamp, toTimestamp int64) (types.MetricData, error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % c.options.RawPartitionSize)
	toBaseTimestamp := toTimestamp - (toTimestamp % c.options.RawPartitionSize)
	rawData := types.MetricData{}

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += c.options.RawPartitionSize {
		partitionData, err := c.readRawPartitionData(uuid, fromTimestamp, toTimestamp, baseTimestamp)

		if err != nil {
			requestsSecondsReadRaw.Observe(time.Since(start).Seconds())
			requestsPointsTotalReadRaw.Add(float64(len(rawData.Points)))

			return types.MetricData{}, err
		}

		rawData.Points = append(rawData.Points, partitionData.Points...)
		rawData.TimeToLive = compare.MaxInt64(rawData.TimeToLive, partitionData.TimeToLive)
	}

	requestsPointsTotalReadRaw.Add(float64(len(rawData.Points)))

	rawData.Points = types.DeduplicatePoints(rawData.Points)

	requestsSecondsReadRaw.Observe(time.Since(start).Seconds())

	return rawData, nil
}

// Returns raw partition data between the specified timestamps of the requested metric
func (c *CassandraTSDB) readRawPartitionData(uuid types.MetricUUID, fromTimestamp, toTimestamp, baseTimestamp int64) (types.MetricData, error) {
	fromOffsetTimestamp := fromTimestamp - baseTimestamp - c.options.BatchSize
	toOffsetTimestamp := toTimestamp - baseTimestamp

	fromOffsetTimestamp = compare.MaxInt64(fromOffsetTimestamp, 0)

	start := time.Now()

	tableSelectDataIter := c.tableSelectDataIter(c.options.dataTable, uuid.String(), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)

	queryDuration := time.Since(start)

	rawPartitionData := types.MetricData{}

	var (
		offsetTimestamp int64
		timeToLive      int64
		values          []byte
	)

	start = time.Now()

	for tableSelectDataIter.Scan(&offsetTimestamp, &timeToLive, &values) {
		queryDuration += time.Since(start)

		points, err := pointsFromRawValues(values, fromTimestamp, toTimestamp, baseTimestamp, offsetTimestamp)

		if err != nil {
			cassandraQueriesSecondsRead.Observe(queryDuration.Seconds())

			return types.MetricData{}, err
		}

		rawPartitionData.Points = append(rawPartitionData.Points, points...)
		rawPartitionData.TimeToLive = compare.MaxInt64(rawPartitionData.TimeToLive, timeToLive)

		start = time.Now()
	}

	cassandraQueriesSecondsRead.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return types.MetricData{}, err
	}

	return rawPartitionData, nil
}

// Returns table select data Query
func (c *CassandraTSDB) tableSelectDataIter(table string, uuid string, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	replacer := strings.NewReplacer("$TABLE", table)
	query := c.session.Query(replacer.Replace(`
		SELECT offset_ts, TTL(values), values FROM $TABLE
		WHERE metric_uuid = ? AND base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
	`), uuid, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
	iter := query.Iter()

	return iter
}

// Return points from bytes aggregated values
func pointsFromAggregateValues(values []byte, fromTimestamp, toTimestamp, baseTimestamp, offsetTimestamp, resolution int64, function string) ([]types.MetricPoint, error) {
	buffer := bytes.NewReader(values)

	var points []types.MetricPoint

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
			timestamp := baseTimestamp + offsetTimestamp + (int64(pointData.Timestamp) * resolution)

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
			return nil, err
		}
	}

	return points, nil
}

// Return points from bytes raw values
func pointsFromRawValues(values []byte, fromTimestamp, toTimestamp, baseTimestamp, offsetTimestamp int64) ([]types.MetricPoint, error) {
	buffer := bytes.NewReader(values)

	var points []types.MetricPoint

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
			return nil, err
		}
	}

	return points, nil
}

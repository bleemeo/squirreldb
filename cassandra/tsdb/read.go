package tsdb

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"

	"bytes"
	"encoding/binary"
	"io"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

// Read returns metrics according to the request made
func (c *CassandraTSDB) Read(request types.MetricRequest) (map[gouuid.UUID]types.MetricData, error) {
	if len(request.UUIDs) == 0 {
		return nil, nil
	}

	readAggregate := request.StepMs >= c.options.AggregateResolution*1000
	metrics := make(map[gouuid.UUID]types.MetricData, len(request.UUIDs))

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
				fromTimestamp = lastPoint.Timestamp + c.options.AggregateResolution*1000
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
func (c *CassandraTSDB) readAggregateData(uuid gouuid.UUID, fromTimestamp, toTimestamp int64, function string) (types.MetricData, error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % (c.options.AggregatePartitionSize * 1000))
	toBaseTimestamp := toTimestamp - (toTimestamp % (c.options.AggregatePartitionSize * 1000))
	aggregateData := types.MetricData{}

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += (c.options.AggregatePartitionSize * 1000) {
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
func (c *CassandraTSDB) readAggregatePartitionData(uuid gouuid.UUID, fromTimestamp, toTimestamp, baseTimestamp int64, function string) (types.MetricData, error) {
	fromOffsetTimestamp := fromTimestamp - baseTimestamp - (c.options.AggregateSize * 1000)
	toOffsetTimestamp := toTimestamp - baseTimestamp

	fromOffsetTimestamp = compare.MaxInt64(fromOffsetTimestamp, 0)

	start := time.Now()

	tableSelectDataIter := c.aggregatedTableSelectDataIter(uuid.String(), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)

	queryDuration := time.Since(start)

	aggregatePartitionData := types.MetricData{}

	var (
		offsetTimestampSec int64
		timeToLive         int64
		values             []byte
	)

	start = time.Now()

	for tableSelectDataIter.Scan(&offsetTimestampSec, &timeToLive, &values) {
		queryDuration += time.Since(start)

		points, err := pointsFromAggregateValues(values, fromTimestamp, toTimestamp, baseTimestamp, offsetTimestampSec*1000, c.options.AggregateResolution, function)

		if err != nil {
			cassandraQueriesSecondsReadAggregated.Observe(queryDuration.Seconds())

			return types.MetricData{}, err
		}

		aggregatePartitionData.Points = append(aggregatePartitionData.Points, points...)
		aggregatePartitionData.TimeToLive = compare.MaxInt64(aggregatePartitionData.TimeToLive, timeToLive)

		start = time.Now()
	}

	cassandraQueriesSecondsReadAggregated.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return types.MetricData{}, err
	}

	return aggregatePartitionData, nil
}

// Returns raw data between the specified timestamps of the requested metric
func (c *CassandraTSDB) readRawData(uuid gouuid.UUID, fromTimestamp, toTimestamp int64) (types.MetricData, error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % (c.options.RawPartitionSize * 1000))
	toBaseTimestamp := toTimestamp - (toTimestamp % (c.options.RawPartitionSize * 1000))
	rawData := types.MetricData{}

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += (c.options.RawPartitionSize * 1000) {
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
func (c *CassandraTSDB) readRawPartitionData(uuid gouuid.UUID, fromTimestamp, toTimestamp, baseTimestamp int64) (types.MetricData, error) {
	fromOffsetTimestamp := fromTimestamp - baseTimestamp - (c.options.BatchSize * 1000)
	toOffsetTimestamp := toTimestamp - baseTimestamp

	fromOffsetTimestamp = compare.MaxInt64(fromOffsetTimestamp, 0)

	start := time.Now()

	tableSelectDataIter := c.rawTableSelectDataIter(uuid.String(), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)

	queryDuration := time.Since(start)

	rawPartitionData := types.MetricData{}

	var (
		offsetTimestampSec int64
		timeToLive         int64
		values             []byte
	)

	start = time.Now()

	for tableSelectDataIter.Scan(&offsetTimestampSec, &timeToLive, &values) {
		queryDuration += time.Since(start)

		points, err := pointsFromRawValues(values, fromTimestamp, toTimestamp, baseTimestamp, offsetTimestampSec*1000)

		if err != nil {
			cassandraQueriesSecondsReadRaw.Observe(queryDuration.Seconds())

			return types.MetricData{}, err
		}

		rawPartitionData.Points = append(rawPartitionData.Points, points...)
		rawPartitionData.TimeToLive = compare.MaxInt64(rawPartitionData.TimeToLive, timeToLive)

		start = time.Now()
	}

	cassandraQueriesSecondsReadRaw.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return types.MetricData{}, err
	}

	return rawPartitionData, nil
}

// Returns table select data Query
func (c *CassandraTSDB) rawTableSelectDataIter(uuid string, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	query := c.session.Query(`
		SELECT offset_ts, TTL(values), values FROM data
		WHERE metric_uuid = ? AND base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
	`, uuid, baseTimestamp/1000, fromOffsetTimestamp/1000, toOffsetTimestamp/1000)
	iter := query.Iter()

	return iter
}

func (c *CassandraTSDB) aggregatedTableSelectDataIter(uuid string, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	query := c.session.Query(`
		SELECT offset_ts, TTL(values), values FROM data_aggregated
		WHERE metric_uuid = ? AND base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
	`, uuid, baseTimestamp/1000, fromOffsetTimestamp/1000, toOffsetTimestamp/1000)
	iter := query.Iter()

	return iter
}

// Return points from bytes aggregated values
func pointsFromAggregateValues(values []byte, fromTimestamp, toTimestamp, baseTimestamp, offsetTimestamp, resolutionSec int64, function string) ([]types.MetricPoint, error) {
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
			timestamp := baseTimestamp + offsetTimestamp + (int64(pointData.Timestamp) * resolutionSec * 1000)

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
			timestamp := baseTimestamp + offsetTimestamp + int64(pointData.Timestamp)*1000

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

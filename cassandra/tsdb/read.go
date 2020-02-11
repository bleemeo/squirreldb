package tsdb

import (
	"errors"
	"os"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"

	"bytes"
	"encoding/binary"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

// Read returns metrics according to the request made
func (c *CassandraTSDB) Read(request types.MetricRequest) (map[types.MetricID]types.MetricData, error) {
	if len(request.IDs) == 0 {
		return nil, nil
	}

	readAggregate := request.StepMs >= c.options.AggregateResolution.Milliseconds()
	metrics := make(map[types.MetricID]types.MetricData, len(request.IDs))

	for _, id := range request.IDs {
		data := types.MetricData{}
		fromTimestamp := request.FromTimestamp

		if readAggregate {
			aggregateData, err := c.readAggregateData(id, fromTimestamp, request.ToTimestamp, request.Function)

			if err != nil {
				return nil, err
			}

			data = aggregateData

			if len(data.Points) != 0 {
				lastPoint := data.Points[len(data.Points)-1]
				fromTimestamp = lastPoint.Timestamp + c.options.AggregateResolution.Milliseconds()
			}
		}

		if fromTimestamp > request.ToTimestamp {
			return metrics, nil
		}

		rawData, err := c.readRawData(id, fromTimestamp, request.ToTimestamp)

		if err != nil {
			return nil, err
		}

		data.Points = append(data.Points, rawData.Points...)
		data.TimeToLive = compare.MaxInt64(data.TimeToLive, rawData.TimeToLive)
		metrics[id] = data
	}

	return metrics, nil
}

// Returns aggregated data between the specified timestamps of the requested metric
func (c *CassandraTSDB) readAggregateData(id types.MetricID, fromTimestamp, toTimestamp int64, function string) (types.MetricData, error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % c.options.AggregatePartitionSize.Milliseconds())
	toBaseTimestamp := toTimestamp - (toTimestamp % c.options.AggregatePartitionSize.Milliseconds())
	aggregateData := types.MetricData{}

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += c.options.AggregatePartitionSize.Milliseconds() {
		aggregatePartitionData, err := c.readAggregatePartitionData(id, fromTimestamp, toTimestamp, baseTimestamp, function)

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
func (c *CassandraTSDB) readAggregatePartitionData(id types.MetricID, fromTimestamp, toTimestamp, baseTimestamp int64, function string) (types.MetricData, error) {
	fromOffset := fromTimestamp - baseTimestamp - c.options.AggregateSize.Milliseconds()
	toOffset := toTimestamp - baseTimestamp

	fromOffset = compare.MaxInt64(fromOffset, 0)

	start := time.Now()

	tableSelectDataIter := c.aggregatedTableSelectDataIter(int64(id), baseTimestamp, fromOffset, toOffset)

	queryDuration := time.Since(start)

	aggregatePartitionData := types.MetricData{}

	var (
		offsetSecond int64
		timeToLive   int64
		values       []byte
	)

	start = time.Now()

	for tableSelectDataIter.Scan(&offsetSecond, &timeToLive, &values) {
		queryDuration += time.Since(start)
		offsetMs := offsetSecond * 1000

		points, err := gorillaDecodeAggregate(values, offsetMs+baseTimestamp-1, function)

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
func (c *CassandraTSDB) readRawData(id types.MetricID, fromTimestamp, toTimestamp int64) (types.MetricData, error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % c.options.RawPartitionSize.Milliseconds())
	toBaseTimestamp := toTimestamp - (toTimestamp % c.options.RawPartitionSize.Milliseconds())
	rawData := types.MetricData{}

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += c.options.RawPartitionSize.Milliseconds() {
		partitionData, err := c.readRawPartitionData(id, fromTimestamp, toTimestamp, baseTimestamp)

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
func (c *CassandraTSDB) readRawPartitionData(id types.MetricID, fromTimestamp, toTimestamp, baseTimestamp int64) (types.MetricData, error) {
	fromOffsetTimestamp := fromTimestamp - baseTimestamp - c.options.BatchSize.Milliseconds()
	toOffsetTimestamp := toTimestamp - baseTimestamp

	fromOffsetTimestamp = compare.MaxInt64(fromOffsetTimestamp, 0)

	start := time.Now()

	tableSelectDataIter := c.rawTableSelectDataIter(int64(id), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)

	queryDuration := time.Since(start)

	rawPartitionData := types.MetricData{}

	var (
		offsetMs   int64
		timeToLive int64
		values     []byte
	)

	start = time.Now()

	for tableSelectDataIter.Scan(&offsetMs, &timeToLive, &values) {
		queryDuration += time.Since(start)

		points, err := gorillaDecode(values, baseTimestamp-1)

		if err != nil {
			cassandraQueriesSecondsReadRaw.Observe(queryDuration.Seconds())

			return types.MetricData{}, err
		}

		points = filterPoints(points, fromTimestamp, toTimestamp)

		if len(points) > 0 {
			rawPartitionData.Points = append(rawPartitionData.Points, points...)
			rawPartitionData.TimeToLive = compare.MaxInt64(rawPartitionData.TimeToLive, timeToLive)
		}

		start = time.Now()
	}

	cassandraQueriesSecondsReadRaw.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return types.MetricData{}, err
	}

	return rawPartitionData, nil
}

// Returns table select data Query
func (c *CassandraTSDB) rawTableSelectDataIter(id int64, baseTimestamp, fromOffset, toOffset int64) *gocql.Iter {
	query := c.session.Query(`
		SELECT offset_ms, TTL(values), values FROM data
		WHERE metric_id = ? AND base_ts = ? AND offset_ms >= ? AND offset_ms <= ?
	`, id, baseTimestamp, fromOffset, toOffset)
	iter := query.Iter()

	return iter
}

func (c *CassandraTSDB) aggregatedTableSelectDataIter(id int64, baseTimestamp, fromOffset, toOffset int64) *gocql.Iter {
	query := c.session.Query(`
		SELECT offset_second, TTL(values), values FROM data_aggregated
		WHERE metric_id = ? AND base_ts = ? AND offset_second >= ? AND offset_second <= ?
	`, id, baseTimestamp, fromOffset/1000, toOffset/1000)
	iter := query.Iter()

	return iter
}

func gorillaDecode(values []byte, baseTimestamp int64) ([]types.MetricPoint, error) {
	i, err := tsz.NewIterator(values)
	if err != nil {
		return nil, err
	}

	result := make([]types.MetricPoint, 0)

	for i.Next() {
		t, v := i.Values()

		result = append(result, types.MetricPoint{
			Timestamp: int64(t) + baseTimestamp,
			Value:     v,
		})
	}

	return result, i.Err()
}

func gorillaDecodeAggregate(values []byte, baseTimestamp int64, function string) ([]types.MetricPoint, error) {
	var (
		length       uint16
		streamNumber int
	)

	reader := bytes.NewReader(values)

	switch function {
	case "min":
		streamNumber = 0
	case "max":
		streamNumber = 1
	case "avg":
		streamNumber = 2
	case "count":
		streamNumber = 3
	default:
		streamNumber = 2
	}

	for i := 0; i < streamNumber; i++ {
		err := binary.Read(reader, binary.BigEndian, &length)
		if err != nil {
			return nil, err
		}

		_, err = reader.Seek(int64(length), os.SEEK_CUR)
		if err != nil {
			return nil, err
		}
	}

	err := binary.Read(reader, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}

	startIndex, _ := reader.Seek(0, os.SEEK_CUR)
	endIndex := int(startIndex) + int(length)

	if endIndex > len(values) {
		return nil, errors.New("corrupted values, stored length larged than actual length")
	}

	return gorillaDecode(values[int(startIndex):endIndex], baseTimestamp)
}

func filterPoints(points []types.MetricPoint, fromTimestamp int64, toTimestamp int64) []types.MetricPoint {
	minIndex := -1
	maxIndex := len(points)

	for i, p := range points {
		if p.Timestamp >= fromTimestamp && minIndex == -1 {
			minIndex = i
		}

		if p.Timestamp > toTimestamp {
			maxIndex = i
			break
		}
	}

	if minIndex == -1 || maxIndex == -1 || minIndex >= maxIndex {
		return nil
	}

	return points[minIndex:maxIndex]
}

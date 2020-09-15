package tsdb

import (
	"errors"
	"fmt"
	"os"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"

	"bytes"
	"encoding/binary"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

type readIter struct {
	c                     *CassandraTSDB
	request               types.MetricRequest
	aggregatedToTimestamp int64
	err                   error
	current               types.MetricData
	offset                int
}

// ReadIter returns metrics according to the request made.
func (c *CassandraTSDB) ReadIter(request types.MetricRequest) (types.MetricDataSet, error) {
	c.l.Lock()

	aggregatedToTimestamp := c.fullyAggregatedAt.UnixNano() / 1e6

	c.l.Unlock()

	if aggregatedToTimestamp == 0 {
		// 1 AggregateSize cover the (maximal) normal delta before aggregation
		// the 2nd is a safe-guard that assume we lag of one aggregation.
		// Anyway normally c.fullyAggregatedAt is filled rather quickly.
		aggregatedToTimestamp = time.Now().Add(-2*c.options.AggregateSize).UnixNano() / 1e6
	}

	return &readIter{
		c:                     c,
		request:               request,
		aggregatedToTimestamp: aggregatedToTimestamp,
	}, nil
}

func (i *readIter) Err() error {
	return i.err
}

func (i *readIter) At() types.MetricData {
	return i.current
}

func (i *readIter) Next() bool {
	readAggregate := i.request.StepMs >= i.c.options.AggregateResolution.Milliseconds()

	if i.offset >= len(i.request.IDs) {
		return false
	}

	id := i.request.IDs[i.offset]
	i.offset++

	data := types.MetricData{
		ID: id,
	}
	fromTimestamp := i.request.FromTimestamp

	if readAggregate {
		aggregateData, err := i.c.readAggregateData(id, fromTimestamp, i.request.ToTimestamp, i.request.Function)

		if err != nil {
			i.err = fmt.Errorf("readAggragateData: %w", err)
			return false
		}

		data = aggregateData

		if len(data.Points) != 0 {
			lastPoint := data.Points[len(data.Points)-1]
			fromTimestamp = lastPoint.Timestamp + i.c.options.AggregateResolution.Milliseconds()
		}

		if fromTimestamp <= i.aggregatedToTimestamp {
			fromTimestamp = i.aggregatedToTimestamp
		}
	}

	if fromTimestamp <= i.request.ToTimestamp {
		rawData, err := i.c.readRawData(id, fromTimestamp, i.request.ToTimestamp)

		if err != nil {
			i.err = fmt.Errorf("readRawData: %w", err)
			return false
		}

		data.Points = append(data.Points, rawData.Points...)
		data.TimeToLive = compare.MaxInt64(data.TimeToLive, rawData.TimeToLive)
	}

	i.current = data

	return true
}

// Returns aggregated data between the specified timestamps of the requested metric.
func (c *CassandraTSDB) readAggregateData(id types.MetricID, fromTimestamp, toTimestamp int64, function string) (types.MetricData, error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % c.options.AggregatePartitionSize.Milliseconds())
	toBaseTimestamp := toTimestamp - (toTimestamp % c.options.AggregatePartitionSize.Milliseconds())
	aggregateData := types.MetricData{
		ID: id,
	}

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += c.options.AggregatePartitionSize.Milliseconds() {
		err := c.readAggregatePartitionData(&aggregateData, fromTimestamp, toTimestamp, baseTimestamp, function)

		if err != nil {
			requestsSecondsReadAggregated.Observe(time.Since(start).Seconds())
			requestsPointsTotalReadAggregated.Add(float64(len(aggregateData.Points)))

			return types.MetricData{}, err
		}
	}

	requestsPointsTotalReadAggregated.Add(float64(len(aggregateData.Points)))

	aggregateData.Points = types.DeduplicatePoints(aggregateData.Points)

	requestsSecondsReadAggregated.Observe(time.Since(start).Seconds())

	return aggregateData, nil
}

// Returns aggregated partition data between the specified timestamps of the requested metric.
func (c *CassandraTSDB) readAggregatePartitionData(aggregateData *types.MetricData, fromTimestamp, toTimestamp, baseTimestamp int64, function string) error {
	fromOffset := fromTimestamp - baseTimestamp - c.options.AggregateSize.Milliseconds()
	toOffset := toTimestamp - baseTimestamp

	fromOffset = compare.MaxInt64(fromOffset, 0)

	if toOffset > c.options.AggregatePartitionSize.Milliseconds() {
		toOffset = c.options.AggregatePartitionSize.Milliseconds()
	}

	start := time.Now()

	tableSelectDataIter := c.aggregatedTableSelectDataIter(int64(aggregateData.ID), baseTimestamp, fromOffset, toOffset)

	queryDuration := time.Since(start)

	var (
		offsetSecond int64
		timeToLive   int64
		values       []byte
	)

	start = time.Now()

	var (
		err error
	)

	tmp := c.getPointsBuffer()

	defer func() {
		c.putPointsBuffer(tmp)
	}()

	for tableSelectDataIter.Scan(&offsetSecond, &timeToLive, &values) {
		queryDuration += time.Since(start)
		offsetMs := offsetSecond * 1000

		tmp, err = gorillaDecodeAggregate(values, offsetMs+baseTimestamp-1, function, tmp)

		if err != nil {
			cassandraQueriesSecondsReadAggregated.Observe(queryDuration.Seconds())

			return fmt.Errorf("gorillaDecodeAggregate: %w", err)
		}

		aggregateData.Points = append(aggregateData.Points, tmp...)
		aggregateData.TimeToLive = compare.MaxInt64(aggregateData.TimeToLive, timeToLive)

		start = time.Now()
	}

	cassandraQueriesSecondsReadAggregated.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return err
	}

	return nil
}

// Returns raw data between the specified timestamps of the requested metric.
func (c *CassandraTSDB) readRawData(id types.MetricID, fromTimestamp, toTimestamp int64) (types.MetricData, error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % c.options.RawPartitionSize.Milliseconds())
	toBaseTimestamp := toTimestamp - (toTimestamp % c.options.RawPartitionSize.Milliseconds())
	rawData := types.MetricData{
		ID: id,
	}

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += c.options.RawPartitionSize.Milliseconds() {
		err := c.readRawPartitionData(&rawData, fromTimestamp, toTimestamp, baseTimestamp)

		if err != nil {
			requestsSecondsReadRaw.Observe(time.Since(start).Seconds())
			requestsPointsTotalReadRaw.Add(float64(len(rawData.Points)))

			return types.MetricData{}, err
		}
	}

	requestsPointsTotalReadRaw.Add(float64(len(rawData.Points)))

	rawData.Points = types.DeduplicatePoints(rawData.Points)

	requestsSecondsReadRaw.Observe(time.Since(start).Seconds())

	return rawData, nil
}

// readRawPartitionData add to rawData data between the specified timestamps of the requested metric.
func (c *CassandraTSDB) readRawPartitionData(rawData *types.MetricData, fromTimestamp, toTimestamp, baseTimestamp int64) error {
	fromOffsetTimestamp := fromTimestamp - baseTimestamp - c.options.BatchSize.Milliseconds()
	toOffsetTimestamp := toTimestamp - baseTimestamp

	fromOffsetTimestamp = compare.MaxInt64(fromOffsetTimestamp, 0)

	if toOffsetTimestamp > c.options.RawPartitionSize.Milliseconds() {
		toOffsetTimestamp = c.options.RawPartitionSize.Milliseconds()
	}

	start := time.Now()

	tableSelectDataIter := c.rawTableSelectDataIter(int64(rawData.ID), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)

	queryDuration := time.Since(start)

	var (
		offsetMs   int64
		timeToLive int64
		values     []byte
	)

	start = time.Now()

	var (
		err error
	)

	tmp := c.getPointsBuffer()

	defer func() {
		c.putPointsBuffer(tmp)
	}()

	for tableSelectDataIter.Scan(&offsetMs, &timeToLive, &values) {
		queryDuration += time.Since(start)

		tmp, err = gorillaDecode(values, baseTimestamp-1, tmp)

		if err != nil {
			cassandraQueriesSecondsReadRaw.Observe(queryDuration.Seconds())

			return fmt.Errorf("gorillaDecode: %w", err)
		}

		points := filterPoints(tmp, fromTimestamp, toTimestamp)

		if len(points) > 0 {
			rawData.Points = append(rawData.Points, points...)
			rawData.TimeToLive = compare.MaxInt64(rawData.TimeToLive, timeToLive)
		}

		start = time.Now()
	}

	cassandraQueriesSecondsReadRaw.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return err
	}

	return nil
}

// Returns table select data Query.
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

func gorillaDecode(values []byte, baseTimestamp int64, result []types.MetricPoint) ([]types.MetricPoint, error) {
	i, err := tsz.NewIterator(values)
	if err != nil {
		return nil, err
	}

	result = result[:0]

	for i.Next() {
		t, v := i.Values()

		result = append(result, types.MetricPoint{
			Timestamp: int64(t) + baseTimestamp,
			Value:     v,
		})
	}

	return result, i.Err()
}

func gorillaDecodeAggregate(values []byte, baseTimestamp int64, function string, result []types.MetricPoint) ([]types.MetricPoint, error) {
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

	return gorillaDecode(values[int(startIndex):endIndex], baseTimestamp, result)
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

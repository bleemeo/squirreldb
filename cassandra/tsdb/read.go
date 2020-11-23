package tsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"squirreldb/compare"
	"squirreldb/types"
	"time"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"
)

type readIter struct {
	c                     *CassandraTSDB
	request               types.MetricRequest
	tmp                   []types.MetricPoint
	aggregatedToTimestamp int64
	err                   error
	current               types.MetricData
	offset                int
}

// ReadIter returns metrics according to the request made.
func (c *CassandraTSDB) ReadIter(ctx context.Context, request types.MetricRequest) (types.MetricDataSet, error) {
	c.l.Lock()

	aggregatedToTimestamp := c.fullyAggregatedAt.UnixNano() / 1e6

	c.l.Unlock()

	if aggregatedToTimestamp == 0 {
		// 1 AggregateSize cover the (maximal) normal delta before aggregation
		// the 2nd is a safe-guard that assume we lag of one aggregation.
		// Anyway normally c.fullyAggregatedAt is filled rather quickly.
		aggregatedToTimestamp = time.Now().Add(-2*aggregateSize).UnixNano() / 1e6
	}

	tmp := c.getPointsBuffer()

	return &readIter{
		c:                     c,
		request:               request,
		tmp:                   tmp,
		aggregatedToTimestamp: aggregatedToTimestamp,
	}, nil
}

func (i *readIter) close() {
	if i.tmp != nil {
		i.c.putPointsBuffer(i.tmp)
		i.tmp = nil
	}
}

func (i *readIter) Err() error {
	i.close()

	return i.err
}

func (i *readIter) At() types.MetricData {
	return i.current
}

func (i *readIter) Next() bool {
	readAggregate := i.request.StepMs >= aggregateResolution.Milliseconds()

	if i.offset >= len(i.request.IDs) {
		i.close()

		return false
	}

	id := i.request.IDs[i.offset]
	i.offset++

	data := types.MetricData{
		ID: id,
	}
	fromTimestamp := i.request.FromTimestamp

	if readAggregate {
		newData, newTmp, err := i.c.readAggregateData(id, data, fromTimestamp, i.request.ToTimestamp, i.tmp, i.request.Function)

		i.tmp = newTmp
		data = newData

		if err != nil {
			i.err = fmt.Errorf("readAggragateData: %w", err)

			i.close()

			return false
		}

		if len(data.Points) != 0 {
			lastPoint := data.Points[len(data.Points)-1]
			fromTimestamp = lastPoint.Timestamp + aggregateResolution.Milliseconds()
		}

		if fromTimestamp <= i.aggregatedToTimestamp {
			fromTimestamp = i.aggregatedToTimestamp
		}
	}

	if fromTimestamp <= i.request.ToTimestamp {
		newData, newTmp, err := i.c.readRawData(id, data, fromTimestamp, i.request.ToTimestamp, i.tmp)

		i.tmp = newTmp
		data = newData

		if err != nil {
			i.err = fmt.Errorf("readRawData: %w", err)

			i.close()

			return false
		}
	}

	reversePoints(data.Points)

	i.current = data

	return true
}

// Returns aggregated data between the specified timestamps of the requested metric. Return points in descending order.
func (c *CassandraTSDB) readAggregateData(id types.MetricID, buffer types.MetricData, fromTimestamp, toTimestamp int64, tmp []types.MetricPoint, function string) (aggrData types.MetricData, newTmp []types.MetricPoint, err error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % aggregatePartitionSize.Milliseconds())
	toBaseTimestamp := toTimestamp - (toTimestamp % aggregatePartitionSize.Milliseconds())
	buffer.ID = id

	for baseTimestamp := toBaseTimestamp; baseTimestamp >= fromBaseTimestamp; baseTimestamp -= aggregatePartitionSize.Milliseconds() {
		tmp, err = c.readAggregatePartitionData(&buffer, fromTimestamp, toTimestamp, baseTimestamp, tmp, function)

		if err != nil {
			requestsSecondsReadAggregated.Observe(time.Since(start).Seconds())
			requestsPointsTotalReadAggregated.Add(float64(len(buffer.Points)))

			return buffer, tmp, err
		}
	}

	requestsPointsTotalReadAggregated.Add(float64(len(buffer.Points)))
	requestsSecondsReadAggregated.Observe(time.Since(start).Seconds())

	return buffer, tmp, nil
}

// Returns aggregated partition data between the specified timestamps of the requested metric.
func (c *CassandraTSDB) readAggregatePartitionData(aggregateData *types.MetricData, fromTimestamp, toTimestamp, baseTimestamp int64, tmp []types.MetricPoint, function string) (newTmp []types.MetricPoint, err error) {
	fromOffset := fromTimestamp - baseTimestamp - aggregatePartitionSize.Milliseconds()
	toOffset := toTimestamp - baseTimestamp

	fromOffset = compare.MaxInt64(fromOffset, 0)

	if toOffset > aggregatePartitionSize.Milliseconds() {
		toOffset = aggregatePartitionSize.Milliseconds()
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

	for tableSelectDataIter.Scan(&offsetSecond, &timeToLive, &values) {
		queryDuration += time.Since(start)

		tmp, err = gorillaDecodeAggregate(values, baseTimestamp, function, tmp, aggregateResolution.Milliseconds())
		if err != nil {
			cassandraQueriesSecondsReadAggregated.Observe(queryDuration.Seconds())

			return tmp, fmt.Errorf("gorillaDecodeAggregate: %w", err)
		}

		aggregateData.Points = mergePoints(aggregateData.Points, tmp)
		aggregateData.TimeToLive = compare.MaxInt64(aggregateData.TimeToLive, timeToLive)

		start = time.Now()
	}

	cassandraQueriesSecondsReadAggregated.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return tmp, fmt.Errorf("read aggr. table: %w", err)
	}

	return tmp, nil
}

// Returns raw data between the specified timestamps of the requested metric. Return points in descending order.
func (c *CassandraTSDB) readRawData(id types.MetricID, buffer types.MetricData, fromTimestamp, toTimestamp int64, tmp []types.MetricPoint) (rawData types.MetricData, newTmp []types.MetricPoint, err error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % rawPartitionSize.Milliseconds())
	toBaseTimestamp := toTimestamp - (toTimestamp % rawPartitionSize.Milliseconds())
	buffer.ID = id

	for baseTimestamp := toBaseTimestamp; baseTimestamp >= fromBaseTimestamp; baseTimestamp -= rawPartitionSize.Milliseconds() {
		tmp, err = c.readRawPartitionData(&buffer, fromTimestamp, toTimestamp, baseTimestamp, tmp)

		if err != nil {
			requestsSecondsReadRaw.Observe(time.Since(start).Seconds())
			requestsPointsTotalReadRaw.Add(float64(len(buffer.Points)))

			return buffer, tmp, err
		}
	}

	requestsPointsTotalReadRaw.Add(float64(len(buffer.Points)))
	requestsSecondsReadRaw.Observe(time.Since(start).Seconds())

	return buffer, tmp, nil
}

// readRawPartitionData add to rawData data between the specified timestamps of the requested metric.
func (c *CassandraTSDB) readRawPartitionData(rawData *types.MetricData, fromTimestamp, toTimestamp, baseTimestamp int64, tmp []types.MetricPoint) (newTmp []types.MetricPoint, err error) {
	fromOffsetTimestamp := fromTimestamp - baseTimestamp - rawPartitionSize.Milliseconds()
	toOffsetTimestamp := toTimestamp - baseTimestamp

	fromOffsetTimestamp = compare.MaxInt64(fromOffsetTimestamp, 0)

	if toOffsetTimestamp > rawPartitionSize.Milliseconds() {
		toOffsetTimestamp = rawPartitionSize.Milliseconds()
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

	for tableSelectDataIter.Scan(&offsetMs, &timeToLive, &values) {
		queryDuration += time.Since(start)

		tmp, err = gorillaDecode(values, baseTimestamp-1, tmp, 1)

		if err != nil {
			cassandraQueriesSecondsReadRaw.Observe(queryDuration.Seconds())

			return tmp, fmt.Errorf("gorillaDecode: %w", err)
		}

		points := filterPoints(tmp, fromTimestamp, toTimestamp)

		if len(points) > 0 {
			rawData.Points = mergePoints(rawData.Points, points)
			rawData.TimeToLive = compare.MaxInt64(rawData.TimeToLive, timeToLive)
		}

		start = time.Now()
	}

	cassandraQueriesSecondsReadRaw.Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return tmp, fmt.Errorf("read raw tables: %w", err)
	}

	return tmp, nil
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

func gorillaDecode(values []byte, baseTimestamp int64, result []types.MetricPoint, scale int64) ([]types.MetricPoint, error) {
	i, err := tsz.NewIterator(values)
	if err != nil {
		return nil, fmt.Errorf("read TSZ value: %w", err)
	}

	result = result[:0]

	for i.Next() {
		t, v := i.Values()

		result = append(result, types.MetricPoint{
			Timestamp: int64(t)*scale + baseTimestamp,
			Value:     v,
		})
	}

	return result, i.Err()
}

func gorillaDecodeAggregate(values []byte, baseTimestamp int64, function string, result []types.MetricPoint, scale int64) ([]types.MetricPoint, error) {
	var streamNumber int

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
		length, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read length for stream %d: %w", i, err)
		}

		_, err = reader.Seek(int64(length), os.SEEK_CUR)
		if err != nil {
			return nil, fmt.Errorf("seek in stream: %w", err)
		}
	}

	length, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("read stream length: %w", err)
	}

	startIndex, _ := reader.Seek(0, os.SEEK_CUR)
	endIndex := int(startIndex) + int(length)

	if endIndex > len(values) {
		return nil, errors.New("corrupted values, stored length larged than actual length")
	}

	return gorillaDecode(values[int(startIndex):endIndex], baseTimestamp, result, scale)
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

func reversePoints(points []types.MetricPoint) {
	for i := len(points)/2 - 1; i >= 0; i-- {
		opp := len(points) - 1 - i
		points[i], points[opp] = points[opp], points[i]
	}
}

// MergeSortedPoints merge two sorted list of points in-place.
// src must be sorted ascending
// dst must be sorted (and de-duplicated) in descending order.
// The result is sorted in descending order and de-duplicated.
func mergePoints(dst, src []types.MetricPoint) []types.MetricPoint {
	dstIndex := len(dst)
	srcIndex := len(src) - 1

	for srcIndex >= 0 {
		pts := src[srcIndex]

		switch {
		case dstIndex == 0 || dst[dstIndex-1].Timestamp > pts.Timestamp:
			dst = append(dst, pts)

			dstIndex++
			srcIndex--
		case dst[dstIndex-1].Timestamp == pts.Timestamp:
			// duplicated point, overwrite the existing one. The new one may be more recent
			srcIndex--
		default:
			// pts might need to be inserted in the "past". Search the insertion point
			for n := dstIndex - 1; n >= -1; n-- {
				if n == -1 || dst[n].Timestamp > pts.Timestamp {
					// pts must be inserted at buffer[n+1]
					dst = append(dst, types.MetricPoint{})

					copy(dst[n+2:dstIndex+1], dst[n+1:dstIndex])

					dst[n+1] = pts
					dstIndex++
					srcIndex--

					break
				} else if dst[n].Timestamp == pts.Timestamp {
					// duplicated point, overwrite the existing one. The new one may be more recent
					srcIndex--

					break
				}
			}
		}
	}

	return dst
}

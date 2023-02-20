package tsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"squirreldb/compare"
	"squirreldb/types"
	"time"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type readIter struct {
	ctx                   context.Context //nolint: containedctx
	err                   error
	c                     *CassandraTSDB
	tmp                   []types.MetricPoint
	current               types.MetricData
	request               types.MetricRequest
	aggregatedToTimestamp int64
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
		ctx:                   ctx,
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

	if i.request.ForcePreAggregated {
		readAggregate = true
	}

	if i.request.ForceRaw {
		readAggregate = false
	}

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
		newData, newTmp, err := i.c.readAggregateData(
			i.ctx, id, data, fromTimestamp, i.request.ToTimestamp, i.tmp, i.request.Function,
		)

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
		newData, newTmp, err := i.c.readRawData(i.ctx, id, data, fromTimestamp, i.request.ToTimestamp, i.tmp)

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
func (c *CassandraTSDB) readAggregateData(
	ctx context.Context,
	id types.MetricID,
	buffer types.MetricData,
	fromTimestamp, toTimestamp int64,
	tmp []types.MetricPoint,
	function string,
) (aggrData types.MetricData, newTmp []types.MetricPoint, err error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % aggregatePartitionSize.Milliseconds())
	toBaseTimestamp := toTimestamp - (toTimestamp % aggregatePartitionSize.Milliseconds())
	buffer.ID = id

	for baseTS := toBaseTimestamp; baseTS >= fromBaseTimestamp; baseTS -= aggregatePartitionSize.Milliseconds() {
		tmp, err = c.readAggregatePartitionData(ctx, &buffer, fromTimestamp, toTimestamp, baseTS, tmp, function)

		if err != nil {
			c.metrics.RequestsSeconds.WithLabelValues("read", "aggregated").Observe(time.Since(start).Seconds())
			c.metrics.RequestsPoints.WithLabelValues("read", "aggregated").Add(float64(len(buffer.Points)))

			return buffer, tmp, err
		}
	}

	c.metrics.RequestsPoints.WithLabelValues("read", "aggregated").Add(float64(len(buffer.Points)))
	c.metrics.RequestsSeconds.WithLabelValues("read", "aggregated").Observe(time.Since(start).Seconds())

	return buffer, tmp, nil
}

// Returns aggregated partition data between the specified timestamps of the requested metric.
func (c *CassandraTSDB) readAggregatePartitionData(
	ctx context.Context,
	aggregateData *types.MetricData,
	fromTimestamp, toTimestamp, baseTimestamp int64,
	tmp []types.MetricPoint,
	function string,
) (newTmp []types.MetricPoint, err error) {
	fromOffset := fromTimestamp - baseTimestamp - aggregatePartitionSize.Milliseconds()
	toOffset := toTimestamp - baseTimestamp

	fromOffset = compare.MaxInt64(fromOffset, 0)

	if toOffset > aggregatePartitionSize.Milliseconds() {
		toOffset = aggregatePartitionSize.Milliseconds()
	}

	start := time.Now()

	session, err := c.connection.Session()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	tableSelectDataIter := c.aggregatedTableSelectDataIter(ctx,
		session.Session,
		int64(aggregateData.ID),
		baseTimestamp,
		fromOffset,
		toOffset,
	)

	queryDuration := time.Since(start)

	var (
		offsetSecond int64
		timeToLive   int64
		values       []byte
	)

	start = time.Now()

	for tableSelectDataIter.Scan(&offsetSecond, &timeToLive, &values) {
		queryDuration += time.Since(start)

		tmp, err = c.decodeAggregatedPoints(values, baseTimestamp, offsetSecond*1000, function, tmp[:0])
		if err != nil {
			c.metrics.CassandraQueriesSeconds.WithLabelValues("read", "aggregated").Observe(queryDuration.Seconds())

			return tmp, fmt.Errorf("decodeAggregatedPoints: %w", err)
		}

		points := filterPoints(tmp, fromTimestamp, toTimestamp)

		if len(points) > 0 {
			aggregateData.Points = mergePoints(aggregateData.Points, points)
			aggregateData.TimeToLive = compare.MaxInt64(aggregateData.TimeToLive, timeToLive)
		}

		start = time.Now()
	}

	c.metrics.CassandraQueriesSeconds.WithLabelValues("read", "aggregated").Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return tmp, fmt.Errorf("read aggr. table: %w", err)
	}

	return tmp, nil
}

// Returns raw data between the specified timestamps of the requested metric. Return points in descending order.
func (c *CassandraTSDB) readRawData(
	ctx context.Context,
	id types.MetricID,
	buffer types.MetricData,
	fromTimestamp, toTimestamp int64,
	tmp []types.MetricPoint,
) (rawData types.MetricData, newTmp []types.MetricPoint, err error) {
	start := time.Now()

	fromBaseTimestamp := fromTimestamp - (fromTimestamp % rawPartitionSize.Milliseconds())
	toBaseTimestamp := toTimestamp - (toTimestamp % rawPartitionSize.Milliseconds())
	buffer.ID = id

	for baseTS := toBaseTimestamp; baseTS >= fromBaseTimestamp; baseTS -= rawPartitionSize.Milliseconds() {
		tmp, err = c.readRawPartitionData(ctx, &buffer, fromTimestamp, toTimestamp, baseTS, tmp)

		if err != nil {
			c.metrics.RequestsSeconds.WithLabelValues("read", "raw").Observe(time.Since(start).Seconds())
			c.metrics.RequestsPoints.WithLabelValues("read", "raw").Add(float64(len(buffer.Points)))

			return buffer, tmp, err
		}
	}

	c.metrics.RequestsPoints.WithLabelValues("read", "raw").Add(float64(len(buffer.Points)))
	c.metrics.RequestsSeconds.WithLabelValues("read", "raw").Observe(time.Since(start).Seconds())

	return buffer, tmp, nil
}

// readRawPartitionData add to rawData data between the specified timestamps of the requested metric.
func (c *CassandraTSDB) readRawPartitionData(
	ctx context.Context,
	rawData *types.MetricData,
	fromTimestamp, toTimestamp, baseTimestamp int64,
	tmp []types.MetricPoint,
) (newTmp []types.MetricPoint, err error) {
	fromOffsetTimestamp := fromTimestamp - baseTimestamp - rawPartitionSize.Milliseconds()
	toOffsetTimestamp := toTimestamp - baseTimestamp

	fromOffsetTimestamp = compare.MaxInt64(fromOffsetTimestamp, 0)

	if toOffsetTimestamp > rawPartitionSize.Milliseconds() {
		toOffsetTimestamp = rawPartitionSize.Milliseconds()
	}

	start := time.Now()

	session, err := c.connection.Session()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	tableSelectDataIter := c.rawTableSelectDataIter(
		ctx, session.Session, int64(rawData.ID), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp,
	)

	queryDuration := time.Since(start)

	var (
		offsetMs   int64
		timeToLive int64
		values     []byte
	)

	start = time.Now()

	for tableSelectDataIter.Scan(&offsetMs, &timeToLive, &values) {
		queryDuration += time.Since(start)

		tmp, err = c.decodePoints(values, baseTimestamp, offsetMs, tmp)

		if err != nil {
			c.metrics.CassandraQueriesSeconds.WithLabelValues("read", "raw").Observe(queryDuration.Seconds())

			return tmp, fmt.Errorf("decodePoints: %w", err)
		}

		points := filterPoints(tmp, fromTimestamp, toTimestamp)

		if len(points) > 0 {
			rawData.Points = mergePoints(rawData.Points, points)
			rawData.TimeToLive = compare.MaxInt64(rawData.TimeToLive, timeToLive)
		}

		start = time.Now()
	}

	c.metrics.CassandraQueriesSeconds.WithLabelValues("read", "raw").Observe(queryDuration.Seconds())

	if err := tableSelectDataIter.Close(); err != nil {
		return tmp, fmt.Errorf("read raw tables: %w", err)
	}

	return tmp, nil
}

// Returns table select data Query.
func (c *CassandraTSDB) rawTableSelectDataIter(
	ctx context.Context,
	session *gocql.Session,
	id int64,
	baseTimestamp,
	fromOffset,
	toOffset int64,
) *gocql.Iter {
	query := session.Query(`
		SELECT offset_ms, TTL(values), values FROM data
		WHERE metric_id = ? AND base_ts = ? AND offset_ms >= ? AND offset_ms <= ?`,
		id, baseTimestamp, fromOffset, toOffset,
	).WithContext(ctx)
	iter := query.Iter()

	return iter
}

func (c *CassandraTSDB) aggregatedTableSelectDataIter(ctx context.Context,
	session *gocql.Session,
	id int64,
	baseTimestamp,
	fromOffset,
	toOffset int64,
) *gocql.Iter {
	query := session.Query(`
		SELECT offset_second, TTL(values), values FROM data_aggregated
		WHERE metric_id = ? AND base_ts = ? AND offset_second >= ? AND offset_second <= ?`,
		id, baseTimestamp, fromOffset/1000, toOffset/1000,
	).WithContext(ctx)
	iter := query.Iter()

	return iter
}

// Decode encoded points.
// The values are mutated and should not be reused.
func (c *CassandraTSDB) decodePoints(
	values []byte,
	baseTimestamp int64,
	offset int64,
	result []types.MetricPoint,
) ([]types.MetricPoint, error) {
	if baseTimestamp+offset < c.newFormatCutoff*1000 {
		return gorillaDecode(values, baseTimestamp-1, result[:0], 1)
	}

	// The new format use the first byte as version.
	if len(values) == 0 {
		return nil, errPointsEmptyValues
	}

	switch values[0] {
	case 0:
		return c.xorChunkDecode(values[1:], result[:0])
	default:
		return nil, fmt.Errorf("%w: version=%d", errUnsupportedFormat, values[0])
	}
}

// Decode gorilla encoded values.
// The values are mutated and should not be reused.
func gorillaDecode(
	values []byte,
	baseTimestamp int64,
	result []types.MetricPoint,
	scale int64,
) ([]types.MetricPoint, error) {
	i, err := tsz.NewIterator(values)
	if err != nil {
		return nil, fmt.Errorf("read TSZ value: %w", err)
	}

	for i.Next() {
		t, v := i.Values()

		result = append(result, types.MetricPoint{
			Timestamp: int64(t)*scale + baseTimestamp,
			Value:     v,
		})
	}

	err = i.Err()
	if err != nil {
		return result, fmt.Errorf("tsz Iterator(): %w", err)
	}

	return result, nil
}

func (c *CassandraTSDB) decodeAggregatedPoints(
	values []byte,
	baseTimestamp int64,
	offset int64,
	function string, result []types.MetricPoint,
) ([]types.MetricPoint, error) {
	if baseTimestamp+offset < c.newFormatCutoff*1000 {
		return gorillaDecodeAggregate(values, baseTimestamp, function, result[:0])
	}

	// The new format use the first byte as version.
	if len(values) == 0 {
		return nil, errPointsEmptyValues
	}

	switch values[0] {
	case 0:
		return c.xorChunkDecodeAggregate(values[1:], function, result[:0])
	default:
		return nil, fmt.Errorf("%w: version=%d", errUnsupportedFormat, values[0])
	}
}

// demuxAggregate return the sub-slice that match the given aggregation function.
func demuxAggregate(values []byte, function string) ([]byte, error) {
	var streamNumber int

	reader := bytes.NewReader(values)

	//nolint:goconst
	switch function {
	case "min", "min_over_time":
		streamNumber = 0
	case "max", "max_over_time":
		streamNumber = 1
	case "avg", "avg_over_time":
		streamNumber = 2
	case "count", "count_over_time":
		streamNumber = 3
	default:
		streamNumber = 2
	}

	for i := 0; i < streamNumber; i++ {
		length, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read length for stream %d: %w", i, err)
		}

		_, err = reader.Seek(int64(length), io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("seek in stream: %w", err)
		}
	}

	length, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("read stream length: %w", err)
	}

	startIndex, _ := reader.Seek(0, io.SeekCurrent)
	endIndex := int(startIndex) + int(length)

	if endIndex > len(values) {
		return nil, errors.New("corrupted values, stored length larged than actual length")
	}

	return values[int(startIndex):endIndex], nil
}

func (c *CassandraTSDB) xorChunkDecodeAggregate(
	values []byte,
	function string,
	result []types.MetricPoint,
) ([]types.MetricPoint, error) {
	subValues, err := demuxAggregate(values, function)
	if err != nil {
		return nil, err
	}

	return c.xorChunkDecode(subValues, result)
}

func (c *CassandraTSDB) xorChunkDecode(values []byte, result []types.MetricPoint) ([]types.MetricPoint, error) {
	chunk, err := c.xorChunkPool.Get(chunkenc.EncXOR, values)
	if err != nil {
		return nil, fmt.Errorf("Get() from pool: %w", err)
	}

	defer c.xorChunkPool.Put(chunk) //nolint:errcheck

	it := chunk.Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		t, v := it.At()

		result = append(result, types.MetricPoint{
			Timestamp: t,
			Value:     v,
		})
	}

	err = it.Err()
	if err != nil {
		return result, fmt.Errorf("Iterator() fail: %w", err)
	}

	return result, nil
}

// Decode gorilla encoded aggregated points.
// The values are mutated and should not be reused.
func gorillaDecodeAggregate(
	values []byte,
	baseTimestamp int64,
	function string,
	result []types.MetricPoint,
) ([]types.MetricPoint, error) {
	subValues, err := demuxAggregate(values, function)
	if err != nil {
		return nil, err
	}

	return gorillaDecode(subValues, baseTimestamp, result, aggregateResolution.Milliseconds())
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

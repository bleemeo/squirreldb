package tsdb

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"

	"bytes"
	"encoding/binary"
	"squirreldb/aggregate"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

const concurrentWriterCount = 4 // Number of Gorouting writing concurrently

type serializedAggregatedPoint struct {
	SubOffset uint16
	Min       float64
	Max       float64
	Average   float64
	Count     float64
}

const (
	serializedAggregatedPointSize = 2 + 8*4
	serializedPointSize           = 4 + 8
)

type serializedPoint struct {
	SubOffsetMs uint32
	Value       float64
}

// Write writes all specified metrics
// metrics points should be sorted and deduplicated
func (c *CassandraTSDB) Write(metrics []types.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}

	start := time.Now()

	var rawPointsCount int

	var wg sync.WaitGroup

	wg.Add(concurrentWriterCount)

	step := len(metrics) / concurrentWriterCount

	for _, data := range metrics {
		rawPointsCount += len(data.Points)
	}

	for i := 0; i < concurrentWriterCount; i++ {
		startIndex := i * step
		endIndex := (i + 1) * step

		if endIndex > len(metrics) || i == concurrentWriterCount-1 {
			endIndex = len(metrics)
		}

		go func() {
			defer wg.Done()
			c.writeMetrics(metrics[startIndex:endIndex])
		}()
	}

	wg.Wait()

	requestsPointsTotalWriteRaw.Add(float64(rawPointsCount))
	requestsSecondsWriteRaw.Observe(time.Since(start).Seconds())

	return nil
}

// Write writes all specified metrics of the slice
func (c *CassandraTSDB) writeMetrics(metrics []types.MetricData) {
	for _, data := range metrics {
		retry.Print(func() error {
			return c.writeRawData(data) // nolint: scopelint
		}, retry.NewExponentialBackOff(retryMaxDelay), logger,
			"write points to Cassandra",
		)
	}
}

// Writes all specified aggregated metrics
func (c *CassandraTSDB) writeAggregate(aggregatedMetrics map[gouuid.UUID]aggregate.AggregatedData) error {
	if len(aggregatedMetrics) == 0 {
		return nil
	}

	start := time.Now()

	var aggregatePointsCount int

	for uuid, aggregatedData := range aggregatedMetrics {
		aggregatePointsCount += len(aggregatedData.Points)

		if err := c.writeAggregateData(uuid, aggregatedData); err != nil {
			requestsSecondsWriteAggregated.Observe(time.Since(start).Seconds())
			requestsPointsTotalWriteAggregated.Add(float64(aggregatePointsCount))

			return err
		}
	}

	requestsSecondsWriteAggregated.Observe(time.Since(start).Seconds())
	requestsPointsTotalWriteAggregated.Add(float64(aggregatePointsCount))

	return nil
}

// writeAggregateData writes aggregated data for one metric. It ensure that points with the same baseTimestamp are written together
func (c *CassandraTSDB) writeAggregateData(uuid gouuid.UUID, aggregatedData aggregate.AggregatedData) error {
	if len(aggregatedData.Points) == 0 {
		return nil
	}

	baseTimestampAggregatedPoints := make(map[int64][]aggregate.AggregatedPoint)

	for _, aggregatedPoint := range aggregatedData.Points {
		baseTimestamp := aggregatedPoint.Timestamp - (aggregatedPoint.Timestamp % (c.options.AggregatePartitionSize * 1000))

		baseTimestampAggregatedPoints[baseTimestamp] = append(baseTimestampAggregatedPoints[baseTimestamp], aggregatedPoint)
	}

	for baseTimestamp, aggregatedPoints := range baseTimestampAggregatedPoints {
		aggregatedPartitionData := aggregate.AggregatedData{
			Points:     aggregatedPoints,
			TimeToLive: aggregatedData.TimeToLive,
		}

		if err := c.writeAggregateRow(uuid, aggregatedPartitionData, baseTimestamp); err != nil {
			return err
		}
	}

	return nil
}

// writeAggregateRow writes one aggregated row
func (c *CassandraTSDB) writeAggregateRow(uuid gouuid.UUID, aggregatedData aggregate.AggregatedData, baseTimestamp int64) error {
	if len(aggregatedData.Points) == 0 {
		return nil
	}

	firstPoint := aggregatedData.Points[0]
	offsetSecond := (firstPoint.Timestamp - baseTimestamp) / 1000
	aggregateValues, err := aggregateValuesFromAggregatedPoints(aggregatedData.Points, baseTimestamp, offsetSecond, c.options.AggregateResolution)

	if err != nil {
		return err
	}

	tableInsertDataQuery := c.tableInsertAggregatedDataQuery(uuid.String(), baseTimestamp, offsetSecond, aggregatedData.TimeToLive, aggregateValues)

	start := time.Now()

	if err := tableInsertDataQuery.Exec(); err != nil {
		return err
	}

	cassandraQueriesSecondsWriteAggregated.Observe(time.Since(start).Seconds())

	return nil
}

// Write raw data per partition
func (c *CassandraTSDB) writeRawData(data types.MetricData) error {
	if len(data.Points) == 0 {
		return nil
	}

	// data.Points is sorted
	n := len(data.Points)
	startBaseTimestamp := data.Points[0].Timestamp - (data.Points[0].Timestamp % (c.options.RawPartitionSize * 1000))
	endBaseTimestamp := data.Points[n-1].Timestamp - (data.Points[n-1].Timestamp % (c.options.RawPartitionSize * 1000))

	if startBaseTimestamp == endBaseTimestamp {
		err := c.writeRawPartitionData(data, startBaseTimestamp)
		return err
	}

	currentBaseTimestamp := startBaseTimestamp
	currentStartIndex := 0

	for i, point := range data.Points {
		baseTimestamp := point.Timestamp - (point.Timestamp % (c.options.RawPartitionSize * 1000))
		if currentBaseTimestamp != baseTimestamp {
			partitionData := types.MetricData{
				UUID:       data.UUID,
				Points:     data.Points[currentStartIndex:i],
				TimeToLive: data.TimeToLive,
			}

			if err := c.writeRawPartitionData(partitionData, currentBaseTimestamp); err != nil {
				return err
			}

			currentStartIndex = i
			currentBaseTimestamp = baseTimestamp
		}
	}

	partitionData := types.MetricData{
		UUID:       data.UUID,
		Points:     data.Points[currentStartIndex:],
		TimeToLive: data.TimeToLive,
	}

	if err := c.writeRawPartitionData(partitionData, currentBaseTimestamp); err != nil {
		return err
	}

	return nil
}

// Write raw partition data
func (c *CassandraTSDB) writeRawPartitionData(data types.MetricData, baseTimestamp int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	n := len(data.Points)
	startOffsetMs := data.Points[0].Timestamp - baseTimestamp

	// The sub-offset is encoded as uint32 number of Millisecond.
	// If all points fit within this range, no need to split in multiple write
	if (data.Points[n-1].Timestamp - startOffsetMs) < 1<<32 {
		err := c.writeRawBatchData(data, baseTimestamp, startOffsetMs)
		return err
	}

	// The following is dead-code. The sub-offset will always fit

	currentOffsetMs := startOffsetMs
	currentStartIndex := 0

	for i, point := range data.Points {
		subOffsetMs := point.Timestamp - baseTimestamp - currentOffsetMs
		if subOffsetMs >= 1<<32 {
			rowData := types.MetricData{
				UUID:       data.UUID,
				Points:     data.Points[currentStartIndex:i],
				TimeToLive: data.TimeToLive,
			}

			if err := c.writeRawBatchData(rowData, baseTimestamp, currentOffsetMs); err != nil {
				return err
			}

			currentStartIndex = i
			currentOffsetMs = point.Timestamp - baseTimestamp
		}
	}

	rowData := types.MetricData{
		UUID:       data.UUID,
		Points:     data.Points[currentStartIndex:],
		TimeToLive: data.TimeToLive,
	}

	if err := c.writeRawBatchData(rowData, baseTimestamp, currentOffsetMs); err != nil {
		return err
	}

	return nil
}

func (c *CassandraTSDB) writeRawBatchData(data types.MetricData, baseTimestamp int64, offsetMs int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	rawValues, err := rawValuesFromPoints(data.Points, baseTimestamp, offsetMs)

	if err != nil {
		return err
	}

	tableInsertDataQuery := c.tableInsertRawDataQuery(data.UUID.String(), baseTimestamp, offsetMs, data.TimeToLive, rawValues)

	start := time.Now()

	if err := tableInsertDataQuery.Exec(); err != nil {
		return err
	}

	cassandraQueriesSecondsWriteRaw.Observe(time.Since(start).Seconds())

	return nil
}

// Returns table insert raw data Query
func (c *CassandraTSDB) tableInsertRawDataQuery(uuid string, baseTimestamp, offsetMs, timeToLive int64, values []byte) *gocql.Query {
	query := c.session.Query(`
		INSERT INTO data (metric_uuid, base_ts, offset_ms, insert_time, values)
		VALUES (?, ?, ?, now(), ?)
		USING TTL ?
	`, uuid, baseTimestamp, offsetMs, values, timeToLive)

	return query
}

// Returns table insert aggregated data Query
func (c *CassandraTSDB) tableInsertAggregatedDataQuery(uuid string, baseTimestamp, offsetSecond, timeToLive int64, values []byte) *gocql.Query {
	query := c.session.Query(`
		INSERT INTO data_aggregated (metric_uuid, base_ts, offset_second, values)
		VALUES (?, ?, ?, ?)
		USING TTL ?
	`, uuid, baseTimestamp, offsetSecond, values, timeToLive)

	return query
}

// Return bytes aggregated values from aggregated points
func aggregateValuesFromAggregatedPoints(aggregatedPoints []aggregate.AggregatedPoint, baseTimestamp, offsetSecond, resolutionSec int64) ([]byte, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(len(aggregatedPoints) * serializedAggregatedPointSize)

	serializedPoints := make([]serializedAggregatedPoint, len(aggregatedPoints))

	for i, aggregatedPoint := range aggregatedPoints {
		subOffset := (aggregatedPoint.Timestamp - baseTimestamp - offsetSecond*1000) / (resolutionSec * 1000)
		serializedPoints[i] = serializedAggregatedPoint{
			SubOffset: uint16(subOffset),
			Min:       aggregatedPoint.Min,
			Max:       aggregatedPoint.Max,
			Average:   aggregatedPoint.Average,
			Count:     aggregatedPoint.Count,
		}
	}

	if err := binary.Write(buffer, binary.BigEndian, serializedPoints); err != nil {
		return nil, err
	}

	aggregateValues := buffer.Bytes()

	return aggregateValues, nil
}

// Return bytes raw values from points
func rawValuesFromPoints(points []types.MetricPoint, baseTimestamp, offsetMs int64) ([]byte, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(len(points) * serializedPointSize)

	serializedPoints := make([]serializedPoint, len(points))

	for i, point := range points {
		subOffsetMs := point.Timestamp - baseTimestamp - offsetMs
		serializedPoints[i] = serializedPoint{
			SubOffsetMs: uint32(subOffsetMs),
			Value:       point.Value,
		}
	}

	if err := binary.Write(buffer, binary.BigEndian, serializedPoints); err != nil {
		return nil, err
	}

	rawValues := buffer.Bytes()

	return rawValues, nil
}

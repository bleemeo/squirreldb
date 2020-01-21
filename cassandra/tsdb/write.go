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
	Timestamp uint16
	Min       float64
	Max       float64
	Average   float64
	Count     float64
}

type serializedPoint struct {
	Timestamp uint16
	Value     float64
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
		baseTimestamp := aggregatedPoint.Timestamp - (aggregatedPoint.Timestamp % c.options.AggregatePartitionSize)

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
	offsetTimestamp := firstPoint.Timestamp - baseTimestamp
	aggregateValues, err := aggregateValuesFromAggregatedPoints(aggregatedData.Points, baseTimestamp, offsetTimestamp, c.options.AggregateResolution)

	if err != nil {
		return err
	}

	tableInsertDataQuery := c.tableInsertAggregatedDataQuery(uuid.String(), baseTimestamp, offsetTimestamp, aggregatedData.TimeToLive, aggregateValues)

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
	startBaseTimestamp := data.Points[0].Timestamp - (data.Points[0].Timestamp % c.options.RawPartitionSize)
	endBaseTimestamp := data.Points[n-1].Timestamp - (data.Points[n-1].Timestamp % c.options.RawPartitionSize)

	if startBaseTimestamp == endBaseTimestamp {
		err := c.writeRawPartitionData(data, startBaseTimestamp)
		return err
	}

	currentBaseTimestamp := startBaseTimestamp
	currentStartIndex := 0

	for i, point := range data.Points {
		baseTimestamp := point.Timestamp - (point.Timestamp % c.options.RawPartitionSize)
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
	startOffsetTimestamp := data.Points[0].Timestamp - baseTimestamp

	// The sub-offset timestamp is encoded as uint16. It first, no need to split in multiple write
	if data.Points[n-1].Timestamp-startOffsetTimestamp < 1<<16 {
		err := c.writeRawBatchData(data, baseTimestamp, startOffsetTimestamp)
		return err
	}

	currentOffsetTimestamp := startOffsetTimestamp
	currentStartIndex := 0

	for i, point := range data.Points {
		subOffset := point.Timestamp - baseTimestamp - currentOffsetTimestamp
		if subOffset >= 1<<16 {
			rowData := types.MetricData{
				UUID:       data.UUID,
				Points:     data.Points[currentStartIndex:i],
				TimeToLive: data.TimeToLive,
			}

			if err := c.writeRawBatchData(rowData, baseTimestamp, currentOffsetTimestamp); err != nil {
				return err
			}

			currentStartIndex = i
			currentOffsetTimestamp = point.Timestamp - baseTimestamp
		}
	}

	rowData := types.MetricData{
		UUID:       data.UUID,
		Points:     data.Points[currentStartIndex:],
		TimeToLive: data.TimeToLive,
	}

	if err := c.writeRawBatchData(rowData, baseTimestamp, currentOffsetTimestamp); err != nil {
		return err
	}

	return nil
}

func (c *CassandraTSDB) writeRawBatchData(data types.MetricData, baseTimestamp int64, offsetTimestamp int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	rawValues, err := rawValuesFromPoints(data.Points, baseTimestamp, offsetTimestamp)

	if err != nil {
		return err
	}

	tableInsertDataQuery := c.tableInsertRawDataQuery(data.UUID.String(), baseTimestamp, offsetTimestamp, data.TimeToLive, rawValues)

	start := time.Now()

	if err := tableInsertDataQuery.Exec(); err != nil {
		return err
	}

	cassandraQueriesSecondsWriteRaw.Observe(time.Since(start).Seconds())

	return nil
}

// Returns table insert raw data Query
func (c *CassandraTSDB) tableInsertRawDataQuery(uuid string, baseTimestamp, offsetTimestamp, timeToLive int64, values []byte) *gocql.Query {
	query := c.session.Query(`
		INSERT INTO data (metric_uuid, base_ts, offset_ts, insert_time, values)
		VALUES (?, ?, ?, now(), ?)
		USING TTL ?
	`, uuid, baseTimestamp, offsetTimestamp, values, timeToLive)

	return query
}

// Returns table insert aggregated data Query
func (c *CassandraTSDB) tableInsertAggregatedDataQuery(uuid string, baseTimestamp, offsetTimestamp, timeToLive int64, values []byte) *gocql.Query {
	query := c.session.Query(`
		INSERT INTO data_aggregated (metric_uuid, base_ts, offset_ts, values)
		VALUES (?, ?, ?, ?)
		USING TTL ?
	`, uuid, baseTimestamp, offsetTimestamp, values, timeToLive)

	return query
}

// Return bytes aggregated values from aggregated points
func aggregateValuesFromAggregatedPoints(aggregatedPoints []aggregate.AggregatedPoint, baseTimestamp, offsetTimestamp, resolution int64) ([]byte, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(len(aggregatedPoints) * 34)

	serializedPoints := make([]serializedAggregatedPoint, len(aggregatedPoints))

	for i, aggregatedPoint := range aggregatedPoints {
		pointTimestamp := (aggregatedPoint.Timestamp - baseTimestamp - offsetTimestamp) / resolution
		serializedPoints[i] = serializedAggregatedPoint{
			Timestamp: uint16(pointTimestamp),
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
func rawValuesFromPoints(points []types.MetricPoint, baseTimestamp, offsetTimestamp int64) ([]byte, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(len(points) * 10)

	serializedPoints := make([]serializedPoint, len(points))

	for i, point := range points {
		pointTimestamp := point.Timestamp - baseTimestamp - offsetTimestamp
		serializedPoints[i] = serializedPoint{
			Timestamp: uint16(pointTimestamp),
			Value:     point.Value,
		}
	}

	if err := binary.Write(buffer, binary.BigEndian, serializedPoints); err != nil {
		return nil, err
	}

	rawValues := buffer.Bytes()

	return rawValues, nil
}

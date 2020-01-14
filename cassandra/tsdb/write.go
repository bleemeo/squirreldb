package tsdb

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"

	"bytes"
	"encoding/binary"
	"squirreldb/aggregate"
	"squirreldb/retry"
	"squirreldb/types"
	"strings"
	"sync"
	"time"
)

const concurrentWriterCount = 4 // Number of Gorouting writing concurrently

// Write writes all specified metrics
// metrics points should be sorted and deduplicated
func (c *CassandraTSDB) Write(metrics map[gouuid.UUID]types.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}

	start := time.Now()

	slicesMetrics := make([]types.MetricData, len(metrics))
	slicesUUIDs := make([]gouuid.UUID, len(metrics))

	var aggregatePointsCount int

	i := 0

	for uuid, data := range metrics {
		aggregatePointsCount += len(data.Points)
		slicesUUIDs[i] = uuid
		slicesMetrics[i] = data
		i++
	}

	requestsPointsTotalWriteRaw.Add(float64(aggregatePointsCount))

	var wg sync.WaitGroup

	wg.Add(concurrentWriterCount)

	step := len(slicesMetrics) / concurrentWriterCount

	for i := 0; i < concurrentWriterCount; i++ {
		startIndex := i * step
		endIndex := (i + 1) * step

		if endIndex > len(slicesMetrics) || i == concurrentWriterCount-1 {
			endIndex = len(slicesMetrics)
		}

		go func() {
			defer wg.Done()
			c.writeMetrics(slicesUUIDs[startIndex:endIndex], slicesMetrics[startIndex:endIndex])
		}()
	}

	wg.Wait()

	requestsSecondsWriteRaw.Observe(time.Since(start).Seconds())

	return nil
}

// Write writes all specified metrics of the slice
func (c *CassandraTSDB) writeMetrics(uuids []gouuid.UUID, metrics []types.MetricData) {
	for i, data := range metrics {
		retry.Print(func() error {
			return c.writeRawData(uuids[i], data) // nolint: scopelint
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

// Write aggregated data per partition
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

		if err := c.writeAggregatePartitionData(uuid, aggregatedPartitionData, baseTimestamp); err != nil {
			return err
		}
	}

	return nil
}

// Write aggregated partition data
func (c *CassandraTSDB) writeAggregatePartitionData(uuid gouuid.UUID, aggregatedData aggregate.AggregatedData, baseTimestamp int64) error {
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
func (c *CassandraTSDB) writeRawData(uuid gouuid.UUID, data types.MetricData) error {
	if len(data.Points) == 0 {
		return nil
	}

	// data.Points is sorted
	n := len(data.Points)
	startBaseTimestamp := data.Points[0].Timestamp - (data.Points[0].Timestamp % c.options.RawPartitionSize)
	endBaseTimestamp := data.Points[n-1].Timestamp - (data.Points[n-1].Timestamp % c.options.RawPartitionSize)

	if startBaseTimestamp == endBaseTimestamp {
		err := c.writeRawPartitionData(uuid, data, startBaseTimestamp)
		return err
	}

	currentBaseTimestamp := startBaseTimestamp
	currentStartIndex := 0

	for i, point := range data.Points {
		baseTimestamp := point.Timestamp - (point.Timestamp % c.options.RawPartitionSize)
		if currentBaseTimestamp != baseTimestamp {
			partitionData := types.MetricData{
				Points:     data.Points[currentStartIndex:i],
				TimeToLive: data.TimeToLive,
			}

			if err := c.writeRawPartitionData(uuid, partitionData, currentBaseTimestamp); err != nil {
				return err
			}

			currentStartIndex = i
			currentBaseTimestamp = baseTimestamp
		}
	}

	partitionData := types.MetricData{
		Points:     data.Points[currentStartIndex:],
		TimeToLive: data.TimeToLive,
	}

	if err := c.writeRawPartitionData(uuid, partitionData, currentBaseTimestamp); err != nil {
		return err
	}

	return nil
}

// Write raw partition data
func (c *CassandraTSDB) writeRawPartitionData(uuid gouuid.UUID, data types.MetricData, baseTimestamp int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	firstPoint := data.Points[0]
	firstOffsetTimestamp := firstPoint.Timestamp - baseTimestamp
	offsetTimestampPoints := make(map[int64][]types.MetricPoint)

	for _, point := range data.Points {
		offsetTimestamp := (point.Timestamp - baseTimestamp) - ((point.Timestamp - firstOffsetTimestamp) % c.options.BatchSize)

		offsetTimestampPoints[offsetTimestamp] = append(offsetTimestampPoints[offsetTimestamp], point)
	}

	for offsetTimestamp, points := range offsetTimestampPoints {
		batchData := types.MetricData{
			Points:     points,
			TimeToLive: data.TimeToLive,
		}

		if err := c.writeRawBatchData(uuid, batchData, baseTimestamp, offsetTimestamp); err != nil {
			return err
		}
	}

	return nil
}

func (c *CassandraTSDB) writeRawBatchData(uuid gouuid.UUID, data types.MetricData, baseTimestamp int64, offsetTimestamp int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	rawValues, err := rawValuesFromPoints(data.Points, baseTimestamp, offsetTimestamp)

	if err != nil {
		return err
	}

	tableInsertDataQuery := c.tableInsertRawDataQuery(uuid.String(), baseTimestamp, offsetTimestamp, data.TimeToLive, rawValues)

	start := time.Now()

	if err := tableInsertDataQuery.Exec(); err != nil {
		return err
	}

	cassandraQueriesSecondsWriteRaw.Observe(time.Since(start).Seconds())

	return nil
}

// Returns table insert raw data Query
func (c *CassandraTSDB) tableInsertRawDataQuery(uuid string, baseTimestamp, offsetTimestamp, timeToLive int64, values []byte) *gocql.Query {
	replacer := strings.NewReplacer("$TABLE", c.options.dataTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $TABLE (metric_uuid, base_ts, offset_ts, insert_time, values)
		VALUES (?, ?, ?, now(), ?)
		USING TTL ?
	`), uuid, baseTimestamp, offsetTimestamp, values, timeToLive)

	return query
}

// Returns table insert aggregated data Query
func (c *CassandraTSDB) tableInsertAggregatedDataQuery(uuid string, baseTimestamp, offsetTimestamp, timeToLive int64, values []byte) *gocql.Query {
	replacer := strings.NewReplacer("$TABLE", c.options.aggregateDataTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $TABLE (metric_uuid, base_ts, offset_ts, values)
		VALUES (?, ?, ?, ?)
		USING TTL ?
	`), uuid, baseTimestamp, offsetTimestamp, values, timeToLive)

	return query
}

// Return bytes aggregated values from aggregated points
func aggregateValuesFromAggregatedPoints(aggregatedPoints []aggregate.AggregatedPoint, baseTimestamp, offsetTimestamp, resolution int64) ([]byte, error) {
	buffer := new(bytes.Buffer)

	for _, aggregatedPoint := range aggregatedPoints {
		pointTimestamp := (aggregatedPoint.Timestamp - baseTimestamp - offsetTimestamp) / resolution
		pointData := []interface{}{
			uint16(pointTimestamp),
			aggregatedPoint.Min,
			aggregatedPoint.Max,
			aggregatedPoint.Average,
			aggregatedPoint.Count,
		}

		for _, element := range pointData {
			if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
				return nil, err
			}
		}
	}

	aggregateValues := buffer.Bytes()

	return aggregateValues, nil
}

// Return bytes raw values from points
func rawValuesFromPoints(points []types.MetricPoint, baseTimestamp, offsetTimestamp int64) ([]byte, error) {
	buffer := new(bytes.Buffer)

	for _, point := range points {
		pointTimestamp := point.Timestamp - baseTimestamp - offsetTimestamp
		pointData := []interface{}{
			uint16(pointTimestamp),
			point.Value,
		}

		for _, element := range pointData {
			if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
				return nil, err
			}
		}
	}

	rawValues := buffer.Bytes()

	return rawValues, nil
}

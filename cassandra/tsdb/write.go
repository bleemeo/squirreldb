package tsdb

import (
	"github.com/gocql/gocql"

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
func (c *CassandraTSDB) Write(metrics map[types.MetricUUID]types.MetricData) {
	if len(metrics) == 0 {
		return
	}

	start := time.Now()

	slicesMetrics := make([]map[types.MetricUUID]types.MetricData, concurrentWriterCount)

	for i := 0; i < concurrentWriterCount; i++ {
		slicesMetrics[i] = make(map[types.MetricUUID]types.MetricData)
	}

	var aggregatePointsCount int

	for uuid, data := range metrics {
		aggregatePointsCount += len(data.Points)

		sliceIndex := int(uuid.Uint64() % uint64(concurrentWriterCount))

		slicesMetrics[sliceIndex][uuid] = data
	}

	requestsPointsTotalWriteRaw.Add(float64(aggregatePointsCount))

	var wg sync.WaitGroup

	wg.Add(concurrentWriterCount)

	for i := 0; i < concurrentWriterCount; i++ {
		i := i

		go func() {
			defer wg.Done()
			c.writeMetrics(slicesMetrics[i])
		}()
	}

	wg.Wait()

	requestsSecondsWriteRaw.Observe(time.Since(start).Seconds())
}

// Write writes all specified metrics of the slice
func (c *CassandraTSDB) writeMetrics(metrics map[types.MetricUUID]types.MetricData) {
	for uuid, data := range metrics {
		retry.Print(func() error {
			return c.writeRawData(uuid, data) // nolint: scopelint
		}, retry.NewExponentialBackOff(retryMaxDelay), logger,
			"write points to Cassandra",
		)
	}
}

// Writes all specified aggregated metrics
func (c *CassandraTSDB) writeAggregate(aggregatedMetrics map[types.MetricUUID]aggregate.AggregatedData) error {
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
func (c *CassandraTSDB) writeAggregateData(uuid types.MetricUUID, aggregatedData aggregate.AggregatedData) error {
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
func (c *CassandraTSDB) writeAggregatePartitionData(uuid types.MetricUUID, aggregatedData aggregate.AggregatedData, baseTimestamp int64) error {
	if len(aggregatedData.Points) == 0 {
		return nil
	}

	firstPoint := aggregatedData.Points[0]
	offsetTimestamp := firstPoint.Timestamp - baseTimestamp
	aggregateValues, err := aggregateValuesFromAggregatedPoints(aggregatedData.Points, baseTimestamp, offsetTimestamp, c.options.AggregateResolution)

	if err != nil {
		return err
	}

	tableInsertDataQuery := c.tableInsertDataQuery(c.options.aggregateDataTable, uuid.String(), baseTimestamp, offsetTimestamp, aggregatedData.TimeToLive, aggregateValues)

	start := time.Now()

	if err := tableInsertDataQuery.Exec(); err != nil {
		return err
	}

	cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())

	return nil
}

// Write raw data per partition
func (c *CassandraTSDB) writeRawData(uuid types.MetricUUID, data types.MetricData) error {
	if len(data.Points) == 0 {
		return nil
	}

	baseTimestampPoints := make(map[int64][]types.MetricPoint)

	for _, point := range data.Points {
		baseTimestamp := point.Timestamp - (point.Timestamp % c.options.RawPartitionSize)

		baseTimestampPoints[baseTimestamp] = append(baseTimestampPoints[baseTimestamp], point)
	}

	for baseTimestamp, points := range baseTimestampPoints {
		partitionData := types.MetricData{
			Points:     points,
			TimeToLive: data.TimeToLive,
		}

		if err := c.writeRawPartitionData(uuid, partitionData, baseTimestamp); err != nil {
			return err
		}
	}

	return nil
}

// Write raw partition data
func (c *CassandraTSDB) writeRawPartitionData(uuid types.MetricUUID, data types.MetricData, baseTimestamp int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	data.Points = types.DeduplicatePoints(data.Points)

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

func (c *CassandraTSDB) writeRawBatchData(uuid types.MetricUUID, data types.MetricData, baseTimestamp int64, offsetTimestamp int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	rawValues, err := rawValuesFromPoints(data.Points, baseTimestamp, offsetTimestamp)

	if err != nil {
		return err
	}

	tableInsertDataQuery := c.tableInsertDataQuery(c.options.dataTable, uuid.String(), baseTimestamp, offsetTimestamp, data.TimeToLive, rawValues)

	start := time.Now()

	if err := tableInsertDataQuery.Exec(); err != nil {
		return err
	}

	cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())

	return nil
}

// Returns table insert data Query
func (c *CassandraTSDB) tableInsertDataQuery(table, uuid string, baseTimestamp, offsetTimestamp, timeToLive int64, values []byte) *gocql.Query {
	replacer := strings.NewReplacer("$TABLE", table)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $TABLE (metric_uuid, base_ts, offset_ts, insert_time, values)
		VALUES (?, ?, ?, now(), ?)
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

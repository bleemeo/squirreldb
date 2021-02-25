package tsdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"squirreldb/aggregate"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"
)

const concurrentWriterCount = 4 // Number of Gorouting writing concurrently

// Write writes all specified metrics
// metrics points should be sorted and deduplicated.
func (c *CassandraTSDB) Write(ctx context.Context, metrics []types.MetricData) error {
	return c.InternalWrite(ctx, metrics, 0)
}

// InternalWrite writes all specified metrics as aggregated data
// This method should only by used for benchmark/tests or bulk import.
// Metrics points should be sorted and deduplicated.
// If writingTimestamp is not 0, it's the timestamp used to write in Cassandra (in microseconds since epoc).
func (c *CassandraTSDB) InternalWrite(ctx context.Context, metrics []types.MetricData, writingTimestamp int64) error {
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
			c.writeMetrics(metrics[startIndex:endIndex], writingTimestamp)
		}()
	}

	wg.Wait()

	requestsPointsTotalWriteRaw.Add(float64(rawPointsCount))
	requestsSecondsWriteRaw.Observe(time.Since(start).Seconds())

	return nil
}

// Write writes all specified metrics of the slice.
func (c *CassandraTSDB) writeMetrics(metrics []types.MetricData, writingTimestamp int64) {
	for _, data := range metrics {
		retry.Print(func() error {
			return c.writeRawData(data, writingTimestamp) // nolint: scopelint
		}, retry.NewExponentialBackOff(context.Background(), retryMaxDelay), logger,
			"write points to Cassandra",
		)
	}
}

// writeAggregateData writes aggregated data for one metric. It ensure that points with the same baseTimestamp are written together.
func (c *CassandraTSDB) writeAggregateData(aggregatedData aggregate.AggregatedData, writingTimestamp int64) error {
	if len(aggregatedData.Points) == 0 {
		return nil
	}

	start := time.Now()

	baseTimestampAggregatedPoints := make(map[int64][]aggregate.AggregatedPoint)

	for _, aggregatedPoint := range aggregatedData.Points {
		baseTimestamp := aggregatedPoint.Timestamp - (aggregatedPoint.Timestamp % aggregatePartitionSize.Milliseconds())

		baseTimestampAggregatedPoints[baseTimestamp] = append(baseTimestampAggregatedPoints[baseTimestamp], aggregatedPoint)
	}

	for baseTimestamp, aggregatedPoints := range baseTimestampAggregatedPoints {
		aggregatedPartitionData := aggregate.AggregatedData{
			Points:     aggregatedPoints,
			TimeToLive: aggregatedData.TimeToLive,
		}

		if err := c.writeAggregateRow(aggregatedData.ID, aggregatedPartitionData, baseTimestamp, writingTimestamp); err != nil {
			requestsSecondsWriteAggregated.Observe(time.Since(start).Seconds())
			requestsPointsTotalWriteAggregated.Add(float64(len(aggregatedData.Points)))

			return err
		}
	}

	requestsSecondsWriteAggregated.Observe(time.Since(start).Seconds())
	requestsPointsTotalWriteAggregated.Add(float64(len(aggregatedData.Points)))

	return nil
}

// writeAggregateRow writes one aggregated row.
func (c *CassandraTSDB) writeAggregateRow(id types.MetricID, aggregatedData aggregate.AggregatedData, baseTimestamp int64, writingTimestamp int64) error {
	if len(aggregatedData.Points) == 0 {
		return nil
	}

	firstPoint := aggregatedData.Points[0]
	offsetMs := firstPoint.Timestamp - baseTimestamp
	aggregateValues := gorillaEncodeAggregate(aggregatedData.Points, firstPoint.Timestamp, baseTimestamp, aggregateResolution.Milliseconds())
	maxTS := aggregatedData.Points[len(aggregatedData.Points)-1].Timestamp

	age := time.Now().Unix() - maxTS/1000
	if age < 0 {
		age = 0
	}

	if age >= aggregatedData.TimeToLive {
		return nil
	}

	tableInsertDataQuery := c.tableInsertAggregatedDataQuery(int64(id), baseTimestamp, offsetMs/1000, aggregatedData.TimeToLive-age, aggregateValues, writingTimestamp)

	start := time.Now()

	if err := tableInsertDataQuery.Exec(); err != nil {
		return fmt.Errorf("insert into data_aggregated fail: %w", err)
	}

	cassandraQueriesSecondsWriteAggregated.Observe(time.Since(start).Seconds())

	return nil
}

// Write raw data per partition.
func (c *CassandraTSDB) writeRawData(data types.MetricData, writingTimestamp int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	// data.Points is sorted
	n := len(data.Points)
	startBaseTimestamp := data.Points[0].Timestamp - (data.Points[0].Timestamp % rawPartitionSize.Milliseconds())
	endBaseTimestamp := data.Points[n-1].Timestamp - (data.Points[n-1].Timestamp % rawPartitionSize.Milliseconds())

	if startBaseTimestamp == endBaseTimestamp {
		err := c.writeRawPartitionData(data, startBaseTimestamp, writingTimestamp)

		return err
	}

	currentBaseTimestamp := startBaseTimestamp
	currentStartIndex := 0

	for i, point := range data.Points {
		baseTimestamp := point.Timestamp - (point.Timestamp % rawPartitionSize.Milliseconds())
		if currentBaseTimestamp != baseTimestamp {
			partitionData := types.MetricData{
				ID:         data.ID,
				Points:     data.Points[currentStartIndex:i],
				TimeToLive: data.TimeToLive,
			}

			if err := c.writeRawPartitionData(partitionData, currentBaseTimestamp, writingTimestamp); err != nil {
				return err
			}

			currentStartIndex = i
			currentBaseTimestamp = baseTimestamp
		}
	}

	partitionData := types.MetricData{
		ID:         data.ID,
		Points:     data.Points[currentStartIndex:],
		TimeToLive: data.TimeToLive,
	}

	return c.writeRawPartitionData(partitionData, currentBaseTimestamp, writingTimestamp)
}

// Write raw partition data.
func (c *CassandraTSDB) writeRawPartitionData(data types.MetricData, baseTimestamp int64, writingTimestamp int64) error {
	if len(data.Points) == 0 {
		return nil
	}

	offsetMs := data.Points[0].Timestamp - baseTimestamp

	// The minus one for baseTimestamp is to ensure baseTimestamp is strickyly less then
	// data.Points[0].Timestamp
	rawValues := gorillaEncode(data.Points, data.Points[0].Timestamp, baseTimestamp-1)
	maxTS := data.Points[len(data.Points)-1].Timestamp

	age := time.Now().Unix() - maxTS/1000
	if age < 0 {
		age = 0
	}

	if age >= data.TimeToLive {
		return nil
	}

	tableInsertDataQuery := c.tableInsertRawDataQuery(int64(data.ID), baseTimestamp, offsetMs, data.TimeToLive-age, rawValues, writingTimestamp)

	start := time.Now()

	if err := tableInsertDataQuery.Exec(); err != nil {
		return fmt.Errorf("unable to write raw for ID=%d, baseTimestamp=%d and offsetMs=%d: %w", data.ID, baseTimestamp, offsetMs, err)
	}

	cassandraQueriesSecondsWriteRaw.Observe(time.Since(start).Seconds())

	return nil
}

// Returns table insert raw data Query.
func (c *CassandraTSDB) tableInsertRawDataQuery(id int64, baseTimestamp, offsetMs, timeToLive int64, values []byte, writingTimestamp int64) *gocql.Query {
	if writingTimestamp == 0 {
		return c.session.Query(`
			INSERT INTO data (metric_id, base_ts, offset_ms, insert_time, values)
			VALUES (?, ?, ?, now(), ?)
			USING TTL ?
		`, id, baseTimestamp, offsetMs, values, timeToLive)
	}

	return c.session.Query(`
		INSERT INTO data (metric_id, base_ts, offset_ms, insert_time, values)
		VALUES (?, ?, ?, now(), ?)
		USING TTL ? AND TIMESTAMP ?
	`, id, baseTimestamp, offsetMs, values, timeToLive, writingTimestamp)
}

// Returns table insert aggregated data Query.
func (c *CassandraTSDB) tableInsertAggregatedDataQuery(id int64, baseTimestamp, offsetSecond, timeToLive int64, values []byte, writingTimestamp int64) *gocql.Query {
	if writingTimestamp == 0 {
		return c.session.Query(`
			INSERT INTO data_aggregated (metric_id, base_ts, offset_second, values)
			VALUES (?, ?, ?, ?)
			USING TTL ?
		`, id, baseTimestamp, offsetSecond, values, timeToLive)
	}

	return c.session.Query(`
		INSERT INTO data_aggregated (metric_id, base_ts, offset_second, values)
		VALUES (?, ?, ?, ?)
		USING TTL ? AND TIMESTAMP ?
	`, id, baseTimestamp, offsetSecond, values, timeToLive, writingTimestamp)
}

// gorillaEncode encode points using Gorilla tsz
//
// It's the encoding described in https://www.vldb.org/pvldb/vol8/p1816-teller.pdf with two change:
// * This function use millisecond precision timestamp (while Gorilla use second). This function
//   will pass the millisecond timestamp as second timestamp.
// * All timestamp of offseted by baseTimestamp (that is, the actual value stored is the timestamp
//   with baseTimestamp subtracted).
//   This second points allow timestamp to remain smaller than 32-bits integer, which is required
//   because Gorilla can't store larger timestamp (strictly speaking, can't store delta larger than
//   a 32-bits integer)
//
// There are the following constraint:
// * points must be sorted
// * baseTimestamp must be *strickly* less than all point timestamps and t0
// * t0 must be less or equal to all points timestamps
// * Delta with biggest timestamp and baseTimestamp must be less than 49 days (fit in 32 bits integer)
// * delta with first point timestamp and t0 must fit in 14-bits integer (that is ~16 seconds).
func gorillaEncode(points []types.MetricPoint, t0 int64, baseTimestamp int64) []byte {
	s := tsz.New(uint32(t0 - baseTimestamp))

	for _, point := range points {
		s.Push(uint32(point.Timestamp-baseTimestamp), point.Value)
	}

	s.Finish()

	buffer := s.Bytes()

	return buffer
}

// gorillaEncodeAggregate encode aggregated points
// It's mostly gorillaEncode() done for each aggregate (min, max, average, ...) concatened, but with all timestamp
// encoded as multiple of scale.
// It also means that same constraint as gorillaEncode apply (but any maximum time delta are scaled), for it may not
// store only ~49 days but 49 * scale days (with default of 300000 - 5 minute in milliseconds - that is ~40 000 year).
func gorillaEncodeAggregate(points []aggregate.AggregatedPoint, t0 int64, baseTimestamp int64, scale int64) []byte {
	var (
		buffer        []byte
		uvarIntBuffer [binary.MaxVarintLen32]byte
	)

	workPoint := make([]types.MetricPoint, len(points))

	for i, p := range points {
		workPoint[i].Timestamp = p.Timestamp / scale
		workPoint[i].Value = p.Min
	}

	tmp := gorillaEncode(workPoint, t0/scale, baseTimestamp/scale)
	n := binary.PutUvarint(uvarIntBuffer[:], uint64(len(tmp)))
	buffer = append(buffer, uvarIntBuffer[:n]...)
	buffer = append(buffer, tmp...)

	for i, p := range points {
		workPoint[i].Value = p.Max
	}

	tmp = gorillaEncode(workPoint, t0/scale, baseTimestamp/scale)
	n = binary.PutUvarint(uvarIntBuffer[:], uint64(len(tmp)))
	buffer = append(buffer, uvarIntBuffer[:n]...)
	buffer = append(buffer, tmp...)

	for i, p := range points {
		workPoint[i].Value = p.Average
	}

	tmp = gorillaEncode(workPoint, t0/scale, baseTimestamp/scale)
	n = binary.PutUvarint(uvarIntBuffer[:], uint64(len(tmp)))
	buffer = append(buffer, uvarIntBuffer[:n]...)
	buffer = append(buffer, tmp...)

	for i, p := range points {
		workPoint[i].Value = p.Count
	}

	tmp = gorillaEncode(workPoint, t0/scale, baseTimestamp/scale)
	n = binary.PutUvarint(uvarIntBuffer[:], uint64(len(tmp)))
	buffer = append(buffer, uvarIntBuffer[:n]...)
	buffer = append(buffer, tmp...)

	return buffer
}

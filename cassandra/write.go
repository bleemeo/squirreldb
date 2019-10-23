package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/gocql/gocql"
	"squirreldb/aggregate"
	"squirreldb/types"
	"strings"
	"time"
)

// Write writes metrics in the data table
func (c *Cassandra) Write(metrics types.Metrics) error {
	startTime := time.Now()
	var totalPoints float64

	nowUnix := time.Now().Unix()

	for uuid, metricData := range metrics {
		baseTimestampPoints := make(map[int64]types.MetricPoints)

		if metricData.TimeToLive <= 0 {
			metricData.TimeToLive = c.options.DefaultTimeToLive
		}

		for _, point := range metricData.Points {
			baseTimestamp := point.Timestamp - (point.Timestamp % c.options.RawPartitionSize)

			baseTimestampPoints[baseTimestamp] = append(baseTimestampPoints[baseTimestamp], point)
		}

		for baseTimestamp, points := range baseTimestampPoints {
			var smallestTimestamp, biggestTimestamp int64

			for i, point := range points {
				if i == 0 {
					smallestTimestamp = point.Timestamp
					biggestTimestamp = point.Timestamp
				} else if point.Timestamp < smallestTimestamp {
					smallestTimestamp = point.Timestamp
				} else if point.Timestamp > biggestTimestamp {
					biggestTimestamp = point.Timestamp
				}
			}

			age := nowUnix - biggestTimestamp
			timeToLive := metricData.TimeToLive - age

			if timeToLive > 0 {
				offsetTimestamp := smallestTimestamp - baseTimestamp
				buffer := new(bytes.Buffer)

				for _, point := range points {
					pointData := []interface{}{
						uint16(point.Timestamp - baseTimestamp - offsetTimestamp),
						point.Value,
					}

					for _, element := range pointData {
						if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
							return err
						}
					}
				}

				if err := c.writeDatabase(c.options.dataTable, gocql.UUID(uuid.UUID), baseTimestamp, offsetTimestamp, timeToLive, buffer.Bytes()); err != nil {
					return err
				}
			}
		}

		totalPoints += float64(len(metricData.Points))
	}

	duration := time.Since(startTime)
	wroteRawSecondsTotal.Observe(duration.Seconds())
	wroteRawPointsTotal.Add(totalPoints)

	return nil
}

// Writes aggregated metrics in the data aggregated table
func (c *Cassandra) writeAggregated(aggregatedMetrics aggregate.AggregatedMetrics) error {
	startTime := time.Now()
	var totalAggregatedPoints float64

	nowUnix := time.Now().Unix()

	for uuid, aggregatedData := range aggregatedMetrics {
		baseTimestampPoints := make(map[int64][]aggregate.AggregatedPoint)

		if aggregatedData.TimeToLive <= 0 {
			aggregatedData.TimeToLive = c.options.DefaultTimeToLive
		}

		for _, point := range aggregatedData.Points {
			baseTimestamp := point.Timestamp - (point.Timestamp % c.options.AggregatePartitionSize)

			baseTimestampPoints[baseTimestamp] = append(baseTimestampPoints[baseTimestamp], point)
		}

		for baseTimestamp, points := range baseTimestampPoints {
			var smallestTimestamp, biggestTimestamp int64

			for i, point := range points {
				if i == 0 {
					smallestTimestamp = point.Timestamp
					biggestTimestamp = point.Timestamp
				} else if point.Timestamp < smallestTimestamp {
					smallestTimestamp = point.Timestamp
				} else if point.Timestamp > biggestTimestamp {
					biggestTimestamp = point.Timestamp
				}
			}

			age := nowUnix - biggestTimestamp
			timeToLive := aggregatedData.TimeToLive - age

			if timeToLive > 0 {
				offsetTimestamp := smallestTimestamp - baseTimestamp
				buffer := new(bytes.Buffer)

				for _, point := range points {
					pointData := []interface{}{
						uint16(point.Timestamp - baseTimestamp - offsetTimestamp),
						point.Min,
						point.Max,
						point.Average,
						point.Count,
					}

					for _, element := range pointData {
						if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
							return err
						}
					}
				}

				if err := c.writeDatabase(c.options.aggregateDataTable, gocql.UUID(uuid.UUID), baseTimestamp, offsetTimestamp, timeToLive, buffer.Bytes()); err != nil {
					return err
				}
			}
		}

		totalAggregatedPoints += float64(len(aggregatedData.Points))
	}

	duration := time.Since(startTime)
	wroteAggregatedSecondsTotal.Observe(duration.Seconds())
	wroteAggregatedPointsTotal.Add(totalAggregatedPoints)

	return nil
}

// Write in the specified table according to the parameters
func (c *Cassandra) writeDatabase(table string, uuid gocql.UUID, baseTimestamp, offsetTimestamp, timeToLive int64, values []byte) error {
	startTime := time.Now()

	insertReplacer := strings.NewReplacer("$TABLE", table)
	insert := c.session.Query(insertReplacer.Replace(`
		INSERT INTO $TABLE (metric_uuid, base_ts, offset_ts, insert_time, values)
		VALUES (?, ?, ?, now(), ?)
		USING TTL ?
	`), uuid, baseTimestamp, offsetTimestamp, values, timeToLive)

	if err := insert.Exec(); err != nil {
		return err
	}

	duration := time.Since(startTime)
	writeQueriesTotal.Observe(duration.Seconds())

	return nil
}

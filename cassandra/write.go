package cassandra

import (
	"fmt"
	"hamsterdb/config"
	"hamsterdb/types"
	"strings"
)

func (c *Cassandra) Write(msPoints []types.MetricPoints) error {
	for _, mPoints := range msPoints {
		partsPoints := make(map[int64][]types.Point)

		for _, point := range mPoints.Points {
			timestamp := point.Time.Unix() - (point.Time.Unix() % config.PartitionLength)

			partsPoints[timestamp] = append(partsPoints[timestamp], point)
		}

		for timestamp, points := range partsPoints {
			var smallestTimestamp int64

			for i, point := range points {
				if (i == 0) || (point.Time.Unix() < smallestTimestamp) {
					smallestTimestamp = point.Time.Unix()
				}
			}

			offsetTimestamp := smallestTimestamp - timestamp

			var elements []string

			for _, point := range points {
				subOffsetTimestamp := point.Time.Unix() - timestamp - offsetTimestamp
				element := fmt.Sprintf("%d=%f", subOffsetTimestamp, point.Value)

				elements = append(elements, element)
			}

			values := []byte(strings.Join(elements, ","))

			insert := c.session.Query(
				"INSERT INTO "+metricsTable+" (metric_uuid, timestamp, offset_timestamp, insert_time, values)"+
					"VALUES (?, ?, ?, now(), ?)",
				MetricUUID(&mPoints.Metric), timestamp, offsetTimestamp, values)

			if err := insert.Exec(); err != nil {
				return err
			}
		}
	}

	return nil
}

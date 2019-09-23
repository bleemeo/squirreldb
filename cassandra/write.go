package cassandra

import (
	"fmt"
	"hamsterdb/config"
	"hamsterdb/types"
	"strings"
	"time"
)

func (c *Cassandra) Write(msPoints []types.MetricPoints) error {
	return c.write(msPoints, time.Now())
}

func (c *Cassandra) write(msPoints []types.MetricPoints, now time.Time) error {
	for _, mPoints := range msPoints {
		partsPoints := make(map[int64][]types.Point)

		for _, point := range mPoints.Points {
			baseTimestamp := point.Time.Unix() - (point.Time.Unix() % config.PartitionLength)

			partsPoints[baseTimestamp] = append(partsPoints[baseTimestamp], point)
		}

		for baseTimestamp, points := range partsPoints {
			var smallestTime time.Time
			var biggestTime time.Time

			for i, point := range points {
				if (i == 0) || (point.Time.Before(smallestTime)) {
					smallestTime = point.Time
				}
				if (i == 0) || (point.Time.After(biggestTime)) {
					biggestTime = point.Time
				}
			}

			age := now.Unix() - biggestTime.Unix()
			timeToLive := int64(config.CassandraMetricRetention)

			if age < timeToLive {
				var elements []string
				offsetTimestamp := smallestTime.Unix() - baseTimestamp

				for _, point := range points {
					subOffsetTimestamp := point.Time.Unix() - baseTimestamp - offsetTimestamp
					element := fmt.Sprintf("%d=%f", subOffsetTimestamp, point.Value)

					elements = append(elements, element)
				}

				values := []byte(strings.Join(elements, ","))

				insert := c.session.Query(
					"INSERT INTO "+metricsTable+" (metric_uuid, base_ts, offset_ts, insert_time, values)"+
						"VALUES (?, ?, ?, now(), ?) "+
						"USING TTL ?",
					MetricUUID(&mPoints.Metric), baseTimestamp, offsetTimestamp, values, timeToLive)

				if err := insert.Exec(); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

package cassandra

import (
	"bytes"
	"encoding/binary"
	"hamsterdb/config"
	"hamsterdb/types"
	"time"
)

func (c *Cassandra) Write(msPoints []types.MetricPoints) error {
	return c.write(msPoints, time.Now())
}

func (c *Cassandra) write(msPoints []types.MetricPoints, now time.Time) error {
	for _, mPoints := range msPoints {
		uuid := metricUUID(mPoints.Metric)
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
				offsetTimestamp := smallestTime.Unix() - baseTimestamp
				buffer := new(bytes.Buffer)

				for _, point := range points {
					pointData := []interface{}{
						uint16(point.Time.Unix() - baseTimestamp - offsetTimestamp),
						point.Value,
					}

					for _, element := range pointData {
						if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
							// TODO: Handle error
							return err
						}
					}
				}

				insert := c.session.Query(
					"INSERT INTO "+metricsTable+" (metric_uuid, base_ts, offset_ts, insert_time, values)"+
						"VALUES (?, ?, ?, now(), ?) "+
						"USING TTL ?",
					uuid, baseTimestamp, offsetTimestamp, buffer.Bytes(), timeToLive)

				if err := insert.Exec(); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

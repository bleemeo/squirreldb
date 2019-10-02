package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"squirreldb/config"
	"squirreldb/types"
	"time"
)

// Write the points in the database
func (c *Cassandra) Write(msPoints []types.MetricPoints) error {
	for _, mPoints := range msPoints {
		var mUUID types.MetricUUID

		_ = backoff.Retry(func() error {
			var err error
			mUUID, err = mPoints.UUID()

			return err
		}, exponentialBackOff)

		// Sort points by partition
		tsPoints := make(map[int64][]types.Point)
		partitionLengthSecs := int64(config.PartitionLength.Seconds())

		for _, point := range mPoints.Points {
			baseTimestamp := point.Time.Unix() - (point.Time.Unix() % partitionLengthSecs)

			tsPoints[baseTimestamp] = append(tsPoints[baseTimestamp], point)
		}

		for baseTimestamp, points := range tsPoints {
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

			age := time.Now().Unix() - biggestTime.Unix()
			timeToLiveSecs := int64(config.CassandraMetricRetention.Seconds())

			if age < timeToLiveSecs {
				offsetTimestamp := smallestTime.Unix() - baseTimestamp
				buffer := new(bytes.Buffer)

				for _, point := range points {
					pointData := []interface{}{
						uint16(point.Time.Unix() - baseTimestamp - offsetTimestamp),
						point.Value,
					}

					for _, element := range pointData {
						if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
							return err
						}
					}
				}

				if err := c.writeSQL(gocql.UUID(mUUID.UUID), baseTimestamp, offsetTimestamp, timeToLiveSecs, buffer.Bytes()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Insert in the database according to the parameters
func (c *Cassandra) writeSQL(uuid gocql.UUID, baseTimestamp, offsetTimestamp, timeToLive int64, values []byte) error {
	insert := c.session.Query(
		"INSERT INTO "+metricsTable+" (metric_uuid, base_ts, offset_ts, insert_time, values)"+
			"VALUES (?, ?, ?, now(), ?) "+
			"USING TTL ?",
		uuid, baseTimestamp, offsetTimestamp, values, timeToLive)

	if err := insert.Exec(); err != nil {
		return err
	}

	return nil
}

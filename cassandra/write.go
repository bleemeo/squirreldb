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
	timeToLive := config.C.Int64("cassandra.time_to_live")

	for _, mPoints := range msPoints {
		partitionSize := config.C.Int64("cassandra.partition_size.classic")
		baseTimestampsPoints := make(map[int64][]types.Point)

		for _, point := range mPoints.Points {
			baseTimestamp := point.Time.Unix() - (point.Time.Unix() % partitionSize)

			baseTimestampsPoints[baseTimestamp] = append(baseTimestampsPoints[baseTimestamp], point)
		}

		var mUUID types.MetricUUID

		_ = backoff.Retry(func() error {
			var err error
			mUUID, err = mPoints.UUID()

			if err != nil {
				logger.Println("Write: Can't generate UUID (", err, ")")
			}

			return err
		}, exponentialBackOff)

		for baseTimestamp, points := range baseTimestampsPoints {
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
							return err
						}
					}
				}

				if err := c.writeSQL(dataTable, gocql.UUID(mUUID.UUID), baseTimestamp, offsetTimestamp, timeToLive, buffer.Bytes()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (c *Cassandra) writePointData(mPoints *types.MetricPoints, timeToLive int64) error {
	partitionSize := config.C.Int64("cassandra.partition_size.classic")
	baseTimestampsPoints := make(map[int64][]types.Point)

	for _, point := range mPoints.Points {
		baseTimestamp := point.Time.Unix() - (point.Time.Unix() % partitionSize)

		baseTimestampsPoints[baseTimestamp] = append(baseTimestampsPoints[baseTimestamp], point)
	}

	var mUUID types.MetricUUID

	_ = backoff.Retry(func() error {
		var err error
		mUUID, err = mPoints.UUID()

		if err != nil {
			logger.Println("Write: Can't generate UUID (", err, ")")
		}

		return err
	}, exponentialBackOff)

	for baseTimestamp, points := range baseTimestampsPoints {
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
						return err
					}
				}
			}

			if err := c.writeSQL(dataTable, gocql.UUID(mUUID.UUID), baseTimestamp, offsetTimestamp, timeToLive, buffer.Bytes()); err != nil {
				return err
			}
		}
	}

	return nil
}

// Insert in the specified table according to the parameters
func (c *Cassandra) writeSQL(table string, uuid gocql.UUID, baseTimestamp, offsetTimestamp, timeToLive int64, values []byte) error {
	insert := c.session.Query(
		"INSERT INTO "+table+" (metric_uuid, base_ts, offset_ts, insert_time, values)"+
			"VALUES (?, ?, ?, now(), ?) "+
			"USING TTL ?",
		uuid, baseTimestamp, offsetTimestamp, values, timeToLive)

	if err := insert.Exec(); err != nil {
		return err
	}

	return nil
}

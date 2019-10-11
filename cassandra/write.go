package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/gocql/gocql"
	"squirreldb/config"
	"squirreldb/types"
	"time"
)

// Write writes metrics in the data table
func (c *Cassandra) Write(metrics types.Metrics) error {
	partitionSize := config.C.Int64("cassandra.partition_size.raw")
	nowUnix := time.Now().Unix()
	timestampToLive := config.C.Int64("cassandra.default_time_to_live") // TODO: Support Time to live label

	for uuid, points := range metrics {
		baseTimestampPoints := make(map[int64]types.MetricPoints)

		for _, point := range points {
			baseTimestamp := point.Timestamp - (point.Timestamp % partitionSize)

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

			if age < timestampToLive {
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

				if err := c.writeSQL(dataTable, gocql.UUID(uuid.UUID), baseTimestamp, offsetTimestamp, timestampToLive, buffer.Bytes()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Write in the specified table according to the parameters
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

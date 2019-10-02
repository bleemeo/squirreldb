package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"io"
	"squirreldb/config"
	"squirreldb/types"
	"time"
)

// Returns the list of points meeting the conditions of the request
func (c *Cassandra) Read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	var mUUID types.MetricUUID

	_ = backoff.Retry(func() error {
		var err error
		mUUID, err = mRequest.UUID()

		return err
	}, exponentialBackOff)

	partitionLengthSecs := int64(config.PartitionLength.Seconds())
	fromBaseTimestamp := mRequest.FromTime.Unix() - (mRequest.FromTime.Unix() % partitionLengthSecs)
	toBaseTimestamp := mRequest.ToTime.Unix() - (mRequest.ToTime.Unix() % partitionLengthSecs)
	fromOffsetTimestamp := mRequest.FromTime.Unix() - fromBaseTimestamp - int64(config.BatchDuration.Seconds())
	toOffsetTimestamp := mRequest.ToTime.Unix() - toBaseTimestamp

	iterator := c.readSQL(gocql.UUID(mUUID.UUID), fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)

	var metricUUID string
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	results := make(map[string]types.MetricPoints)

	for iterator.Scan(&metricUUID, &baseTimestamp, &offsetTimestamp, &values) {
		item, exists := results[metricUUID]

		if !exists {
			item = types.MetricPoints{
				Metric: mRequest.Metric,
			}
		}

		buffer := bytes.NewReader(values)

		for {
			var pointData struct {
				Timestamp uint16
				Value     float64
			}

			err := binary.Read(buffer, binary.BigEndian, &pointData)

			if err == nil {
				pointTime := time.Unix(baseTimestamp+offsetTimestamp+int64(pointData.Timestamp), 0)

				if (mRequest.Step == 0 || ((int64(pointData.Timestamp) % mRequest.Step) == 0)) &&
					!pointTime.Before(mRequest.FromTime) && !pointTime.After(mRequest.ToTime) {

					point := types.Point{
						Time:  pointTime,
						Value: pointData.Value,
					}

					item.Points = append(item.Points, point)
				}
			} else if err != io.EOF {
				return nil, err
			} else {
				break
			}
		}

		results[metricUUID] = item
	}

	if err := iterator.Close(); err != nil {
		return nil, err
	}

	var msPoints []types.MetricPoints

	for _, mPoint := range results {
		msPoints = append(msPoints, mPoint)
	}

	return msPoints, nil
}

// Returns an interpreter according to the parameters
func (c *Cassandra) readSQL(uuid gocql.UUID, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iterator := c.session.Query(
		"SELECT metric_uuid, base_ts, offset_ts, values FROM "+metricsTable+" "+
			"WHERE metric_uuid = ? AND base_ts IN (?, ?) AND offset_ts >= ? AND offset_ts <= ?",
		uuid, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	return iterator
}

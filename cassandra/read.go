package cassandra

import (
	"bytes"
	"encoding/binary"
	"hamsterdb/config"
	"hamsterdb/types"
	"io"
	"time"
)

func (c *Cassandra) Read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	uuid := metricUUID(mRequest.Metric)
	fromBaseTimestamp := mRequest.FromTime.Unix() - (mRequest.FromTime.Unix() % config.PartitionLength)
	toBaseTimestamp := mRequest.ToTime.Unix() - (mRequest.ToTime.Unix() % config.PartitionLength)
	fromOffsetTimestamp := mRequest.FromTime.Unix() - fromBaseTimestamp
	toOffsetTimestamp := mRequest.ToTime.Unix() - toBaseTimestamp

	iterator := c.session.Query(
		"SELECT metric_uuid, base_ts, offset_ts, values FROM "+metricsTable+" "+
			"WHERE metric_uuid = ? AND base_ts IN (?, ?) AND offset_ts >= ? AND offset_ts <= ?",
		uuid, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	var metricUUID string
	var baseTimestamp int64
	var offsetTimestamp int64
	var values []byte
	result := make(map[string]types.MetricPoints)

	for iterator.Scan(&metricUUID, &baseTimestamp, &offsetTimestamp, &values) {
		item, exists := result[metricUUID]

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
				// TODO: Handle error
				return nil, err
			} else {
				break
			}
		}

		result[metricUUID] = item
	}

	if err := iterator.Close(); err != nil {
		return nil, err
	}

	var msPoints []types.MetricPoints

	for _, value := range result {
		msPoints = append(msPoints, value)
	}

	return msPoints, nil
}

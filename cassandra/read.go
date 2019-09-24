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
	fromBaseTimestamp := mRequest.FromTime.Unix() - (mRequest.FromTime.Unix() % config.PartitionLength)
	toBaseTimestamp := mRequest.ToTime.Unix() - (mRequest.ToTime.Unix() % config.PartitionLength)
	fromOffsetTimestamp := mRequest.FromTime.Unix() - fromBaseTimestamp
	toOffsetTimestamp := mRequest.ToTime.Unix() - toBaseTimestamp

	iterator := c.session.Query(
		"SELECT metric_uuid, base_ts, offset_ts, values FROM "+metricsTable+" "+
			"WHERE metric_uuid = ? AND base_ts IN (?, ?) AND offset_ts >= ? AND offset_ts <= ?",
		MetricUUID(&mRequest.Metric), fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

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
		var err error

		for err != io.EOF {
			var data struct {
				PointTimestamp uint16
				PointValue     float64
			}

			err = binary.Read(buffer, binary.BigEndian, &data)

			if err == nil {
				pointTime := time.Unix(baseTimestamp+offsetTimestamp+int64(data.PointTimestamp), 0)

				if (mRequest.Step == 0 || ((int64(data.PointTimestamp) % mRequest.Step) == 0)) &&
					!pointTime.Before(mRequest.FromTime) && !pointTime.After(mRequest.ToTime) {

					point := types.Point{
						Time:  pointTime,
						Value: data.PointValue,
					}

					item.Points = append(item.Points, point)
				}
			} else if err != io.EOF {
				logger.Printf("Write: Can't read bytes (%v)"+"\n", err)
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

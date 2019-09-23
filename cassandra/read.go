package cassandra

import (
	"hamsterdb/config"
	"hamsterdb/types"
	"regexp"
	"strconv"
	"strings"
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
	var valuesBlob []byte
	result := make(map[string]types.MetricPoints)

	for iterator.Scan(&metricUUID, &baseTimestamp, &offsetTimestamp, &valuesBlob) {
		item, exists := result[metricUUID]

		if !exists {
			item = types.MetricPoints{
				Metric: mRequest.Metric,
			}
		}

		for _, element := range strings.Split(string(valuesBlob), ",") {
			regex := regexp.MustCompile(`([+-]?[0-9]+)=([+-]?[0-9]*[.]?[0-9]*)`)
			regexMatches := regex.FindStringSubmatch(element)

			subOffsetTimestamp, err := strconv.ParseInt(regexMatches[1], 10, 64)

			if err != nil {
				return nil, err
			}

			pointTime := time.Unix(baseTimestamp+offsetTimestamp+subOffsetTimestamp, 0)

			if (mRequest.Step == 0 || ((subOffsetTimestamp % mRequest.Step) == 0)) &&
				!pointTime.Before(mRequest.FromTime) && !pointTime.After(mRequest.ToTime) {
				value, err := strconv.ParseFloat(regexMatches[2], 64)

				if err != nil {
					return nil, err
				}

				point := types.Point{
					Time:  pointTime,
					Value: value,
				}

				item.Points = append(item.Points, point)
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

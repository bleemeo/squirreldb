package cassandra

import (
	"hamsterdb/config"
	"hamsterdb/types"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func toMetricPoints(labels map[string]string, points []types.Point) types.MetricPoints {
	mPoint := types.MetricPoints{
		Metric: types.Metric{Labels: labels},
		Points: points,
	}

	return mPoint
}

func (c *Cassandra) Read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	var msPoints []types.MetricPoints
	var points []types.Point
	var timestamp int64
	var offsetTimestamp int64
	var valuesBlob []byte

	fromTimestamp := mRequest.FromTime.Unix() - (mRequest.FromTime.Unix() % config.PartitionLength)
	toTimestamp := mRequest.ToTime.Unix() - (mRequest.ToTime.Unix() % config.PartitionLength)
	fromOffsetTimestamp := mRequest.FromTime.Unix() - fromTimestamp
	toOffsetTimestamp := mRequest.ToTime.Unix() - toTimestamp

	selectt := c.session.Query(
		"SELECT timestamp, offset_timestamp, values FROM "+metricsTable+" "+
			"WHERE metric_uuid = ? AND timestamp >= ? AND timestamp <= ? AND offset_timestamp >= ? AND offset_timestamp <= ? "+
			"ALLOW FILTERING",
		MetricUUID(&mRequest.Metric), fromTimestamp, toTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	for selectt.Scan(&timestamp, &offsetTimestamp, &valuesBlob) {
		for _, element := range strings.Split(string(valuesBlob), ",") {
			regex := regexp.MustCompile(`([+-]?[0-9]+)=([+-]?[0-9]*[.]?[0-9]*)`)
			regexMatches := regex.FindStringSubmatch(element)

			subOffsetTimestamp, err := strconv.ParseInt(regexMatches[1], 10, 64)

			if err != nil {
				return nil, err
			}

			pointTime := time.Unix(timestamp+offsetTimestamp+subOffsetTimestamp, 0)

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

				points = append(points, point)
			}
		}

		mPoints := toMetricPoints(mRequest.Labels, points)

		msPoints = append(msPoints, mPoints)
	}

	if err := selectt.Close(); err != nil {
		return nil, err
	}

	return msPoints, nil
}

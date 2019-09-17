package cassandra

import (
	"hamsterdb/types"
	"hamsterdb/util"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func toMetricPoints(labels map[string]string, points []types.Point) types.MetricPoints {
	// Create MetricPoints
	mPoint := types.MetricPoints{
		Metric: types.Metric{Labels: labels},
		Points: points,
	}

	return mPoint
}

func (cassandra *Cassandra) Read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	var msPoints []types.MetricPoints
	var points []types.Point
	var timestamp int64
	var valuesByte []byte

	// Create iterator with all entries in metrics table who matches with conditions
	selectQuery := cassandra.session.Query(
		"SELECT timestamp, values FROM "+metricsTable+" "+
			"WHERE labels = ? AND timestamp >= ? AND timestamp <= ?",
		mRequest.CanonicalLabels(), mRequest.FromTime.Unix(), mRequest.ToTime.Unix()).Iter()

	// Iterate on each entry
	for selectQuery.Scan(&timestamp, &valuesByte) {
		// Iterate on each data
		for _, data := range strings.Split(string(valuesByte), ",") {
			// Capture timestamp and value
			regex := regexp.MustCompile(`([+-]?[0-9]+)=([+-]?[0-9]*[.]?[0-9]*)`)
			regexMatches := regex.FindStringSubmatch(data)

			// Parse timestamp string match as an int
			offsetTimestamp, err := strconv.ParseInt(regexMatches[1], 10, 64)

			if err != nil {
				return nil, err
			}

			// Check if timestamp respects the request conditions
			if (mRequest.Step == 0 || ((offsetTimestamp % mRequest.Step) == 0)) && util.TimeBetween(
				time.Unix(timestamp+offsetTimestamp, 0), mRequest.FromTime, mRequest.ToTime) {
				// Parse value string match as a float
				value, err := strconv.ParseFloat(regexMatches[2], 64)

				if err != nil {
					return nil, err
				}

				// Create and append a Point to the Point array
				point := types.Point{
					Time:  time.Unix(timestamp+offsetTimestamp, 0),
					Value: value,
				}

				points = append(points, point)
			}
		}

		// Create MetricPoints array
		mPoints := toMetricPoints(mRequest.Labels, points)

		msPoints = append(msPoints, mPoints)
	}

	// Close iterator
	if err := selectQuery.Close(); err != nil {
		return nil, err
	}

	return msPoints, nil
}

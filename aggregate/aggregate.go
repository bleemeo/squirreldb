package aggregate

import (
	"math"
	"squirreldb/types"
)

type AggregatedPoint struct {
	Timestamp int64
	Min       float64
	Max       float64
	Average   float64
	Count     float64
}

type AggregatedPoints []AggregatedPoint

type AggregatedData struct {
	Points     AggregatedPoints
	TimeToLive int64
}

type AggregatedMetrics map[types.MetricUUID]AggregatedData

// Metrics returns AggregatedMetrics according to the parameters
func Metrics(metrics types.Metrics, fromTimestamp, toTimestamp, resolution int64) AggregatedMetrics {
	aggregatedMetrics := make(AggregatedMetrics)

	for uuid, metricData := range metrics {
		aggregatedMetrics[uuid] = MetricData(metricData, fromTimestamp, toTimestamp, resolution)
	}

	return aggregatedMetrics
}

// MetricData returns AggregatedData according to the parameters
func MetricData(metricData types.MetricData, fromTimestamp, toTimestamp, resolution int64) AggregatedData {
	aggregatedData := AggregatedData{
		TimeToLive: metricData.TimeToLive,
	}

	for timestamp := fromTimestamp; timestamp < toTimestamp; timestamp += resolution {
		var resolutionPoints types.MetricPoints

		for _, point := range metricData.Points {
			if (point.Timestamp >= timestamp) && (point.Timestamp < (timestamp + resolution)) {
				resolutionPoints = append(resolutionPoints, point)
			}
		}

		if len(resolutionPoints) != 0 {
			aggregatedPoint := aggregate(timestamp, resolutionPoints)

			aggregatedData.Points = append(aggregatedData.Points, aggregatedPoint)
		}
	}

	return aggregatedData
}

// Returns an AggregatedPoint from specified MetricPoints
// Calculations are: minimum, maximum, average and count of points
func aggregate(timestamp int64, points types.MetricPoints) AggregatedPoint {
	aggregatedPoint := AggregatedPoint{
		Timestamp: timestamp,
		Count:     float64(len(points)),
	}

	for i, point := range points {
		if i == 0 {
			aggregatedPoint.Min = point.Value
			aggregatedPoint.Max = point.Value
		} else {
			aggregatedPoint.Min = math.Min(point.Value, aggregatedPoint.Min)
			aggregatedPoint.Max = math.Max(point.Value, aggregatedPoint.Max)
		}

		aggregatedPoint.Average += point.Value
	}

	aggregatedPoint.Average /= aggregatedPoint.Count

	return aggregatedPoint
}

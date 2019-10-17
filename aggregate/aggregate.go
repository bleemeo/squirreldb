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

type AggregatedMetrics map[types.MetricUUID]AggregatedPoints

// Metrics returns AggregatedMetrics according to the parameters
func Metrics(metrics types.Metrics, fromTimestamp, toTimestamp, resolution int64) AggregatedMetrics {
	aggregatedMetrics := make(AggregatedMetrics)

	for uuid, points := range metrics {
		aggregatedMetrics[uuid] = MetricPoints(points, fromTimestamp, toTimestamp, resolution)
	}

	return aggregatedMetrics
}

// MetricPoints returns AggregatedPoints according to the parameters
func MetricPoints(points types.MetricPoints, fromTimestamp, toTimestamp, resolution int64) AggregatedPoints {
	var aggregatedPoints AggregatedPoints

	for timestamp := fromTimestamp; timestamp < toTimestamp; timestamp += resolution {
		var resolutionPoints types.MetricPoints

		for _, point := range points {
			if (point.Timestamp >= timestamp) && (point.Timestamp < (timestamp + resolution)) {
				resolutionPoints = append(resolutionPoints, point)
			}
		}

		if len(resolutionPoints) != 0 {
			aggregatedPoint := aggregate(timestamp, resolutionPoints)

			aggregatedPoints = append(aggregatedPoints, aggregatedPoint)
		}
	}

	return aggregatedPoints
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

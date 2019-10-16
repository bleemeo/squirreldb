package aggregate

import (
	"squirreldb/math"
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

// TODO: Comment
func Metrics(metrics types.Metrics, fromTimestamp, toTimestamp, step int64) AggregatedMetrics {
	aggregatedMetrics := make(AggregatedMetrics)

	for uuid, points := range metrics {
		aggregatedMetrics[uuid] = MetricPoints(points, fromTimestamp, toTimestamp, step)
	}

	return aggregatedMetrics
}

// TODO: Comment
func MetricPoints(points types.MetricPoints, fromTimestamp, toTimestamp, step int64) AggregatedPoints {
	var aggregatedPoints AggregatedPoints

	for timestamp := fromTimestamp; timestamp < toTimestamp; timestamp += step {
		var stepPoints types.MetricPoints

		for _, point := range points {
			if (point.Timestamp >= timestamp) && (point.Timestamp <= (timestamp + step)) {
				stepPoints = append(stepPoints, point)
			}
		}

		if len(stepPoints) != 0 {
			aggregatedPoint := aggregate(timestamp, stepPoints)

			aggregatedPoints = append(aggregatedPoints, aggregatedPoint)
		}
	}

	return aggregatedPoints
}

// TODO: Comment
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
			aggregatedPoint.Min = math.Float64Min(point.Value, aggregatedPoint.Min)
			aggregatedPoint.Max = math.Float64Max(point.Value, aggregatedPoint.Max)
		}

		aggregatedPoint.Average += point.Value
	}

	aggregatedPoint.Average /= aggregatedPoint.Count

	return aggregatedPoint
}

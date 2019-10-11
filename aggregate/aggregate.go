package aggregate

import "squirreldb/types"

type AggregatedPoint struct {
	Timestamp int64
	Min       float64
	Max       float64
	Average   float64
	Count     float64
}

func Calculate(timestamp int64, points types.MetricPoints) AggregatedPoint {
	aggregatedPoints := AggregatedPoint{
		Timestamp: timestamp,
		Count:     float64(len(points)),
	}

	for i, point := range points {
		if i == 0 {
			aggregatedPoints.Min = point.Value
			aggregatedPoints.Max = point.Value
		} else {
			if point.Value < aggregatedPoints.Min {
				aggregatedPoints.Min = point.Value
			} else if point.Value > aggregatedPoints.Max {
				aggregatedPoints.Max = point.Value
			}
		}

		aggregatedPoints.Average += point.Value
	}

	aggregatedPoints.Average /= aggregatedPoints.Count

	return aggregatedPoints
}

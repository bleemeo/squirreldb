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

type AggregatedData struct {
	ID         types.MetricID
	Points     []AggregatedPoint
	TimeToLive int64
}

// Aggregate aggregates data
func Aggregate(data types.MetricData, resolution int64) AggregatedData {
	if len(data.Points) == 0 {
		return AggregatedData{}
	}

	workingPoints := make([]types.MetricPoint, 0)

	var currentAggregatedTimestamp int64

	aggregatedData := AggregatedData{
		TimeToLive: data.TimeToLive,
		ID:         data.ID,
	}

	for i, point := range data.Points {
		aggregatedTimestamp := point.Timestamp - (point.Timestamp % resolution)

		if i == 0 {
			currentAggregatedTimestamp = aggregatedTimestamp
		} else if currentAggregatedTimestamp != aggregatedTimestamp {
			aggregatedPoint := aggregatePoints(workingPoints, currentAggregatedTimestamp)
			aggregatedData.Points = append(aggregatedData.Points, aggregatedPoint)
			workingPoints = workingPoints[:0]
			currentAggregatedTimestamp = aggregatedTimestamp
		}

		workingPoints = append(workingPoints, point)
	}

	if len(workingPoints) > 0 {
		aggregatedPoint := aggregatePoints(workingPoints, currentAggregatedTimestamp)
		aggregatedData.Points = append(aggregatedData.Points, aggregatedPoint)
	}

	return aggregatedData
}

// Returns an aggregated point from a point list
func aggregatePoints(points []types.MetricPoint, timestamp int64) AggregatedPoint {
	aggregatedPoint := AggregatedPoint{
		Timestamp: timestamp,
		Count:     float64(len(points)),
	}

	for i, point := range points {
		if i == 0 {
			aggregatedPoint.Min = point.Value
		}

		aggregatedPoint.Min = math.Min(point.Value, aggregatedPoint.Min)
		aggregatedPoint.Max = math.Max(point.Value, aggregatedPoint.Max)
		aggregatedPoint.Average += point.Value
	}

	aggregatedPoint.Average /= aggregatedPoint.Count

	return aggregatedPoint
}

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
	Points     []AggregatedPoint
	TimeToLive int64
}

// Aggregate returns an aggregated metric list from a metric list
// points must be sorted in ascending order
func Aggregate(metrics map[types.MetricUUID]types.MetricData, resolution int64) map[types.MetricUUID]AggregatedData {
	if len(metrics) == 0 {
		return nil
	}

	aggregatedMetrics := make(map[types.MetricUUID]AggregatedData)

	for uuid, data := range metrics {
		aggregatedData := aggregateData(data, resolution)

		aggregatedMetrics[uuid] = aggregatedData
	}

	return aggregatedMetrics
}

// Returns aggregated data from data
func aggregateData(data types.MetricData, resolution int64) AggregatedData {
	if len(data.Points) == 0 {
		return AggregatedData{}
	}

	workingPoints := make([]types.MetricPoint, 0)

	var currentAggregatedTimestamp int64

	aggregatedData := AggregatedData{
		TimeToLive: data.TimeToLive,
	}

	for _, point := range data.Points {
		aggregatedTimestamp := point.Timestamp - (point.Timestamp % resolution)
		if currentAggregatedTimestamp != aggregatedTimestamp {
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

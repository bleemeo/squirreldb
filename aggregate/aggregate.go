package aggregate

import (
	"math"
	"sort"
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

// PointsSort returns the AggregatedPoint list sorted by timestamp
func PointsSort(aggregatedPoints []AggregatedPoint) []AggregatedPoint {
	if len(aggregatedPoints) == 0 {
		return nil
	}

	sortedAggregatedPoints := make([]AggregatedPoint, len(aggregatedPoints))

	copy(sortedAggregatedPoints, aggregatedPoints)

	sort.Slice(sortedAggregatedPoints, func(i, j int) bool {
		return sortedAggregatedPoints[i].Timestamp < sortedAggregatedPoints[j].Timestamp
	})

	return sortedAggregatedPoints
}

// Returns aggregated data from data
func aggregateData(data types.MetricData, resolution int64) AggregatedData {
	if len(data.Points) == 0 {
		return AggregatedData{}
	}

	resolutionTimestampPoints := make(map[int64][]types.MetricPoint)

	for _, point := range data.Points {
		resolutionTimestamp := point.Timestamp - (point.Timestamp % resolution)

		resolutionTimestampPoints[resolutionTimestamp] = append(resolutionTimestampPoints[resolutionTimestamp], point)
	}

	aggregatedData := AggregatedData{
		TimeToLive: data.TimeToLive,
	}

	for resolutionTimestamp, points := range resolutionTimestampPoints {
		aggregatedPoint := aggregatePoints(points, resolutionTimestamp)

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

package aggregate

import (
	"math"
	"squirreldb/types"

	"github.com/prometheus/prometheus/model/value"
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
	ID         types.MetricID
	TimeToLive int64
}

// Aggregate aggregates data.
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
			aggregatedPoint, ok := aggregatePoints(workingPoints, currentAggregatedTimestamp)
			if ok {
				aggregatedData.Points = append(aggregatedData.Points, aggregatedPoint)
			}

			workingPoints = workingPoints[:0]
			currentAggregatedTimestamp = aggregatedTimestamp
		}

		workingPoints = append(workingPoints, point)
	}

	if len(workingPoints) > 0 {
		aggregatedPoint, ok := aggregatePoints(workingPoints, currentAggregatedTimestamp)
		if ok {
			aggregatedData.Points = append(aggregatedData.Points, aggregatedPoint)
		}
	}

	return aggregatedData
}

// Returns an aggregated point from a point list.
func aggregatePoints(points []types.MetricPoint, timestamp int64) (AggregatedPoint, bool) {
	aggregatedPoint := AggregatedPoint{
		Timestamp: timestamp,
		Count:     float64(len(points)),
	}

	count := 0

	for _, point := range points {
		if math.Float64bits(point.Value) == value.StaleNaN {
			continue
		}

		count++

		if count == 1 {
			aggregatedPoint.Min = point.Value
			aggregatedPoint.Max = point.Value
		}

		if point.Value < aggregatedPoint.Min || math.IsNaN(aggregatedPoint.Min) {
			aggregatedPoint.Min = point.Value
		}

		if point.Value > aggregatedPoint.Max || math.IsNaN(aggregatedPoint.Max) {
			aggregatedPoint.Max = point.Value
		}

		aggregatedPoint.Average += point.Value
	}

	if count == 0 {
		return aggregatedPoint, false
	}

	aggregatedPoint.Count = float64(count)
	aggregatedPoint.Average /= aggregatedPoint.Count

	if math.IsNaN(aggregatedPoint.Average) {
		aggregatedPoint.Average = math.Float64frombits(value.NormalNaN)
	}

	if math.IsNaN(aggregatedPoint.Min) {
		aggregatedPoint.Min = math.Float64frombits(value.NormalNaN)
	}

	if math.IsNaN(aggregatedPoint.Max) {
		aggregatedPoint.Max = math.Float64frombits(value.NormalNaN)
	}

	return aggregatedPoint, true
}

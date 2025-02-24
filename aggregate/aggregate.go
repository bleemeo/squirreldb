// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregate

import (
	"math"

	"github.com/bleemeo/squirreldb/types"

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
// The returned data keeps the same order as the input points.
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

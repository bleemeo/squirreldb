package types

import (
	"math/rand"
	"sort"

	"github.com/prometheus/prometheus/pkg/labels"
)

type MetricID int64

type MetricLabel struct {
	ID     MetricID
	Labels labels.Labels
}

type MetricPoint struct {
	Timestamp int64
	Value     float64
}

type MetricData struct {
	ID         MetricID
	Points     []MetricPoint
	TimeToLive int64
}

type MetricRequest struct {
	IDs           []MetricID
	FromTimestamp int64
	ToTimestamp   int64
	StepMs        int64
	Function      string
}

// CopyPoints returns a copy of points.
func CopyPoints(points []MetricPoint) []MetricPoint {
	if len(points) == 0 {
		return nil
	}

	copiedPoints := make([]MetricPoint, len(points))

	copy(copiedPoints, points)

	return copiedPoints
}

// DeduplicatePoints returns the MetricPoint list deduplicated and sorted by timestamp.
func DeduplicatePoints(points []MetricPoint) []MetricPoint {
	if len(points) <= 1 {
		return points
	}

	sortPoints(points)

	j := 0

	for i := 1; i < len(points); i++ {
		if points[j].Timestamp == points[i].Timestamp {
			continue
		}
		j++

		points[j] = points[i]
	}

	result := points[:j+1]

	return result
}

// sortPoints returns the MetricPoint list sorted by timestamp.
func sortPoints(points []MetricPoint) {
	if len(points) <= 1 {
		return
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
}

// MakePointsForTest generate a list a MetricPoint for testing
// It generate point from timestamp Tue Sep 17 07:42:44 UTC 2019 with 10 seconds
// between each points.
func MakePointsForTest(size int) []MetricPoint {
	return MakePointsForTestOffset(size, 0)
}

// MakePointsForTestOffset is like MakePointsForTest but include a timestamp offset.
func MakePointsForTestOffset(size int, offsetMillisecond int64) []MetricPoint {
	result := make([]MetricPoint, size)
	for i := 0; i < size; i++ {
		result[i].Timestamp = int64(1568706164+i*10)*1000 + offsetMillisecond
		result[i].Value = float64(i)
	}

	return result
}

// MakeMetricDataForTest generate a list a MetricData for testing.
func MakeMetricDataForTest(countMetric int, countPoints int, offsetMillisecond int64) []MetricData {
	result := make([]MetricData, countMetric)
	for i := range result {
		result[i].ID = MetricID(100 + i)
		result[i].TimeToLive = 86400
		result[i].Points = MakePointsForTestOffset(countPoints, offsetMillisecond)
	}

	return result
}

// AddDuplicateForTest add duplicate points to a list of MetricPoint for testing.
func AddDuplicateForTest(input []MetricPoint, numberDuplicate int) []MetricPoint {
	duplicates := make([]int, numberDuplicate)
	for i := 0; i < numberDuplicate; i++ {
		duplicates[i] = rand.Intn(len(input)) // nolint: gosec
	}
	sort.Ints(duplicates)

	result := make([]MetricPoint, len(input)+numberDuplicate)

	inputIndex := 0
	duplicatesIndex := 0

	for i := 0; i < len(input)+numberDuplicate; i++ {
		result[i] = input[inputIndex]

		if duplicatesIndex < len(duplicates) && inputIndex == duplicates[duplicatesIndex] {
			duplicatesIndex++
		} else {
			inputIndex++
		}
	}

	if duplicatesIndex != len(duplicates) || inputIndex != len(input) {
		panic("Unexpected value for inputIndex or duplicatesIndex")
	}

	return result
}

// ShuffleForTest shuffle a list of MetricPoint for testing.
func ShuffleForTest(input []MetricPoint) []MetricPoint {
	rand.Shuffle(len(input), func(i, j int) {
		input[i], input[j] = input[j], input[i]
	})

	return input
}

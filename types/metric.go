package types

import (
	gouuid "github.com/gofrs/uuid"

	"math/big"
	"math/rand"
	"sort"
	"strings"
)

type MetricPoint struct {
	Timestamp int64
	Value     float64
}

type MetricData struct {
	Points     []MetricPoint
	TimeToLive int64
}

type MetricLabel struct {
	Name  string
	Value string
}

type MetricLabelMatcher struct {
	MetricLabel
	Type uint8
}

type MetricRequest struct {
	UUIDs         []gouuid.UUID
	FromTimestamp int64
	ToTimestamp   int64
	Step          int64
	Function      string
}

// UintFromUUID returns an uint64 from the UUID
func UintFromUUID(uuid gouuid.UUID) uint64 {
	bigInt := big.NewInt(0).SetBytes(uuid.Bytes())

	return bigInt.Uint64()
}

// CopyLabels returns a copy of labels
func CopyLabels(labels []MetricLabel) []MetricLabel {
	if len(labels) == 0 {
		return nil
	}

	copiedLabels := make([]MetricLabel, len(labels))

	copy(copiedLabels, labels)

	return copiedLabels
}

// CopyPoints returns a copy of points
func CopyPoints(points []MetricPoint) []MetricPoint {
	if len(points) == 0 {
		return nil
	}

	copiedPoints := make([]MetricPoint, len(points))

	copy(copiedPoints, points)

	return copiedPoints
}

// DeduplicatePoints returns the MetricPoint list deduplicated and sorted by timestamp
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

// DeleteLabelsValue deletes value via its name from a MetricLabel list
func DeleteLabelsValue(labels *[]MetricLabel, key string) {
	for i, label := range *labels {
		if label.Name == key {
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
			return
		}
	}
}

// GetLabelsValue gets value via its name from a MetricLabel list
func GetLabelsValue(labels []MetricLabel, name string) (string, bool) {
	for _, label := range labels {
		if label.Name == name {
			return label.Value, true
		}
	}

	return "", false
}

// GetMatchersValue gets value via its name from a MetricLabel list
func GetMatchersValue(matchers []MetricLabelMatcher, name string) (string, bool) {
	for _, matcher := range matchers {
		if matcher.Name == name {
			return matcher.Value, true
		}
	}

	return "", false
}

// SortLabels returns the MetricLabel list sorted by name
func SortLabels(labels []MetricLabel) []MetricLabel {
	if len(labels) == 0 {
		return nil
	}

	sortedLabels := CopyLabels(labels)

	sort.Slice(sortedLabels, func(i, j int) bool {
		return sortedLabels[i].Name < sortedLabels[j].Name
	})

	return sortedLabels
}

// sortPoints returns the MetricPoint list sorted by timestamp
func sortPoints(points []MetricPoint) {
	if len(points) <= 1 {
		return
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
}

// StringFromLabels returns a string generated from a MetricLabel list
func StringFromLabels(labels []MetricLabel) string {
	if len(labels) == 0 {
		return ""
	}

	strLabels := make([]string, 0, len(labels))
	quoter := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

	for _, label := range labels {
		str := label.Name + "=\"" + quoter.Replace(label.Value) + "\""

		strLabels = append(strLabels, str)
	}

	str := strings.Join(strLabels, ",")

	return str
}

// MakePointsForTest generate a list a MetricPoint for testing
// It generate point from timestamp Tue Sep 17 07:42:44 UTC 2019 with 10 seconds
// between each points.
func MakePointsForTest(size int) []MetricPoint {
	result := make([]MetricPoint, size)
	for i := 0; i < size; i++ {
		result[i].Timestamp = int64(1568706164 + i*10)
		result[i].Value = float64(i)
	}

	return result
}

// AddDuplicateForTest add duplicate points to a list of MetricPoint for testing
func AddDuplicateForTest(input []MetricPoint, numberDuplicate int) []MetricPoint {
	duplicates := make([]int, numberDuplicate)
	for i := 0; i < numberDuplicate; i++ {
		duplicates[i] = rand.Intn(len(input))
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

// ShuffleForTest shuffle a list of MetricPoint for testing
func ShuffleForTest(input []MetricPoint) []MetricPoint {
	rand.Shuffle(len(input), func(i, j int) {
		input[i], input[j] = input[j], input[i]
	})

	return input
}

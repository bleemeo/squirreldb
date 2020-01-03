package types

import (
	gouuid "github.com/gofrs/uuid"

	"math/big"
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

type MetricUUID struct {
	gouuid.UUID
}

type MetricRequest struct {
	UUIDs         []MetricUUID
	FromTimestamp int64
	ToTimestamp   int64
	Step          int64
	Function      string
}

// Uint64 returns an uint64 generated from the UUID
func (m *MetricUUID) Uint64() uint64 {
	bigInt := big.NewInt(0).SetBytes(m.Bytes())

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
	if len(points) == 0 {
		return nil
	}

	sortedPoints := SortPoints(points)

	i := 0

	for j, length := 0, len(sortedPoints); j < length; j++ {
		if sortedPoints[i].Timestamp == sortedPoints[j].Timestamp {
			continue
		}

		i++

		sortedPoints[i] = sortedPoints[j]
	}

	deduplicatedPoints := sortedPoints[:(i + 1)]

	return deduplicatedPoints
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

// EqualLabels returns a boolean defined by the equality of the specified MetricLabel
func EqualLabels(labels, reference []MetricLabel) bool {
	if len(labels) != len(reference) {
		return false
	}

	for i := range labels {
		if labels[i] != reference[i] {
			return false
		}
	}

	return true
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

// LabelsFromMatchers returns a list of MetricLabel generated from a MetricLabelMatcher list
func LabelsFromMatchers(matchers []MetricLabelMatcher) []MetricLabel {
	if len(matchers) == 0 {
		return nil
	}

	labels := make([]MetricLabel, 0, len(matchers))

	for _, matcher := range matchers {
		labels = append(labels, matcher.MetricLabel)
	}

	return labels
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

// SortPoints returns the MetricPoint list sorted by timestamp
func SortPoints(points []MetricPoint) []MetricPoint {
	if len(points) == 0 {
		return nil
	}

	sortedPoints := CopyPoints(points)

	sort.Slice(sortedPoints, func(i, j int) bool {
		return sortedPoints[i].Timestamp < sortedPoints[j].Timestamp
	})

	return sortedPoints
}

// StringFromLabels returns a string generated from a MetricLabel list
func StringFromLabels(labels []MetricLabel) string {
	if len(labels) == 0 {
		return ""
	}

	strLabels := make([]string, 0, len(labels))

	for _, label := range labels {
		str := label.Name + ":" + label.Value

		strLabels = append(strLabels, str)
	}

	str := strings.Join(strLabels, ",")

	return str
}

// UUIDFromString returns a MetricUUID generated from a string
func UUIDFromString(s string) (MetricUUID, error) {
	u, err := gouuid.FromString(s)

	if err != nil {
		return MetricUUID{}, err
	}

	uuid := MetricUUID{
		UUID: u,
	}

	return uuid, nil
}

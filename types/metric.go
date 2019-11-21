package types

import (
	gouuid "github.com/gofrs/uuid"
	"math/big"
	"sort"
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

// LabelsContains returns a bool and a string
func LabelsContains(labels []MetricLabel, name string) (bool, string) {
	if len(labels) == 0 {
		return false, ""
	}

	for _, label := range labels {
		if label.Name == name {
			return true, label.Value
		}
	}

	return false, ""
}

// LabelsEqual returns a boolean defined by the equality of the specified MetricLabel
func LabelsEqual(labels, reference []MetricLabel) bool {
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

// LabelsFromMatchers returns a list of MetricLabel generated from a list of MetricLabelMatcher
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

// LabelsFromMap returns a list of MetricLabel generated from a map
func LabelsFromMap(m map[string]string) []MetricLabel {
	if len(m) == 0 {
		return nil
	}

	labels := make([]MetricLabel, 0, len(m))

	for name, value := range m {
		label := MetricLabel{
			Name:  name,
			Value: value,
		}

		labels = append(labels, label)
	}

	return labels
}

// LabelsSort returns the MetricLabel list sorted by name
func LabelsSort(labels []MetricLabel) []MetricLabel {
	if len(labels) == 0 {
		return nil
	}

	sortedLabels := make([]MetricLabel, len(labels))

	copy(sortedLabels, labels)

	sort.Slice(sortedLabels, func(i, j int) bool {
		return sortedLabels[i].Name < sortedLabels[j].Name
	})

	return sortedLabels
}

// MapFromLabels returns a map generated from a MetricLabel list
func MapFromLabels(labels []MetricLabel) map[string]string {
	if len(labels) == 0 {
		return nil
	}

	m := make(map[string]string, len(labels))

	for _, label := range labels {
		m[label.Name] = label.Value
	}

	return m
}

// PointsDeduplicate returns the MetricPoint list deduplicated and sorted by timestamp
func PointsDeduplicate(points []MetricPoint) []MetricPoint {
	if len(points) == 0 {
		return nil
	}

	sortedPoints := PointsSort(points)

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

// PointsSort returns the MetricPoint list sorted by timestamp
func PointsSort(points []MetricPoint) []MetricPoint {
	if len(points) == 0 {
		return nil
	}

	sortedPoints := make([]MetricPoint, len(points))

	copy(sortedPoints, points)

	sort.Slice(sortedPoints, func(i, j int) bool {
		return sortedPoints[i].Timestamp < sortedPoints[j].Timestamp
	})

	return sortedPoints
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

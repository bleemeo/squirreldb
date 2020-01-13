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
func (m MetricUUID) Uint64() uint64 {
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

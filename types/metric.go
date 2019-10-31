package types

import (
	gouuid "github.com/gofrs/uuid"
	"sort"
	"strings"
)

const (
	LabelTypeEq        = 0
	LabelTypeNeq       = 1
	LabelTypeRe        = 2
	LabelTypeNre       = 3
	LabelPrefixSpecial = "__bleemeo_"
)

type MetricPoint struct {
	Timestamp int64
	Value     float64
}

type MetricPoints []MetricPoint

type MetricData struct {
	Points     MetricPoints
	TimeToLive int64
}

type MetricLabel struct {
	Name  string
	Value string
	Type  uint8
}

type MetricLabels []MetricLabel

type MetricUUID struct {
	gouuid.UUID
}

type MetricUUIDs []MetricUUID

type Metrics map[MetricUUID]MetricData

type MetricRequest struct {
	UUIDs         MetricUUIDs
	FromTimestamp int64
	ToTimestamp   int64
	Step          int64
	Function      string
}

// Deduplicate returns a sorted and deduplicated list of point
func (m MetricPoints) Deduplicate() MetricPoints {
	if len(m) == 0 {
		return m
	}

	sort.Slice(m, func(i, j int) bool {
		return m[i].Timestamp < m[j].Timestamp
	})

	i := 0

	for j := 0; j < len(m); j++ {
		if m[i].Timestamp == m[j].Timestamp {
			continue
		}

		i++

		m[i] = m[j]
	}

	result := m[:(i + 1)]

	return result
}

// Canonical returns string from labels
func (m MetricLabels) Canonical() string {
	sort.Slice(m, func(i, j int) bool {
		return m[i].Name < m[j].Name
	})

	elements := make([]string, 0, len(m))

	for _, label := range m {
		if !strings.HasPrefix(label.Name, LabelPrefixSpecial) {
			value := strings.ReplaceAll(label.Value, `"`, `\"`)
			element := label.Name + `="` + value + `"`

			elements = append(elements, element)
		}
	}

	canonical := strings.Join(elements, ",")

	return canonical
}

// Map returns labels as a map
func (m MetricLabels) Map() map[string]string {
	res := make(map[string]string, len(m))

	for _, label := range m {
		res[label.Name] = label.Value
	}

	return res
}

// Value returns value corresponding to specified label name
func (m MetricLabels) Value(name string) (string, bool) {
	for _, label := range m {
		if label.Name == name {
			return label.Value, true
		}
	}

	return "", false
}

// LabelsFromMap returns labels generated from a map
func LabelsFromMap(m map[string]string) MetricLabels {
	labels := make(MetricLabels, 0, len(m))

	for name, value := range m {
		label := MetricLabel{
			Name:  name,
			Value: value,
		}

		labels = append(labels, label)
	}

	return labels
}

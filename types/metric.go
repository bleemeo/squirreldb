package types

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Metric struct {
	Labels map[string]string
}

type Point struct {
	Time  time.Time
	Value float64
}

type MetricPoints struct {
	Metric
	Points []Point
}

type MetricRequest struct {
	Metric
	FromTime time.Time
	ToTime   time.Time
	Step     int64
}

func (m *Metric) CanonicalLabels() string {
	var keys []string
	var elements []string

	for key := range m.Labels {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		value := strings.Replace(m.Labels[key], `"`, `\"`, -1)
		element := fmt.Sprintf(`%s="%s"`, key, value)

		elements = append(elements, element)
	}

	canonical := strings.Join(elements, ",")

	return canonical
}

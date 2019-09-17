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

func (metric *Metric) CanonicalLabels() string {
	var canonicalLabels string
	var keys []string

	for key := range metric.Labels {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for i, key := range keys {
		value := strings.Replace(metric.Labels[key], `"`, `\"`, -1)

		if i == 0 {
			canonicalLabels = fmt.Sprintf("%s=\"%s\"", key, value)
		} else {
			canonicalLabels = fmt.Sprintf("%s,%s=\"%s\"", canonicalLabels, key, value)
		}
	}

	return canonicalLabels
}

func (mPoints *MetricPoints) AddPoints(points []Point) {
	mPoints.Points = append(mPoints.Points, points...)
}

func (mPoints *MetricPoints) RemovePoints(pointsTime []time.Time) {
	for _, pointTime := range pointsTime {
		for i := 0; i < len(mPoints.Points); i++ {
			if mPoints.Points[i].Time.Equal(pointTime) {
				mPoints.Points = append(mPoints.Points[:i], mPoints.Points[i+1:]...)
				i -= 1
			}
		}
	}
}

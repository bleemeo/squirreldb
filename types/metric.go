package types

import (
	"crypto/md5"
	"github.com/gofrs/uuid"
	"math/big"
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
	uuid.UUID
}

type Metrics map[MetricUUID]MetricData

type MetricRequest struct {
	UUIDs         []MetricUUID
	FromTimestamp int64
	ToTimestamp   int64
	Step          int64
	Function      string
}

func (m MetricPoints) SortUnify() MetricPoints {
	if len(m) <= 0 {
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

// UUID returns generated UUID from labels
// If a UUID label is specified, the UUID will be generated from its value
func (m MetricLabels) UUID() MetricUUID {
	uuidString, exists := m.Value("__bleemeo_uuid__")
	var metricUUID MetricUUID

	if exists {
		metricUUID.UUID = uuid.FromStringOrNil(uuidString)
	} else {
		canonical := m.Canonical()

		metricUUID.UUID = md5.Sum([]byte(canonical))
	}

	return metricUUID
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

// Uint64 returns uint64 from the UUID value
func (m *MetricUUID) Uint64() uint64 {
	bigInt := big.NewInt(0).SetBytes(m.Bytes())

	uuidUint64 := bigInt.Uint64()

	return uuidUint64
}

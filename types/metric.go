package types

import (
	"crypto/md5"
	"github.com/gofrs/uuid"
	"math/big"
	"sort"
	"squirreldb/config"
	"strings"
)

const (
	MetricLabelTypeEq  = 0
	MetricLabelTypeNeq = 1
	MetricLabelTypeRe  = 2
	MetricLabelTypeNre = 3
)

type MetricPoint struct {
	Timestamp int64
	Value     float64
}

type MetricPoints []MetricPoint

type MetricLabel struct {
	Name  string
	Value string
	Type  uint8
}

type MetricLabels []MetricLabel

type MetricUUID struct {
	uuid.UUID
}

type Metrics map[MetricUUID]MetricPoints

type MetricRequest struct {
	UUIDs         []MetricUUID
	FromTimestamp int64
	ToTimestamp   int64
	Step          int64
	Function      string
}

// Canonical returns string from labels
func (m MetricLabels) Canonical() string {
	sort.Slice(m, func(i, j int) bool {
		return m[i].Name < m[j].Name
	})

	elements := make([]string, 0, len(m))

	for _, label := range m {
		if !strings.HasPrefix(label.Name, config.LabelSpecialPrefix) {
			value := strings.ReplaceAll(label.Value, `"`, `\"`)
			element := label.Name + `="` + value + `"`

			elements = append(elements, element)
		}
	}

	canonical := strings.Join(elements, ",")

	return canonical
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

// UUID returns generated UUID from labels
// If a UUID label is specified, the UUID will be generated from its value
func (m MetricLabels) UUID() MetricUUID {
	value, exists := m.Value("__bleemeo_uuid__")
	var mUUID MetricUUID

	if exists {
		mUUID.UUID = uuid.FromStringOrNil(value)
	} else {
		canonical := m.Canonical()

		mUUID.UUID = md5.Sum([]byte(canonical))
	}

	return mUUID
}

// Uint64 returns uint64 from the UUID value
func (m *MetricUUID) Uint64() uint64 {
	bigInt := big.NewInt(0).SetBytes(m.Bytes())

	mUint64 := bigInt.Uint64()

	return mUint64
}

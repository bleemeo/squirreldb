package types

import (
	"crypto/md5"
	"github.com/gofrs/uuid"
	"math/big"
	"sort"
	"strings"
)

type MetricPoint struct {
	Timestamp int64
	Value     float64
}

type MetricData struct {
	Points []MetricPoint
}

type MetricLabels map[string]string

type MetricUUID struct {
	uuid.UUID
}

type MetricRequest struct {
	UUIDs         []MetricUUID
	FromTimestamp int64
	ToTimestamp   int64
	Step          int64
}

// Canonical returns string from labels
func (m MetricLabels) Canonical() string {
	count := len(m)
	keys := make([]string, 0, count)

	for key := range m {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	elements := make([]string, 0, count)

	for _, key := range keys {
		value := strings.ReplaceAll(m[key], `"`, `\"`)
		element := key + `="` + value + `"`

		elements = append(elements, element)
	}

	canonical := strings.Join(elements, ",")

	return canonical
}

// UUID returns generated UUID from labels
// If a UUID label is specified, the UUID will be generated from its value
func (m MetricLabels) UUID() MetricUUID {
	value, exists := m["__uuid__"]
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

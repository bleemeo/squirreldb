package types

import (
	"crypto/md5"
	"fmt"
	"github.com/gofrs/uuid"
	"math/big"
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

type MetricUUID struct {
	uuid.UUID
}

func (m *Metric) CanonicalLabels() string {
	var keys []string
	var elements []string

	for key := range m.Labels {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		value := strings.ReplaceAll(m.Labels[key], `"`, `\"`)
		element := fmt.Sprintf(`%s="%s"`, key, value)

		elements = append(elements, element)
	}

	canonical := strings.Join(elements, ",")

	return canonical
}

func (m *Metric) UUID() (MetricUUID, error) {
	var mUUID MetricUUID
	var err error
	uuidLabel, exists := m.Labels["__uuid__"]

	if !exists {
		hash := md5.New()
		canonical := m.CanonicalLabels()

		_, err := hash.Write([]byte(canonical))

		if err != nil {
			return mUUID, err
		}

		hashed := hash.Sum(nil)

		mUUID.UUID, err = uuid.FromBytes(hashed)

		if err != nil {
			return mUUID, err
		}
	} else {
		mUUID.UUID, err = uuid.FromString(uuidLabel)

		if err != nil {
			return mUUID, err
		}
	}

	return mUUID, nil
}

func (m *MetricUUID) Int64() int64 {
	bigInt := big.NewInt(0).SetBytes(m.Bytes())

	bigInt.SetInt64(bigInt.Int64())
	bigInt.Abs(bigInt)

	return bigInt.Int64()
}

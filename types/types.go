package types

import (
	"sync"

	"github.com/prometheus/prometheus/prompb"
)

type Index interface {
	AllIDs() ([]MetricID, error)
	LookupLabels(id MetricID) ([]prompb.Label, error)
	LookupIDs(labelsList [][]prompb.Label) ([]MetricID, []int64, error)
	Search(matchers []*prompb.LabelMatcher) ([]MetricID, error)
}

type MetricReader interface {
	Read(request MetricRequest) (map[MetricID]MetricData, error)
}

type MetricWriter interface {
	Write(metrics []MetricData) error
}

// TryLocker is a Locker with an additional TryLock() method
type TryLocker interface {
	sync.Locker
	// TryLock try to acquire a lock but return false if unable to acquire it.
	TryLock() bool
}

type State interface {
	Read(name string, value interface{}) (bool, error)
	Write(name string, value interface{}) error
}

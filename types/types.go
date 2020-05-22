package types

import (
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
)

type Index interface {
	AllIDs() ([]MetricID, error)
	LookupLabels(id MetricID) (labels.Labels, error)
	LookupIDs(labelsList []labels.Labels) ([]MetricID, []int64, error)
	Search(matchers []*labels.Matcher) ([]MetricID, error)
}

type MetricDataSet interface {
	Next() bool
	At() MetricData
	Err() error
}

type MetricReader interface {
	ReadIter(request MetricRequest) (MetricDataSet, error)
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

package types

import "sync"

type Index interface {
	AllUUIDs() ([]MetricUUID, error)
	LookupLabels(uuid MetricUUID) ([]MetricLabel, error)
	LookupUUID(labels []MetricLabel) (MetricUUID, error)
	Search(matchers []MetricLabelMatcher) ([]MetricUUID, error)
}

type MetricReader interface {
	Read(request MetricRequest) (map[MetricUUID]MetricData, error)
}

type MetricWriter interface {
	Write(metrics map[MetricUUID]MetricData) error
}

// TryLocker is a Locker with an additional TryLock() method
type TryLocker interface {
	sync.Locker
	// TryLock try to acquire a lock but return false if unable to acquire it.
	TryLock() bool
}

type State interface {
	Read(name string, value interface{}) (bool, error)
	Update(name string, value interface{}) error
	Write(name string, value interface{}) error
}

type Instance struct {
	Hostname string
	UUID     string
}

package types

import (
	"sync"

	gouuid "github.com/gofrs/uuid"
)

type Index interface {
	AllUUIDs() ([]gouuid.UUID, error)
	LookupLabels(uuid gouuid.UUID) ([]MetricLabel, error)
	LookupUUID(labels []MetricLabel) (gouuid.UUID, int64, error)
	Search(matchers []MetricLabelMatcher) ([]gouuid.UUID, error)
}

type MetricReader interface {
	Read(request MetricRequest) (map[gouuid.UUID]MetricData, error)
}

type MetricWriter interface {
	Write(metrics map[gouuid.UUID]MetricData) error
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

package types

import (
	"sync"

	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"
)

type Index interface {
	AllUUIDs() ([]gouuid.UUID, error)
	LookupLabels(uuid gouuid.UUID) ([]*prompb.Label, error)
	LookupUUID(labels []*prompb.Label) (gouuid.UUID, int64, error)
	Search(matchers []*prompb.LabelMatcher) ([]gouuid.UUID, error)
}

type MetricReader interface {
	Read(request MetricRequest) (map[gouuid.UUID]MetricData, error)
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

type Instance struct {
	Hostname string
	UUID     string
}

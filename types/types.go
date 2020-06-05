package types

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
)

// Task is a background worked that will be running until ctx is cancelled.
// If readiness is not nil, when ready the task send one nil.
// If an error occure, the task will send the error on the channel and return.
// You are allowed to re-call Run() if an error is returned.
type Task interface {
	Run(ctx context.Context, readiness chan error)
}

type TaskFun func(ctx context.Context, readiness chan error)

func (f TaskFun) Run(ctx context.Context, readiness chan error) {
	f(ctx, readiness)
}

type Memberlist interface {
	// Addresses return a list of addresse to reach API of SquirrelDB that are in our cluster.
	// It also return which index in the list is self.
	// This list (once memberlist has converged) will be the same on everyone, including the order.
	Nodes() []Node
	Send(n Node, requestType uint8, data []byte) ([]byte, error)
	SetRequestHandler(func(requestType uint8, data []byte) ([]byte, error))
}

type Node interface {
	APIAddress() string
	ClusterAddress() string
	IsSelf() bool
}

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

type MetricReadWriter interface {
	MetricReader
	MetricWriter
}

// TryLocker is a Locker with an additional TryLock() method.
type TryLocker interface {
	sync.Locker
	// TryLock try to acquire a lock but return false if unable to acquire it.
	TryLock() bool
}

type State interface {
	Read(name string, value interface{}) (bool, error)
	Write(name string, value interface{}) error
}

type WalStore interface {
	Write(metrics []MetricData) error
	Checkpoint() WalCheckpoint
	ReadWAL() ([]MetricData, error)
	Flush()
}

type WalCheckpoint interface {
	Abort()
	Purge()
	ReadOther() ([]MetricData, error)
}

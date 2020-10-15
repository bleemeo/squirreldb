package types

import (
	"context"
	"sync"
	"time"

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

type Cluster interface {
	// Addresses return a list of addresse to reach API of SquirrelDB that are in our cluster.
	// It also return which index in the list is self.
	// This list (once memberlist has converged) will be the same on everyone, including the order.
	Nodes() []Node
	Send(n Node, requestType uint8, data []byte) ([]byte, error)
	SetRequestHandler(func(ctx context.Context, requestType uint8, data []byte) ([]byte, error))
}

type Node interface {
	APIAddress() string
	ClusterAddress() string
	IsSelf() bool
}

type LookupRequest struct {
	Labels labels.Labels
	Start  time.Time
	End    time.Time
}

type Index interface {
	AllIDs(start time.Time, end time.Time) ([]MetricID, error)
	LookupIDs(ctx context.Context, requests []LookupRequest) ([]MetricID, []int64, error)
	Search(start time.Time, end time.Time, matchers []*labels.Matcher) ([]MetricLabel, error)
}

type MetricDataSet interface {
	Next() bool
	At() MetricData
	Err() error
}

type MetricReader interface {
	ReadIter(ctx context.Context, request MetricRequest) (MetricDataSet, error)
}

type MetricWriter interface {
	Write(ctx context.Context, metrics []MetricData) error
}

type MetricReadWriter interface {
	MetricReader
	MetricWriter
}

// TryLocker is a Locker with an additional TryLock() method.
type TryLocker interface {
	sync.Locker

	// TryLock try to acquire a lock but return false if unable to acquire it.
	TryLock(ctx context.Context, retryDelay time.Duration) bool
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

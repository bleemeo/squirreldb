package types

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

// OldTask is a background worked that will be running until ctx is cancelled.
// If readiness is not nil, when ready the task send one nil.
// If an error occure, the task will send the error on the channel and return.
// You are allowed to re-call Run() if an error is returned.
type OldTask interface {
	Run(ctx context.Context, readiness chan error)
}

type TaskFun func(ctx context.Context, readiness chan error)

func (f TaskFun) Run(ctx context.Context, readiness chan error) {
	f(ctx, readiness)
}

// Task is a background worker that will be running until Stop() is called
// If Start() fail, the worker isn't running, but Start() could be retried.
// Start() could after worker is running and will do nothing and return nil.
// Stop() will shutdown and wait for shutdown before returning.
// Start() & Stop() could be be called concurrently.
type Task interface {
	Start() error
	Stop() error
}

type LookupRequest struct {
	Labels labels.Labels
	Start  time.Time
	End    time.Time
}

type Index interface {
	AllIDs(start time.Time, end time.Time) ([]MetricID, error)
	LookupIDs(ctx context.Context, requests []LookupRequest) ([]MetricID, []int64, error)
	Search(start time.Time, end time.Time, matchers []*labels.Matcher) (MetricsSet, error)
}

type MetricsSet interface {
	Next() bool
	At() MetricLabel
	Err() error
	Count() int
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

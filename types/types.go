package types

import (
	"context"
	"net/http"
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
// Start() called after worker is running and will do nothing and return nil.
// Stop() will shutdown and wait for shutdown before returning.
// Start() & Stop() could be be called concurrently.
// The context for Start() is should only be used for start itself. The worker
// will continue to run even if context is cancelled.
type Task interface {
	Start(ctx context.Context) error
	Stop() error
}

type Cluster interface {
	// Publish sends a message that will be received by all subscribed nodes including sender.
	Publish(ctx context.Context, topic string, message []byte) error
	Subscribe(topic string, callback func([]byte))
	Close() error
}

type LookupRequest struct {
	Start  time.Time
	End    time.Time
	Labels labels.Labels
}

type Index interface {
	AllIDs(ctx context.Context, start time.Time, end time.Time) ([]MetricID, error)
	LookupIDs(ctx context.Context, requests []LookupRequest) ([]MetricID, []int64, error)
	Search(ctx context.Context, start time.Time, end time.Time, matchers []*labels.Matcher) (MetricsSet, error)
	LabelValues(ctx context.Context, start, end time.Time, name string, matchers []*labels.Matcher) ([]string, error)
	LabelNames(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) ([]string, error)
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

// RequestContextKey is used as a key in a context to a HTTP request.
type RequestContextKey struct{}

// WrapContext adds a request to the context.
func WrapContext(ctx context.Context, r *http.Request) context.Context {
	return context.WithValue(ctx, RequestContextKey{}, r)
}

package types

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

// OldTask is a background worked that will be running until ctx is cancelled.
// If readiness is not nil, when ready the task send one nil.
// If an error occur, the task will send the error on the channel and return.
// You are not allowed to re-call Run() if an error is returned.
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
// Start() & Stop() can be called concurrently.
// The context for Start() should only be used for start itself. The worker
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
	Start      time.Time
	End        time.Time
	Labels     labels.Labels
	TTLSeconds int64
}

type Index interface {
	AllIDs(ctx context.Context, start time.Time, end time.Time) ([]MetricID, error)
	LookupIDs(ctx context.Context, requests []LookupRequest) ([]MetricID, []int64, error)
	Search(ctx context.Context, start time.Time, end time.Time, matchers []*labels.Matcher) (MetricsSet, error)
	LabelValues(ctx context.Context, start, end time.Time, name string, matchers []*labels.Matcher) ([]string, error)
	LabelNames(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) ([]string, error)
}

type IndexDumper interface {
	InfoGlobal(ctx context.Context, w io.Writer) error
	InfoByID(ctx context.Context, w io.Writer, id MetricID) error
	Dump(ctx context.Context, w io.Writer) error
	DumpByLabels(ctx context.Context, w io.Writer, start, end time.Time, matchers []*labels.Matcher) error
	DumpByExpirationDate(ctx context.Context, w io.Writer, expirationDate time.Time) error
	DumpByShard(ctx context.Context, w io.Writer, shard time.Time) error
	DumpByPosting(ctx context.Context, w io.Writer, shard time.Time, name string, value string) error
}

type IndexBlocker interface {
	BlockCassandraWrite(context.Context) error
	UnblockCassandraWrite(context.Context) error
}

type VerifiableIndex interface {
	Verifier(w io.Writer) IndexVerifier
}

type IndexVerifier interface {
	WithNow(now time.Time) IndexVerifier
	WithDoFix(enable bool) IndexVerifier
	WithLock(enable bool) IndexVerifier
	WithStrictExpiration(enable bool) IndexVerifier
	WithStrictMetricCreation(enable bool) IndexVerifier
	WithPedanticExpiration(enable bool) IndexVerifier
	Verify(ctx context.Context) (hadIssue bool, err error)
}

type IndexInternalExpirerer interface {
	InternalForceExpirationTimestamp(ctx context.Context, value time.Time) error
}

type IndexRunner interface {
	RunOnce(ctx context.Context, now time.Time) bool
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

// TryLocker is a Locker with an additional TryLock() method and blocking methods.
type TryLocker interface {
	sync.Locker

	// TryLock try to acquire a lock but return false if unable to acquire it.
	TryLock(ctx context.Context, retryDelay time.Duration) bool
	BlockLock(ctx context.Context, blockTTL time.Duration) error
	UnblockLock(ctx context.Context) error
	BlockStatus(ctx context.Context) (bool, time.Duration, error)
}

type State interface {
	Read(ctx context.Context, name string, value interface{}) (bool, error)
	Write(ctx context.Context, name string, value interface{}) error
}

// RequestContextKey is used as a key in a context to a HTTP request.
type RequestContextKey struct{}

// WrapContext adds a request to the context.
func WrapContext(ctx context.Context, r *http.Request) context.Context {
	return context.WithValue(ctx, RequestContextKey{}, r)
}

// HTTP headers available to dynamically change settings on PromQL and remote read.
const (
	// Add one matcher to limit the evaluated series.
	HeaderForcedMatcher = "X-SquirrelDB-Forced-Matcher"
	// Limit the number of series that can be evaluated by a request.
	// A limit of 0 means unlimited.
	HeaderMaxEvaluatedSeries = "X-SquirrelDB-Max-Evaluated-Series"
	// Limit the number of points that can be evaluated by a request.
	// A limit of 0 means unlimited.
	HeaderMaxEvaluatedPoints = "X-SquirrelDB-Max-Evaluated-Points"
	// Force using pre-aggregated data instead of raw points. Default for
	// query is raw data. Default for query_range depends on step value.
	HeaderForcePreAggregated = "X-SquirrelDB-ForcePreAggregated"
	// Force using raw data instead of pre-aggregated points. If both ForcePreAggregated
	// and ForceRaw are true, ForceRaw has priority. Default for query is raw data.
	// Default for query_range depends on step value.
	HeaderForceRaw = "X-SquirrelDB-ForceRaw"
	// Only match metrics from this tenant. Metrics written with this header
	// are associated to this tenant (a tenant label is added to the metric labels).
	HeaderTenant = "X-SquirrelDB-Tenant"
	// Set the metric Time To Live when writing.
	HeaderTimeToLive = "X-SquirrelDB-TTL"
	// Enable debugging information printed in SquirrelDB server log about a query.
	HeaderQueryDebug        = "X-SquirrelDB-Query-Debug"         //nolint:gosec
	HeaderQueryVerboseDebug = "X-SquirrelDB-Query-Verbose-Debug" //nolint:gosec
)

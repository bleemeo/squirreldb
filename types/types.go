// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// TSDB is the component that store metrics (and possible states) for SquirrelDB.
// A backend had multiple component (metrics reader - Queryable - and writer Appendable, ...) which
// could be lazily initialized (when calling XXXComponent()) or initialized when the backend is created.
type TSDB interface {
	AppendableWithOptions
	QueryableWithOptions

	Close(ctx context.Context) error
}

// HTTPOptionsParser is the interface TSDB might want to implement in order to support per-HTTP request option,
// based on HTTP header. The result from ParseHTTPOptions is passed in BackendOptions of AppenderOptions/QuerierOptions.
type HTTPOptionsParser interface {
	ParseHTTPOptions(h http.Header) (any, error)
}

type AppenderOptions struct {
	Tenant       string
	RequestDebug DebugLevel
	WriteTTL     int64

	// BackendOptions is filled by calls like ParseHTTPOptions()
	BackendOptions any
}

// QuerierOptions contains the per-request (e.g. per HTTP request) options.
// It's intended to be consumer by TSDB Appender or Queryable.
type QuerierOptions struct {
	Tenant       string
	RequestDebug DebugLevel
	// MaxEvaluatedSeries caps how many series a single Select may resolve before
	// it must fail; 0 means unlimited. It mirrors the limit limitqueryable
	// enforces cumulatively across a request, exposed to backends so they can
	// bound the work (and query size) of a broad matcher up-front rather than
	// only once the caller iterates the result.
	MaxEvaluatedSeries uint32
	// BackendOptions is filled by calls like ParseHTTPOptions()
	BackendOptions any
}

type AppendableWithOptions interface {
	AppenderWithOptions(ctx context.Context, options AppenderOptions) storage.Appender
}

type QueryableWithOptions interface {
	QuerierWithOptions(opts QuerierOptions, mint int64, maxt int64) (storage.Querier, error)
	ChunkQuerierWithOptions(opts QuerierOptions, mint int64, maxt int64) (storage.ChunkQuerier, error)
}

type DebugLevel int

const (
	DebugLevelNone DebugLevel = iota
	DebugLevelSimple
	DebugLevelVerbose
)

// OldTask is a background worker that will be running until ctx is canceled.
// If readiness is not nil, when ready the task send one nil.
// If an error occurs, the task will send the error on the channel and return.
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
	// Size returns the size of the cluster, including *this* SquirrelDB.
	Size() int
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
	BlockCassandraWrite(ctx context.Context) error
	UnblockCassandraWrite(ctx context.Context) error
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
	InternalRunOnce(ctx context.Context, now time.Time) bool
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
	Read(ctx context.Context, name string, value any) (bool, error)
	Write(ctx context.Context, name string, value any) error
}

// BackdateContextKey is used as a key in a context to a points backdate offset.
type BackdateContextKey struct{}

// HTTPRegistrar is the minimal HTTP routing interface used to register
// backend-specific HTTP endpoints (debug, management, etc.) on the API router.
type HTTPRegistrar interface {
	Get(path string, handler http.HandlerFunc)
	Post(path string, handler http.HandlerFunc)
	Del(path string, handler http.HandlerFunc)
}

// HTTP headers available to dynamically change settings on PromQL and remote read.
const (
	// HeaderForcedMatcher Add one matcher to limit the evaluated series.
	HeaderForcedMatcher = "X-SquirrelDB-Forced-Matcher"
	// HeaderMaxEvaluatedSeries Limit the number of series that can be evaluated by a request.
	// A limit of 0 means unlimited.
	HeaderMaxEvaluatedSeries = "X-SquirrelDB-Max-Evaluated-Series"
	// HeaderMaxEvaluatedPoints Limit the number of points that can be evaluated by a request.
	// A limit of 0 means unlimited.
	HeaderMaxEvaluatedPoints = "X-SquirrelDB-Max-Evaluated-Points"
	// HeaderForcePreAggregated Force using pre-aggregated data instead of raw points. Default for
	// query is raw data. Default for query_range depends on step value.
	HeaderForcePreAggregated = "X-SquirrelDB-ForcePreAggregated"
	// HeaderForceRaw Force using raw data instead of pre-aggregated points. If both ForcePreAggregated
	// and ForceRaw are true, ForceRaw has priority. Default for query is raw data.
	// Default for query_range depends on step value.
	HeaderForceRaw = "X-SquirrelDB-ForceRaw"
	// HeaderTenant Only match metrics from this tenant. Metrics written with this header
	// are associated to this tenant (a tenant label is added to the metric labels).
	HeaderTenant = "X-SquirrelDB-Tenant"
	// HeaderTimeToLive Set the metric Time To Live when writing.
	HeaderTimeToLive = "X-SquirrelDB-TTL"
	// HeaderQueryDebug Enable debugging information printed in SquirrelDB server log about a query.
	HeaderQueryDebug        = "X-SquirrelDB-Query-Debug"
	HeaderQueryVerboseDebug = "X-SquirrelDB-Query-Verbose-Debug"
)

const PrometheusValidationScheme = model.UTF8Validation

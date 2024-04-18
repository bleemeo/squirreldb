package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec,gci
	"net/url"
	"runtime"
	"github.com/bleemeo/squirreldb/api/promql"
	"github.com/bleemeo/squirreldb/api/remotestorage"
	"github.com/bleemeo/squirreldb/cassandra/mutable"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/types"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/rs/zerolog"
)

const (
	httpServerShutdownTimeout = 10 * time.Second

	// A read sample limit is already implemented dynamically with the header X-SquirrelDB-Max-Evaluated-Points.
	remoteReadSampleLimit     = 0       // No limit
	remoteReadMaxBytesInFrame = 1048576 // 1 MiB (Prometheus default)
)

var (
	regexErrInvalidMatcher      = regexp.MustCompile(fmt.Sprintf("^%s", remotestorage.ErrInvalidMatcher))
	regexErrMissingTenantHeader = regexp.MustCompile(fmt.Sprintf("^%s", remotestorage.ErrMissingTenantHeader))
)

// API it the SquirrelDB HTTP API server.
type API struct {
	ListenAddress               string
	ReadOnly                    bool
	Index                       types.Index
	Reader                      types.MetricReader
	Writer                      types.MetricWriter
	MutableLabelWriter          MutableLabelInterface
	FlushCallback               func() error
	PreAggregateCallback        func(ctx context.Context, thread int, from, to time.Time) error
	MaxConcurrentRemoteRequests int
	PromQLMaxEvaluatedPoints    uint64
	MetricRegistry              prometheus.Registerer
	PromQLMaxEvaluatedSeries    uint32
	TenantLabelName             string
	MutableLabelDetector        remotestorage.MutableLabelDetector
	// When enabled, return an response to queries and write
	// requests that don't provide the tenant header.
	RequireTenantHeader   bool
	UseThanosPromQLEngine bool
	Logger                zerolog.Logger

	ready      int32
	router     http.Handler
	listenPort int
	metrics    *metrics

	l                   sync.Mutex
	defaultDebugRequest bool
}

type MutableLabelInterface interface {
	WriteLabelValues(ctx context.Context, lbls []mutable.LabelWithValues) error
	DeleteLabelValues(ctx context.Context, lbls []mutable.Label) error
	WriteLabelNames(ctx context.Context, lbls []mutable.LabelWithName) error
	DeleteLabelNames(ctx context.Context, names []mutable.LabelKey) error
	Dump(ctx context.Context, w io.Writer) error
	Import(ctx context.Context, r io.Reader, w io.Writer, dryRun bool) error
}

// NewPrometheus returns a new initialized Prometheus web API.
// Only remote read/write and PromQL endpoints are implemented.
// appendable can be empty if remote write is not used.
func NewPrometheus(
	queryable storage.SampleAndChunkQueryable,
	appendable storage.Appendable,
	maxConcurrent int,
	metricRegistry prometheus.Registerer,
	useThanosPromQLEngine bool,
	apiLogger zerolog.Logger,
) *v1.API {
	queryLogger := apiLogger.With().Str("component", "query_engine").Logger()

	queryEngine := promql.NewEngine(queryLogger, useThanosPromQLEngine, metricRegistry)
	queryEngine = promql.WrapEngine(queryEngine, apiLogger)

	scrapePoolRetrieverFunc := func(_ context.Context) v1.ScrapePoolsRetriever { return mockScrapePoolRetriever{} }
	targetRetrieverFunc := func(context.Context) v1.TargetRetriever { return mockTargetRetriever{} }
	alertmanagerRetrieverFunc := func(context.Context) v1.AlertmanagerRetriever { return mockAlertmanagerRetriever{} }
	rulesRetrieverFunc := func(context.Context) v1.RulesRetriever { return mockRulesRetriever{} }

	runtimeInfoFunc := func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errNotImplemented }

	configFunc := func() config.Config { return config.DefaultConfig } // Only used to read the external labels.

	readyFunc := func(f http.HandlerFunc) http.HandlerFunc {
		// Waiting for the storage to be ready is already handled by API.ServeHTTP.
		return f
	}

	// flagsMap is only used on /status/flags, it can empty.
	var flagsMap map[string]string

	// dbDir can be empty because it's only used in mockTSDBAdminStat.Snapshot, which is not implemented.
	dbDir := ""

	// Admin endpoints are not implemented.
	enableAdmin := false

	// SquirrelDB is assumed to run on a private network, therefore CORS don't apply.
	CORSOrigin := regexp.MustCompile(".*")

	// We enable both remote-write and OTLP-write handlers, even if we don't explicitly support OTLP.
	rwEnabled, otlpEnabled := true, true

	api := v1.NewAPI(
		queryEngine,
		queryable,
		appendable,
		mockExemplarQueryable{},
		scrapePoolRetrieverFunc,
		targetRetrieverFunc,
		alertmanagerRetrieverFunc,
		configFunc,
		flagsMap,
		v1.GlobalURLOptions{},
		readyFunc,
		mockTSDBAdminStat{},
		dbDir,
		enableAdmin,
		logger.NewKitLogger(&apiLogger),
		rulesRetrieverFunc,
		remoteReadSampleLimit,
		maxConcurrent,
		remoteReadMaxBytesInFrame,
		false,
		CORSOrigin,
		runtimeInfoFunc,
		&v1.PrometheusVersion{},
		mockGatherer{},
		metricRegistry,
		nil,
		rwEnabled,
		otlpEnabled,
	)

	return api
}

func (a *API) init() {
	a.metrics = newMetrics(a.MetricRegistry)

	router := route.New()

	router.Get("/metrics", promhttp.Handler().ServeHTTP)
	router.Get("/ready", a.readyHandler)
	router.Get("/debug/", a.debugHelpHandler)
	router.Get("/debug/index_info", a.indexInfoHandler)
	router.Get("/debug/flush", a.flushHandler)
	router.Get("/debug/index_verify", a.indexVerifyHandler)
	router.Get("/debug/index_dump", a.indexDumpHandler)
	router.Get("/debug/index_dump_by_labels", a.indexDumpByLabelsHandler)
	router.Get("/debug/index_dump_by_expiration", a.indexDumpByExpirationDateHandler)
	router.Get("/debug/index_dump_by_shard", a.indexDumpByShardHandler)
	router.Get("/debug/index_dump_by_posting", a.indexDumpByPostingHandler)
	router.Get("/debug/index_block", a.indexBlockHandler)
	router.Get("/debug/index_unblock", a.indexUnblockHandler)
	router.Get("/debug/toggle_debug_query", a.toggleDebugQueryHandler)
	router.Get("/debug/preaggregate", a.aggregateHandler)
	router.Get("/debug/mutable_dump", a.mutableDumpHandler)
	router.Post("/debug/mutable_import", a.mutableImportHandler)
	router.Get("/debug/pprof/*item", http.DefaultServeMux.ServeHTTP)

	router.Post("/mutable/names", a.mutableLabelNamesWriteHandler)
	router.Del("/mutable/names", a.mutableLabelNamesDeleteHandler)
	router.Post("/mutable/values", a.mutableLabelValuesWriteHandler)
	router.Del("/mutable/values", a.mutableLabelValuesDeleteHandler)

	queryable := promql.NewStore(
		a.Logger,
		a.Index,
		a.Reader,
		a.TenantLabelName,
		a.RequireTenantHeader,
		a.PromQLMaxEvaluatedSeries,
		a.PromQLMaxEvaluatedPoints,
		a.MetricRegistry,
	)

	maxConcurrent := a.MaxConcurrentRemoteRequests
	if maxConcurrent <= 0 {
		maxConcurrent = runtime.GOMAXPROCS(0) * 2
	}

	appendable := remotestorage.NewReadOnly()

	if !a.ReadOnly {
		appendable = remotestorage.New(
			a.Writer,
			a.Index,
			maxConcurrent,
			a.TenantLabelName,
			a.MutableLabelDetector,
			a.RequireTenantHeader,
			a.MetricRegistry,
		)
	}

	api := NewPrometheus(
		queryable,
		appendable,
		maxConcurrent,
		a.MetricRegistry,
		a.UseThanosPromQLEngine,
		a.Logger,
	)

	// Wrap the router to add the http request to the context so the querier can access the HTTP headers.
	routerWrapper := router.WithInstrumentation(func(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
		operation := strings.Trim(handlerName, "/")

		h := func(rw http.ResponseWriter, r *http.Request) {
			a.l.Lock()

			if a.defaultDebugRequest {
				r.Header.Add(types.HeaderQueryDebug, "1")
			}

			a.l.Unlock()

			t0 := time.Now()
			defer func() {
				a.metrics.RequestsSeconds.WithLabelValues(operation).Observe(time.Since(t0).Seconds())
			}()

			// We must create the cachingReader at this stage
			// so that only one is allocated per request,
			// and thus be able to benefit from its cache.
			ctx, err := queryable.ContextFromRequest(r)
			if err != nil {
				a.respondError(rw, http.StatusUnprocessableEntity, err)

				return
			}

			r = r.WithContext(ctx) //nolint: contextcheck

			// Prometheus always returns a status 500 when write fails, but if
			// the labels are invalid we want to return a status 400, so we use the
			// interceptor to change the returned status in this case.
			if operation == "write" {
				rw = &interceptor{OrigWriter: rw}
			}

			handler(rw, r)

			enableDebug := r.Header.Get(types.HeaderQueryDebug) != ""
			if enableDebug {
				humainURL := r.URL.Path

				if r.URL.RawQuery != "" {
					v, _ := url.QueryUnescape(r.URL.RawQuery)
					humainURL += "?" + v
				}

				a.Logger.Info().Msgf(
					"%s %s took %s",
					r.Method,
					humainURL,
					time.Since(t0).String(),
				)
			}
		}

		return h
	})

	apiRouter := routerWrapper.WithPrefix("/api/v1")
	api.Register(apiRouter)

	a.router = apiRouter
}

// Run start the HTTP api server.
func (a *API) Run(ctx context.Context, readiness chan error) {
	a.init()

	server := &http.Server{
		Addr:              a.ListenAddress,
		Handler:           a,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverStopped := make(chan error)

	ln, err := net.Listen("tcp", a.ListenAddress)
	if err != nil {
		readiness <- err

		return
	}

	if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok {
		a.listenPort = tcpAddr.Port
	}

	go func() {
		defer logger.ProcessPanic()

		serverStopped <- server.Serve(ln)
	}()

	a.Logger.Info().Msgf("Server listening on %s", a.ListenAddress)

	readiness <- nil

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpServerShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil { //nolint: contextcheck
			a.Logger.Err(err).Msg("Failed stop the HTTP server")
		}

		err = <-serverStopped
	case err = <-serverStopped:
	}

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		a.Logger.Err(err).Msg("HTTP server failed")
	}

	a.Logger.Debug().Msg("Server stopped")
}

// ListenPort return the port listenning on. Should not be used before Run()
// signalled its readiness.
// This is useful for tests that use port "0" to known the actual listenning port.
func (a *API) ListenPort() int {
	return a.listenPort
}

// Ready mark the system as ready to access requests.
func (a *API) Ready() {
	atomic.StoreInt32(&a.ready, 1)
}

func (a *API) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	v := atomic.LoadInt32(&a.ready)
	if v > 0 {
		a.router.ServeHTTP(w, req)

		return
	}

	http.Error(w, "Server not yet ready", http.StatusServiceUnavailable)
}

func (a *API) flushHandler(w http.ResponseWriter, _ *http.Request) {
	start := time.Now()

	if a.FlushCallback != nil {
		err := a.FlushCallback()
		if err != nil {
			http.Error(w, fmt.Sprintf("Flush failed: %v", err), http.StatusInternalServerError)

			return
		}
	}

	msg := "Flush points (of this SquirrelDB instance) from temporary store to TSDB done in %v\n"
	fmt.Fprintf(w, msg, time.Since(start))
}

func (a *API) readyHandler(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprintln(w, "Ready")
}

func (a *API) debugHelpHandler(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprint(w, `The following index info enpoints exists

/debug/index_info
	provide global information about index

/debug/index_info?metricID=XXX
	provide information on given metric ID

/debug/toggle_debug_query
	Enable or disable some tracing of query submitted to SquirrelDB. This could be very verbose.
	By using the header X-SquirrelDB-Query-Debug you could enable tracing for a single query.

/debug/preaggregate?from=2006-01-02&to=2006-01-02
	(Re)-run the preaggragation on specified day(s). If you inserted data with timestamp older than
	ingestion window (8 hours), the pre-aggregation might already have processed that day.
	This endpoints allow to force re-running the pre-aggregations on that day.

/debug/pprof/
	Golang pprof endpoints

/debug/flush
	Write metric from the memory store to Cassandra.
	SquirrelDB first write incoming points to a memory store with fast random access (Redis or local memory),
	and after some time write them to Cassandra. This is done to improve compression and reduce number of writes
	to Cassandra.
	You can force flushing this memory store to Cassandra with this endpoints. This only apply to this SquirrelDB
	instance, so in a cluster you need to call this endpoint on all SquirrelDB instance.

/debug/mutable_dump
	Produce a CSV with all known mutable labels.

/debug/mutable_import
	Remove all known mutable labels and add all labels provided by input CSV.
	Example of curl to export/import:
	$ curl http://localhost:9201/debug/mutable_dump > file.csv
	$ curl http://localhost:9201/debug/mutable_import --data-binary @file.csv

/debug/index_block
	Block write on Cassandra for 5 minutes. Use /debug/index_unblock to
	release the block before the 5 minutes delay. Re-use /debug/index_block
	to extend to block duration.

/debug/index_unblock
	Undo the block from /debug/index_block

The is some index dump enpoints, that produce CSV export of the index information.
The CSV format is: metricID, labels,expirationDate

/debug/index_dump
	Returns all known metrics

/debug/index_dump_by_labels?query=cpu_used{instance=\"my_server\"}
	Returns metrics matching a PromQL query. The "query" parameter is required.
	You can optionally provide "start" and "end" parameters (format 2006-01-02) to limit the search
	to a time range. Default is from 1971 to now

/debug/index_dump_by_expiration?date=2006-01-02
	Returns metrics that will be checked for expiration at "date" parameter.
	The metrics don't necessary expire at this date, but SquirrelDB will check for their expiration at this date.
	The way expiration works is that SquirrelDB write in a bucket per day metrics that need to be checked for expiration
	to avoid scanning the whole index for expiration.

/debug/index_dump_by_shard?shard_time=2006-01-02
	Returns metrics contained in shard for given "shard_time" parameter.
	SquirrelDB index is sharded by time, the shard size is 7 days. Each week a new shard is created.

/debug/index_dump_by_posting?name=__name__
	Returns metrics matching label name/value couple (called posting in SquirrelDB index)
	If only "name" parameter is provided, returns metrics that had a label with that name regardless of the label value.
	If both "name" and "value" parameters are provided, returns metrics that had the same label name and value matching
	You can optionally provide "shard_time" parameter to query a specific index shard. Default is now.

	You can also use the value "0001-01-01" for shard_time, in order to query the SquirrelDB special global shard. This
	shard contains special value. Example (remember to use quote in your shell):
	/debug/index_dump_by_posting?shard_time=0001-01-01&name=__global__all|metrics__

`)
}

func (a *API) indexInfoHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexDumper); ok {
		metricIDText := req.URL.Query()["metricID"]

		switch {
		case len(metricIDText) > 0:
			a.indexInfoIDs(ctx, w, idx, metricIDText)
		default:
			if err := idx.InfoGlobal(ctx, w); err != nil {
				http.Error(w, fmt.Sprintf("Index info failed: %v", err), http.StatusInternalServerError)

				return
			}
		}
	} else {
		http.Error(w, "Index does not implement Info*()", http.StatusNotImplemented)

		return
	}
}

func (a *API) indexInfoIDs(
	ctx context.Context,
	w http.ResponseWriter,
	idx types.IndexDumper,
	metricIDText []string,
) {
	for _, text := range metricIDText {
		id, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("Index info failed: %v", err), http.StatusInternalServerError)

			return
		}

		if err := idx.InfoByID(ctx, w, types.MetricID(id)); err != nil {
			http.Error(w, fmt.Sprintf("Index info failed: %v", err), http.StatusInternalServerError)

			return
		}
	}
}

func (a *API) indexVerifyHandler(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	ctx := req.Context()

	if idx, ok := a.Index.(types.VerifiableIndex); ok { //nolint:nestif
		doFix := req.FormValue("fix") != ""
		acquireLock := req.FormValue("lock") != ""
		strict := req.FormValue("strict") != ""

		nowList := req.URL.Query()["now"]
		if len(nowList) > 1 {
			http.Error(w, `Expect at most one parameter "now"`, http.StatusBadRequest)

			return
		}

		now := time.Now()

		if len(nowList) == 1 {
			var err error

			now, err = time.Parse("2006-01-02", nowList[0])
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to parse date: %v", err), http.StatusBadRequest)

				return
			}
		}

		_, err := idx.Verifier(w).
			WithNow(now).
			WithLock(acquireLock).
			WithDoFix(doFix).
			WithStrictExpiration(strict).
			WithStrictMetricCreation(strict).
			Verify(ctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("Index verification failed: %v", err), http.StatusInternalServerError)

			return
		}
	} else {
		http.Error(w, "Index does not implement Verify()", http.StatusNotImplemented)

		return
	}

	fmt.Fprintf(w, "Index verification took %v\n", time.Since(start))
}

func (a *API) indexDumpHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexDumper); ok {
		if err := idx.Dump(ctx, w); err != nil {
			http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

			return
		}
	} else {
		http.Error(w, "Index does not implement Dump()", http.StatusNotImplemented)

		return
	}
}

func (a *API) mutableDumpHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if err := a.MutableLabelWriter.Dump(ctx, w); err != nil {
		http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

		return
	}
}

func (a *API) mutableImportHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	force := len(req.URL.Query()["force"]) > 0

	if err := a.MutableLabelWriter.Import(ctx, req.Body, w, !force); err != nil {
		http.Error(w, fmt.Sprintf("Index import failed: %v", err), http.StatusInternalServerError)

		return
	}

	if !force {
		fmt.Fprintln(
			w,
			"To apply change, add \"force\" parameter (e.g. curl http://localhost:9201/debug/mutable_import?force [...])",
		)
	}
}

func (a *API) indexDumpByLabelsHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	idx, ok := a.Index.(types.IndexDumper)
	if !ok {
		http.Error(w, "Index does not implement DumpByLabels()", http.StatusNotImplemented)

		return
	}

	var err error

	// Set start to the oldest date possible if it's not set.
	start := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC)

	startStr := req.URL.Query().Get("start")
	if startStr != "" {
		start, err = time.Parse("2006-01-02", startStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("parse start time: %v", err), http.StatusBadRequest)

			return
		}
	}

	// Set end to now if it's not set.
	end := time.Now()

	endStr := req.URL.Query().Get("end")
	if endStr != "" {
		end, err = time.Parse("2006-01-02", endStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("parse end time: %v", err), http.StatusBadRequest)

			return
		}
	}

	matchers, err := parser.ParseMetricSelector(req.URL.Query().Get("query"))
	if err != nil {
		http.Error(w, fmt.Sprintf("fail to parse query: %v", err), http.StatusBadRequest)

		return
	}

	if err := idx.DumpByLabels(ctx, w, start, end, matchers); err != nil {
		http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

		return
	}
}

func (a *API) indexDumpByExpirationDateHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexDumper); ok {
		dates := req.URL.Query()["date"]
		if len(dates) != 1 {
			http.Error(w, `Expect one parameter "date"`, http.StatusBadRequest)

			return
		}

		date, err := time.Parse("2006-01-02", dates[0])
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to parse date: %v", err), http.StatusBadRequest)

			return
		}

		if err := idx.DumpByExpirationDate(ctx, w, date); err != nil {
			http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

			return
		}
	} else {
		http.Error(w, "Index does not implement DumpByExpirationDate()", http.StatusNotImplemented)

		return
	}
}

func (a *API) indexDumpByShardHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexDumper); ok {
		dates := req.URL.Query()["shard_time"]
		if len(dates) != 1 {
			http.Error(w, `Expect one parameter "shard_time"`, http.StatusBadRequest)

			return
		}

		date, err := time.Parse("2006-01-02", dates[0])
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to parse date: %v", err), http.StatusBadRequest)

			return
		}

		if err := idx.DumpByShard(ctx, w, date); err != nil {
			http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

			return
		}
	} else {
		http.Error(w, "Index does not implement DumpByExpirationDate()", http.StatusNotImplemented)

		return
	}
}

func getIndexDumpByPostingArgument(req *http.Request) (time.Time, string, string, string) {
	dates := req.URL.Query()["shard_time"]
	if len(dates) > 1 {
		return time.Time{}, "", "", `Expect at most one parameter "shard_time"`
	}

	date := time.Now()

	if len(dates) == 1 {
		var err error

		date, err = time.Parse("2006-01-02", dates[0])
		if err != nil {
			return date, "", "", fmt.Sprintf("Failed to parse date: %v", err)
		}
	}

	names := req.URL.Query()["name"]
	values := req.URL.Query()["value"]

	if len(names) != 1 {
		return date, "", "", `Expect one parameter "name"`
	}

	if len(values) > 1 {
		return date, names[0], "", `Expect at most one parameter "value"`
	}

	var value string
	if len(values) == 1 {
		value = values[0]
	}

	return date, names[0], value, ""
}

func (a *API) indexDumpByPostingHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexDumper); ok {
		date, name, value, errorMessage := getIndexDumpByPostingArgument(req)
		if errorMessage != "" {
			http.Error(w, errorMessage, http.StatusBadRequest)

			return
		}

		if err := idx.DumpByPosting(ctx, w, date, name, value); err != nil {
			http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

			return
		}
	} else {
		http.Error(w, "Index does not implement DumpByExpirationDate()", http.StatusNotImplemented)

		return
	}
}

func (a *API) indexBlockHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexBlocker); ok {
		if err := idx.BlockCassandraWrite(ctx); err != nil {
			http.Error(w, fmt.Sprintf("Index block failed: %v", err), http.StatusInternalServerError)

			return
		}

		fmt.Fprintln(w, "Write to Cassandra blocked")
	} else {
		http.Error(w, "Index does not implement BlockCassandraWrite()", http.StatusNotImplemented)

		return
	}
}

func (a *API) indexUnblockHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexBlocker); ok {
		if err := idx.UnblockCassandraWrite(ctx); err != nil {
			http.Error(w, fmt.Sprintf("Index block failed: %v", err), http.StatusInternalServerError)

			return
		}

		fmt.Fprintln(w, "Write to Cassandra unblocked")
	} else {
		http.Error(w, "Index does not implement UnblockCassandraWrite()", http.StatusNotImplemented)

		return
	}
}

func (a *API) toggleDebugQueryHandler(w http.ResponseWriter, _ *http.Request) {
	a.l.Lock()

	a.defaultDebugRequest = !a.defaultDebugRequest
	debugRequest := a.defaultDebugRequest

	a.l.Unlock()

	fmt.Fprintf(w, "defaultDebugRequest is now %v\n", debugRequest)
}

func (a *API) aggregateHandler(w http.ResponseWriter, req *http.Request) {
	fromRaw := req.URL.Query().Get("from")
	toRaw := req.URL.Query().Get("to")
	threadRaw := req.URL.Query().Get("thread")
	ctx := req.Context()

	from, err := time.Parse("2006-01-02", fromRaw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	to, err := time.Parse("2006-01-02", toRaw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	thread := 1

	if threadRaw != "" {
		tmp, err := strconv.ParseInt(threadRaw, 10, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		thread = int(tmp)
	}

	start := time.Now()

	// Flush the points from the temporary store so they can be pre-aggregated.
	// Recent points are still kept in memory for faster reads.
	if a.FlushCallback != nil {
		err := a.FlushCallback()
		if err != nil {
			http.Error(w, fmt.Sprintf("Flush failed: %v", err), http.StatusInternalServerError)

			return
		}
	}

	if a.PreAggregateCallback != nil {
		err := a.PreAggregateCallback(ctx, thread, from, to)
		if err != nil {
			http.Error(w, "pre-aggregation failed", http.StatusInternalServerError)

			return
		}
	}

	fmt.Fprintf(w, "Pre-aggregation terminated in %v\n", time.Since(start))
}

func (a *API) mutableLabelValuesWriteHandler(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)

	var lbls []mutable.LabelWithValues

	err := decoder.Decode(&lbls)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to decode body: %v", err), http.StatusBadRequest)

		return
	}

	// Check that all fields except the associated values are not empty.
	for _, label := range lbls {
		if label.Tenant == "" || label.Name == "" || label.Value == "" {
			errMsg := "keys must be provided and not empty (tenant, name, value): %#v"
			http.Error(w, fmt.Sprintf(errMsg, label), http.StatusBadRequest)

			return
		}

		for _, value := range label.AssociatedValues {
			if value == "" {
				errMsg := "associated values can't contain an empty string: %#v"
				http.Error(w, fmt.Sprintf(errMsg, label), http.StatusBadRequest)

				return
			}
		}
	}

	if err := a.MutableLabelWriter.WriteLabelValues(req.Context(), lbls); err != nil {
		http.Error(w, fmt.Sprintf("failed to write label values: %v", err), http.StatusInternalServerError)

		return
	}

	fmt.Fprint(w, "ok")
}

func (a *API) mutableLabelValuesDeleteHandler(w http.ResponseWriter, req *http.Request) { //nolint:dupl
	decoder := json.NewDecoder(req.Body)

	var lbls []mutable.Label

	err := decoder.Decode(&lbls)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to decode body: %v", err), http.StatusBadRequest)

		return
	}

	// Check that all fields are not empty.
	for _, label := range lbls {
		if label.Tenant == "" || label.Name == "" || label.Value == "" {
			errMsg := "all keys must be provided and not empty (tenant, name, value): %#v"
			http.Error(w, fmt.Sprintf(errMsg, label), http.StatusBadRequest)

			return
		}
	}

	if err := a.MutableLabelWriter.DeleteLabelValues(req.Context(), lbls); err != nil {
		http.Error(w, fmt.Sprintf("failed to delete label values: %v", err), http.StatusInternalServerError)

		return
	}

	fmt.Fprint(w, "ok")
}

func (a *API) mutableLabelNamesWriteHandler(w http.ResponseWriter, req *http.Request) { //nolint:dupl
	decoder := json.NewDecoder(req.Body)

	var lbls []mutable.LabelWithName

	err := decoder.Decode(&lbls)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to decode body: %v", err), http.StatusBadRequest)

		return
	}

	// Check that all fields are not empty.
	for _, label := range lbls {
		if label.Tenant == "" || label.Name == "" || label.AssociatedName == "" {
			errMsg := "all keys must be provided and not empty (tenant, name, associated_name): %#v"
			http.Error(w, fmt.Sprintf(errMsg, label), http.StatusBadRequest)

			return
		}
	}

	if err := a.MutableLabelWriter.WriteLabelNames(req.Context(), lbls); err != nil {
		http.Error(w, fmt.Sprintf("failed to write label names: %v", err), http.StatusInternalServerError)

		return
	}

	fmt.Fprint(w, "ok")
}

func (a *API) mutableLabelNamesDeleteHandler(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)

	var labelKeys []mutable.LabelKey

	err := decoder.Decode(&labelKeys)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to decode body: %v", err), http.StatusBadRequest)

		return
	}

	// Check that all fields are not empty.
	for _, label := range labelKeys {
		if label.Tenant == "" || label.Name == "" {
			errMsg := "all keys must be provided and not empty (tenant, name): %#v"
			http.Error(w, fmt.Sprintf(errMsg, label), http.StatusBadRequest)

			return
		}
	}

	if err := a.MutableLabelWriter.DeleteLabelNames(req.Context(), labelKeys); err != nil {
		http.Error(w, fmt.Sprintf("failed to delete label values: %v", err), http.StatusInternalServerError)

		return
	}

	fmt.Fprint(w, "ok")
}

// respondError mimics the Prometheus v1.API behavior for returning an error to the client.
func (a *API) respondError(w http.ResponseWriter, status int, err error) {
	b, err := json.Marshal(&v1.Response{
		Status: "error",
		Error:  err.Error(),
	})
	if err != nil {
		a.Logger.Err(err).Msg("error marshaling json response")
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if _, err := w.Write(b); err != nil {
		a.Logger.Err(err).Msg("error writing response")
	}
}

// interceptor implements the http.ResponseWriter interface,
// it allows to catch and modify the response status code.
type interceptor struct {
	OrigWriter http.ResponseWriter
	status     int
}

func (i *interceptor) WriteHeader(rc int) {
	i.status = rc
}

func (i *interceptor) Write(b []byte) (int, error) {
	if i.status == http.StatusInternalServerError &&
		(regexErrInvalidMatcher.Match(b) || regexErrMissingTenantHeader.Match(b)) {
		i.status = http.StatusBadRequest
	}

	// Don't write the header if the status is unset because it raises a panic.
	if i.status != 0 {
		i.OrigWriter.WriteHeader(i.status)
		i.status = 0
	}

	return i.OrigWriter.Write(b)
}

func (i *interceptor) Header() http.Header {
	return i.OrigWriter.Header()
}

package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint: gosec, gci
	"os"
	"regexp"
	"squirreldb/api/promql"
	"squirreldb/api/remotestorage"
	"squirreldb/types"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	ppromql "github.com/prometheus/prometheus/promql"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

const (
	httpServerShutdownTimeout = 10 * time.Second
)

// API it the SquirrelDB HTTP API server.
type API struct {
	ListenAddress            string
	Index                    types.Index
	Reader                   types.MetricReader
	Writer                   types.MetricWriter
	FlushCallback            func() error
	PreAggregateCallback     func(ctx context.Context, thread int, from, to time.Time) error
	MaxConcurrentRemoteWrite int
	PromQLMaxEvaluatedPoints uint64
	MetricRegisty            prometheus.Registerer
	PromQLMaxEvaluatedSeries uint32

	ready      int32
	logger     log.Logger
	router     http.Handler
	listenPort int
}

// newAPI returns a new initialized Prometheus web API.
func newAPI(
	index types.Index,
	reader types.MetricReader,
	writer types.MetricWriter,
	promQLMaxEvaluatedPoints uint64,
	promQLMaxEvaluatedSeries uint32,
	metricRegistry prometheus.Registerer,
) *v1.API {
	const (
		// TODO: better defaults?
		remoteReadSampleLimit      = 0 // No limit
		remoteReadConcurrencyLimit = 20
		remoteReadMaxBytesInFrame  = 10000000
	)

	stderrLogger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	queryEngine := ppromql.NewEngine(ppromql.EngineOpts{
		Logger:             log.With(stderrLogger, "component", "query engine"),
		Reg:                metricRegistry,
		MaxSamples:         50000000,
		Timeout:            2 * time.Minute,
		ActiveQueryTracker: nil,
		LookbackDelta:      5 * time.Minute,
	})

	queryable := promql.Store{
		Index:                     index,
		Reader:                    reader,
		DefaultMaxEvaluatedSeries: promQLMaxEvaluatedSeries,
		DefaultMaxEvaluatedPoints: promQLMaxEvaluatedPoints,
	}

	appendable := remotestorage.RemoteStorage{
		Reader: reader,
		Writer: writer,
		Index:  index,
	}

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

	// TODO: Should we limit the CORS origin?
	CORSOrigin := regexp.MustCompile(".*")

	api := v1.NewAPI(
		queryEngine,
		queryable,
		appendable,
		mockExemplarQueryable{},
		targetRetrieverFunc,
		alertmanagerRetrieverFunc,
		configFunc,
		flagsMap,
		v1.GlobalURLOptions{},
		readyFunc,
		mockTSDBAdminStat{},
		dbDir,
		enableAdmin,
		log.With(stderrLogger, "component", "api"),
		rulesRetrieverFunc,
		remoteReadSampleLimit,
		remoteReadConcurrencyLimit,
		remoteReadMaxBytesInFrame,
		CORSOrigin,
		runtimeInfoFunc,
		&v1.PrometheusVersion{},
		mockGatherer{},
		metricRegistry,
	)

	return api
}

// Run start the HTTP api server.
func (a *API) Run(ctx context.Context, readiness chan error) {
	a.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	router := route.New()

	router.Get("/metrics", promhttp.Handler().ServeHTTP)
	router.Get("/flush", a.flushHandler)
	router.Get("/ready", a.readyHandler)
	router.Get("/debug/index_verify", a.indexVerifyHandler)
	router.Get("/debug/index_dump", a.indexDumpHandler)
	router.Get("/debug/preaggregate", a.aggregateHandler)
	router.Get("/debug_preaggregate", a.aggregateHandler)
	router.Get("/debug/pprof/*item", http.DefaultServeMux.ServeHTTP)

	api := newAPI(a.Index, a.Reader, a.Writer, a.PromQLMaxEvaluatedPoints, a.PromQLMaxEvaluatedSeries, a.MetricRegisty)
	apiRouter := route.New().WithPrefix("/api/v1")
	api.Register(apiRouter)

	promql := promql.PromQL{
		MetricRegistry: a.MetricRegisty,
		APIRouter:      apiRouter,
	}

	remote := remotestorage.RemoteStorage{
		Index:                    a.Index,
		Reader:                   a.Reader,
		Writer:                   a.Writer,
		MaxConcurrentRemoteWrite: a.MaxConcurrentRemoteWrite,
		MetricRegisty:            a.MetricRegisty,
		APIRouter:                apiRouter,
	}

	promql.Register(router.WithPrefix("/api/v1"))
	remote.Register(router)

	server := &http.Server{
		Addr:    a.ListenAddress,
		Handler: a,
	}
	a.router = router

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
		serverStopped <- server.Serve(ln)
	}()

	_ = a.logger.Log("msg", "Server listening on", "address", a.ListenAddress)

	readiness <- nil

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpServerShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = a.logger.Log("msg", "Failed stop the HTTP server", "err", err)
		}

		err = <-serverStopped
	case err = <-serverStopped:
	}

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		_ = a.logger.Log("msg", "HTTP server failed", "err", err)
	}

	_ = level.Debug(a.logger).Log("msg", "server stopped")
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

func (a API) flushHandler(w http.ResponseWriter, req *http.Request) {
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

func (a *API) readyHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(w, "Ready")
}

type indexVerifier interface {
	Verify(ctx context.Context, w io.Writer, doFix bool, acquireLock bool) (hadIssue bool, err error)
}

func (a API) indexVerifyHandler(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	ctx := req.Context()

	if idx, ok := a.Index.(indexVerifier); ok {
		doFix := req.FormValue("fix") != ""
		acquireLock := req.FormValue("lock") != ""

		_, err := idx.Verify(ctx, w, doFix, acquireLock)
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

type indexDumper interface {
	Dump(ctx context.Context, w io.Writer) error
}

func (a API) indexDumpHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(indexDumper); ok {
		err := idx.Dump(ctx, w)
		if err != nil {
			http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

			return
		}
	} else {
		http.Error(w, "Index does not implement Verify()", http.StatusNotImplemented)

		return
	}
}

func (a API) aggregateHandler(w http.ResponseWriter, req *http.Request) {
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

	if a.PreAggregateCallback != nil {
		err := a.PreAggregateCallback(ctx, thread, from, to)
		if err != nil {
			http.Error(w, "pre-aggregation failed", http.StatusInternalServerError)

			return
		}
	}

	fmt.Fprintf(w, "Pre-aggregation terminated in %v\n", time.Since(start))
}

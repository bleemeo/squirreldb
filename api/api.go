package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec,gci
	"runtime"
	"squirreldb/api/promql"
	"squirreldb/api/remotestorage"
	"squirreldb/cassandra/mutable"
	"squirreldb/logger"
	"squirreldb/types"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	ppromql "github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/rs/zerolog"
)

const (
	httpServerShutdownTimeout = 10 * time.Second

	// A read sample limit is already implemented dynamically with the header X-PromQL-Max-Evaluated-Points.
	remoteReadSampleLimit     = 0       // No limit
	remoteReadMaxBytesInFrame = 1048576 // 1 MiB (Prometheus default)
)

var regexErrInvalidMatcher = regexp.MustCompile(fmt.Sprintf("^%s", remotestorage.ErrInvalidMatcher))

// API it the SquirrelDB HTTP API server.
type API struct {
	ListenAddress               string
	Index                       types.Index
	Reader                      types.MetricReader
	Writer                      types.MetricWriter
	MutableLabelWriter          mutable.LabelWriter
	FlushCallback               func() error
	PreAggregateCallback        func(ctx context.Context, thread int, from, to time.Time) error
	MaxConcurrentRemoteRequests int
	PromQLMaxEvaluatedPoints    uint64
	MetricRegistry              prometheus.Registerer
	PromQLMaxEvaluatedSeries    uint32
	Logger                      zerolog.Logger

	ready      int32
	router     http.Handler
	listenPort int
	metrics    *metrics
}

// NewPrometheus returns a new initialized Prometheus web API.
// Only remote read/write and PromQL endpoints are implemented.
// appendable can be empty if remote write is not used.
func NewPrometheus(
	queryable storage.SampleAndChunkQueryable,
	appendable storage.Appendable,
	maxConcurrent int,
	metricRegistry prometheus.Registerer,
	apiLogger zerolog.Logger,
) *v1.API {
	queryLogger := apiLogger.With().Str("component", "query_engine").Logger()

	queryEngine := ppromql.NewEngine(ppromql.EngineOpts{
		Logger:             logger.NewKitLogger(&queryLogger),
		Reg:                metricRegistry,
		MaxSamples:         50000000,
		Timeout:            2 * time.Minute,
		ActiveQueryTracker: nil,
		LookbackDelta:      5 * time.Minute,
	})

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
	)

	return api
}

func (a *API) init() {
	a.metrics = newMetrics(a.MetricRegistry)

	router := route.New()

	router.Get("/metrics", promhttp.Handler().ServeHTTP)
	router.Get("/flush", a.flushHandler)
	router.Get("/ready", a.readyHandler)
	router.Get("/debug/", a.debugHelpHandler)
	router.Get("/debug/index_info", a.indexInfoHandler)
	router.Get("/debug/index_verify", a.indexVerifyHandler)
	router.Get("/debug/index_dump", a.indexDumpHandler)
	router.Get("/debug/index_dump_by_expiration", a.indexDumpByExpirationDateHandler)
	router.Get("/debug/index_dump_by_shard", a.indexDumpByShardHandler)
	router.Get("/debug/index_dump_by_posting", a.indexDumpByPostingHandler)
	router.Get("/debug/preaggregate", a.aggregateHandler)
	router.Get("/debug_preaggregate", a.aggregateHandler)
	router.Get("/debug/update_shard_expiration", a.indexUpdateShardExpirationHandler)
	router.Get("/debug/pprof/*item", http.DefaultServeMux.ServeHTTP)

	router.Post("/mutable/names", a.mutableLabelNamesWriteHandler)
	router.Del("/mutable/names", a.mutableLabelNamesDeleteHandler)
	router.Post("/mutable/values", a.mutableLabelValuesWriteHandler)
	router.Del("/mutable/values", a.mutableLabelValuesDeleteHandler)

	queryable := promql.NewStore(
		a.Index,
		a.Reader,
		a.PromQLMaxEvaluatedSeries,
		a.PromQLMaxEvaluatedPoints,
		a.MetricRegistry,
	)

	maxConcurrent := a.MaxConcurrentRemoteRequests
	if maxConcurrent <= 0 {
		maxConcurrent = runtime.GOMAXPROCS(0) * 2
	}

	appendable := remotestorage.New(a.Writer, a.Index, maxConcurrent, a.MetricRegistry)

	api := NewPrometheus(
		queryable,
		appendable,
		maxConcurrent,
		a.MetricRegistry,
		a.Logger,
	)

	// Wrap the router to add the http request to the context so the querier can access the HTTP headers.
	routerWrapper := router.WithInstrumentation(func(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
		operation := strings.Trim(handlerName, "/")

		h := func(rw http.ResponseWriter, r *http.Request) {
			t0 := time.Now()
			defer func() {
				a.metrics.RequestsSeconds.WithLabelValues(operation).Observe(time.Since(t0).Seconds())
			}()

			ctx := r.Context()
			r = r.WithContext(types.WrapContext(ctx, r))

			// Prometheus always returns a status 500 when write fails, but if
			// the labels are invalid we want to return a status 400, so we use the
			// interceptor to change the returned status in this case.
			if operation == "write" {
				rw = &interceptor{OrigWriter: rw}
			}

			handler(rw, r)
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

func (a *API) debugHelpHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(w, "The following debug enpoints exists")
	fmt.Fprintln(w, "/debug/index_info: provide global information")
	fmt.Fprintln(w, "/debug/index_info?metricID=XXX: provide information on given metric ID")
	fmt.Fprintln(w, "/debug/index_info?verbose&metricID=XXX: provide more information on given metric ID")
	fmt.Fprintln(w, "/debug/index_info?metricLabels=XXX: provide information on given metric labels")
	fmt.Fprintln(w, "/debug/index_dump: all known metrics")
	fmt.Fprintln(w, "/debug/index_dump_by_expiration?date=2006-01-02: metrics to check for expiration at given date")
	fmt.Fprintln(w, "/debug/index_dump_by_shard?shard_time=2006-01-02: metrics in given shard")
	fmt.Fprintln(w, "/debug/index_dump_by_posting?shard_time=2006-01-02&name=disk_used&value=/: metrics in given posting")
	fmt.Fprintln(w, "/debug/index_dump_by_posting?shard_time=2006-01-02&name=disk_used: metrics in given postings ")
	fmt.Fprintln(w, "/debug/update_shard_expiration?ttlDays=365: update all shard expirations with a ttl of one year")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "With the dump_by_posting you can omit the shard_time, which will query the special global shard.")
	fmt.Fprintln(w, "This allows to query the globalShardNumber which contains few special values (like all ")
	fmt.Fprintln(w, "assigned ID, existing shard,...)")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "To use /debug/index_info?metricLabels=XXX, you need to encode the labels. The easiest is using curl")
	fmt.Fprintln(w, "curl http://localhost:9201/debug/index_info -G -d metricLabels='item=\"/\",__name__=\"disk_used\"")
	fmt.Fprintln(w, "If curl complains about bad URL format, remove whitespace after each \",\"")
}

func (a API) indexInfoHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexDumper); ok {
		metricIDText := req.URL.Query()["metricID"]
		metricLabelsText := req.URL.Query()["metricLabels"]

		switch {
		case len(metricLabelsText) > 0:
			a.indexInfoLabels(ctx, w, idx, metricLabelsText)
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

func (a API) indexInfoLabels(
	ctx context.Context,
	w http.ResponseWriter,
	idx types.IndexDumper,
	metricLabelsText []string,
) {
	for _, text := range metricLabelsText {
		metricLabels, err := textToLabels(text)
		if err != nil {
			http.Error(w, fmt.Sprintf("Index info failed: %v", err), http.StatusInternalServerError)

			return
		}

		if err := idx.InfoByLabels(ctx, w, metricLabels); err != nil {
			http.Error(w, fmt.Sprintf("Index info failed: %v", err), http.StatusInternalServerError)

			return
		}
	}
}

func (a API) indexInfoIDs(
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

func (a API) indexVerifyHandler(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	ctx := req.Context()

	if idx, ok := a.Index.(types.IndexVerifier); ok {
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

func (a API) indexDumpHandler(w http.ResponseWriter, req *http.Request) {
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

func (a API) indexDumpByExpirationDateHandler(w http.ResponseWriter, req *http.Request) {
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

func (a API) indexDumpByShardHandler(w http.ResponseWriter, req *http.Request) {
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

	var date time.Time

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

func (a API) indexDumpByPostingHandler(w http.ResponseWriter, req *http.Request) {
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

func (a API) indexUpdateShardExpirationHandler(w http.ResponseWriter, req *http.Request) {
	ttlRaw := req.URL.Query().Get("ttlDays")
	if ttlRaw == "" {
		http.Error(w, `param "ttlDays" required"`, http.StatusBadRequest)

		return
	}

	ttlDays, err := strconv.Atoi(ttlRaw)
	if err != nil {
		http.Error(w, fmt.Sprintf(`can't convert "ttlDays" to int: %s"`, err), http.StatusBadRequest)

		return
	}

	ttl := time.Duration(ttlDays) * 24 * time.Hour

	if index, ok := a.Index.(types.IndexInternalShardExpirer); ok {
		if err := index.InternalUpdateAllShards(req.Context(), ttl); err != nil {
			http.Error(w, fmt.Sprintf("Update shard failed: %v", err), http.StatusInternalServerError)

			return
		}
	} else {
		http.Error(w, "Index does not implement DumpByExpirationDate()", http.StatusNotImplemented)

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

func (a API) mutableLabelValuesWriteHandler(w http.ResponseWriter, req *http.Request) {
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

func (a API) mutableLabelValuesDeleteHandler(w http.ResponseWriter, req *http.Request) { //nolint:dupl
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

func (a API) mutableLabelNamesWriteHandler(w http.ResponseWriter, req *http.Request) { //nolint:dupl
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

func (a API) mutableLabelNamesDeleteHandler(w http.ResponseWriter, req *http.Request) {
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
	if i.status == http.StatusInternalServerError && regexErrInvalidMatcher.Match(b) {
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

func textToLabels(text string) (labels.Labels, error) {
	matchers, err := parser.ParseMetricSelector("{" + text + "}")
	if err != nil {
		return nil, fmt.Errorf("fail to parse labels: %w", err)
	}

	builder := labels.NewBuilder(nil)
	for _, v := range matchers {
		builder.Set(v.Name, v.Value)
	}

	return builder.Labels(nil), nil
}

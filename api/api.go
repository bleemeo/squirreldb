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

package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec,gci
	"net/url"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/api/promql"
	"github.com/bleemeo/squirreldb/api/requestscoped"
	"github.com/bleemeo/squirreldb/appendable/requestappender"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/types"

	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/gate"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/rs/zerolog"
)

const (
	httpServerShutdownTimeout = 10 * time.Second

	// A read sample limit is already implemented dynamically with the header X-SquirrelDB-Max-Evaluated-Points.
	remoteReadSampleLimit     = 0       // No limit
	remoteReadMaxBytesInFrame = 1048576 // 1 MiB (Prometheus default)
)

var allowedProtoMsgs = []config.RemoteWriteProtoMsg{ //nolint:gochecknoglobals
	config.RemoteWriteProtoMsgV1,
	config.RemoteWriteProtoMsgV2,
}

var (
	regexErrInvalidLabel        = regexp.MustCompile(fmt.Sprintf("^%s", requestappender.ErrInvalidLabel))
	regexErrMissingTenantHeader = regexp.MustCompile(fmt.Sprintf("^%s", requestscoped.ErrMissingTenantHeader))
)

// TSDBDebugEndpoint define the (legacy) troubleshooting endpoints exposed by API.
// Because newer backend/tsdb might not implement thing the same way (e.g. no index),
// This should be deprecated and replaced by another approch.
type TSDBDebugEndpoint interface {
	ForcePreAggregation(
		ctx context.Context,
		threadCount int,
		from time.Time,
		to time.Time,
		matchers []*labels.Matcher,
	) error
	Flush(ctx context.Context) error
	Index() types.Index
}

// API it the SquirrelDB HTTP API server.
type API struct {
	ListenAddress string
	// AdminListenAddress, when set, moves the admin endpoints (/debug/ and
	// /mutable/) to a dedicated listener on this address: they are no longer
	// served on ListenAddress. When empty, everything is served on ListenAddress.
	AdminListenAddress string
	Queryable          types.QueryableWithOptions
	Appendable         types.AppendableWithOptions
	// OptionsParser (optional) parses per-request backend options from HTTP headers;
	// DebugEndpoint (optional) exposes the legacy index/flush/pre-aggregate debug
	// operations. Both are backend capabilities, set only when the backend provides
	// them (nil otherwise).
	OptionsParser               types.HTTPOptionsParser
	DebugEndpoint               TSDBDebugEndpoint
	MutableLabelWriter          MutableLabelInterface
	DefaultTTL                  time.Duration
	MaxConcurrentRemoteRequests int
	// MaxRequestBodySizeBytes defines the maximum size of incoming requests body.
	MaxRequestBodySizeBytes  int64
	PromQLMaxEvaluatedPoints uint64
	MetricRegistry           prometheus.Registerer
	PromQLMaxEvaluatedSeries uint32
	TenantLabelName          string
	MutableLabelDetector     requestscoped.MutableLabelDetector
	// When enabled, return an response to queries and write
	// requests that don't provide the tenant header.
	RequireTenantHeader bool
	// BackendDebug, when set, registers backend-specific debug endpoints under
	// /debug/<BackendName>/. BackendName identifies the backend (e.g. "clickhouse")
	// so two backends never collide on the same relative path.
	BackendDebug BackendDebugRegistrar
	BackendName  string
	// RoutedBackendDebug (optional) registers the debug endpoints of the routed
	// backends (see docs/routing.md), by backend name, under /debug/<name>/.
	RoutedBackendDebug map[string]BackendDebugRegistrar
	// BackendResolver (optional) validates the backend a request is routed to
	// (X-SquirrelDB-Backend header, routing rules and backends of the
	// authenticated user) when the request starts.
	BackendResolver requestscoped.BackendResolver
	Logger          zerolog.Logger
	// TLS, when enabled, serves both listeners over TLS.
	TLS auth.TLSOptions
	// Auth configures the authentication of both listeners. Its MetricRegistry and
	// Logger are ignored, the ones of the API are used.
	Auth auth.Options

	ready                 atomic.Int32
	authenticator         *auth.Authenticator
	tlsConfig             *tls.Config
	router                http.Handler
	listenPort            int
	adminListenPort       int
	metrics               *metrics
	backendDebugEndpoints []backendDebugEndpoint
	// adminRouter serves the admin endpoints on AdminListenAddress. It is nil
	// when AdminListenAddress is empty, the admin endpoints then being on router.
	adminRouter http.Handler
	// handler and adminHandler serve the main and admin listeners: they add the
	// authentication and the readiness check to router and adminRouter.
	handler      http.Handler
	adminHandler http.Handler

	FuturePointsBackdateOffset time.Duration
}

// BackendDebugRegistrar is implemented by the storage backend to register its
// debug HTTP endpoints. The API calls it with a register function that prepends
// /debug/<BackendName>/ to each (relative) path; the description feeds the
// /debug/ index.
type BackendDebugRegistrar interface {
	RegisterHTTPEndpoints(register func(method, path, description string, handler http.HandlerFunc))
}

type backendDebugEndpoint struct {
	method      string
	path        string
	description string
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
	apiLogger zerolog.Logger,
) *v1.API {
	queryLogger := apiLogger.With().Str("component", "query_engine").Logger()

	queryEngine := promql.NewEngine(queryLogger, metricRegistry)
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

	otlpDeltaToCumulative := false
	ctZeroIngestionEnabled := false

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
		logger.NewSLogger(apiLogger),
		rulesRetrieverFunc,
		remoteReadSampleLimit,
		maxConcurrent,
		remoteReadMaxBytesInFrame,
		false, // isAgent
		CORSOrigin,
		runtimeInfoFunc,
		&v1.PrometheusVersion{},
		nil, // notificationsGetter - we currently have no UI
		nil, // notificationsSub - same here
		mockGatherer{},
		metricRegistry,
		nil, // statsRenderer
		rwEnabled,
		allowedProtoMsgs,
		otlpEnabled,
		otlpDeltaToCumulative,
		ctZeroIngestionEnabled,
		false, // ctZeroIngestionEnabled
		time.Duration(time.Duration(5).Minutes()), // lookbackDelta
		false, // enableTypeAndUnitLabels
		nil,   // OverrideErrorCode
	)

	return api
}

func (a *API) init(ctx context.Context) error {
	a.metrics = newMetrics(a.MetricRegistry)

	authOptions := a.Auth
	authOptions.MetricRegistry = a.MetricRegistry
	authOptions.Logger = a.Logger.With().Str("component", "auth").Logger()

	authenticator, err := auth.New(authOptions)
	if err != nil {
		return fmt.Errorf("authentication: %w", err)
	}

	a.authenticator = authenticator

	a.tlsConfig, err = auth.NewTLSConfig(a.TLS)
	if err != nil {
		return err
	}

	maxConcurrent := a.MaxConcurrentRemoteRequests
	if maxConcurrent <= 0 {
		maxConcurrent = runtime.GOMAXPROCS(0) * 2
	}

	// remoteWriteGate limits the number of concurrent remote write requests. It is acquired and
	// released in the HTTP middleware (around the whole request), so a slot is always released
	// even if the underlying Appender's Commit/Rollback is never called.
	remoteWriteGate := gate.New(maxConcurrent)

	storeOption := requestscoped.Options{
		DefaultMaxEvaluatedSeries: a.PromQLMaxEvaluatedSeries,
		DefaultMaxEvaluatedPoints: a.PromQLMaxEvaluatedPoints,
		DefaultTTL:                int64(a.DefaultTTL.Seconds()),
		TenantLabelName:           a.TenantLabelName,
		RequireTenantHeader:       a.RequireTenantHeader,
		Queryable:                 a.Queryable,
		Appendable:                a.Appendable,
		OptionsParser:             a.OptionsParser,
		MutableLabelDetector:      a.MutableLabelDetector,
		BackendResolver:           a.BackendResolver,
		MetricRegistry:            a.MetricRegistry,
	}

	requestScopedStore := requestscoped.NewStore(storeOption)

	router := route.New()

	// The admin endpoints share the main router unless they have their own listener.
	adminRouter := router
	if a.AdminListenAddress != "" {
		adminRouter = route.New()
		a.adminRouter = adminRouter

		a.registerCommonRoutes(adminRouter)
	}

	a.registerCommonRoutes(router)
	a.registerAdminRoutes(adminRouter, requestScopedStore)

	api := NewPrometheus(
		requestScopedStore,
		requestScopedStore,
		maxConcurrent,
		a.MetricRegistry,
		a.Logger,
	)

	patchRemoteWriteHandler(api, a.FuturePointsBackdateOffset)

	// Wrap the router to add the http request to the context so the querier can access the HTTP headers.
	routerWrapper := router.WithInstrumentation(func(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
		operation := strings.Trim(handlerName, "/")

		h := func(rw http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(rw, r.Body, a.MaxRequestBodySizeBytes)

			t0 := time.Now()

			defer func() {
				a.metrics.RequestsSeconds.WithLabelValues(operation).Observe(time.Since(t0).Seconds())
			}()

			ctx, debugLevel, err := requestScopedStore.ContextWithRequest(r.Context(), r, operation)
			if err != nil {
				status := http.StatusUnprocessableEntity

				switch {
				case errors.Is(err, types.ErrBackendNotSelectable):
					status = http.StatusBadRequest
				case errors.Is(err, types.ErrBackendNotAllowed):
					// The user is authenticated but routed to a backend it may not use.
					status = http.StatusForbidden

					a.authenticator.CountBackendNotAllowed()
				}

				a.respondError(rw, status, err)

				return
			}

			ctx = promql.SetDebugLevel(ctx, debugLevel)

			r = r.WithContext(ctx)

			// Prometheus always returns a status 500 when write fails, but if
			// the labels are invalid we want to return a status 400, so we use the
			// interceptor to change the returned status in this case.
			if operation == "write" {
				// Limit the number of concurrent remote write requests. Acquiring and releasing
				// here (rather than in the Appender) guarantees the slot is freed when the request
				// ends, even if the Appender's Commit/Rollback is never called.
				if err := remoteWriteGate.Start(ctx); err != nil {
					a.respondError(rw, http.StatusServiceUnavailable,
						fmt.Errorf("too many concurrent remote write: %w", err))

					return
				}

				defer remoteWriteGate.Done()

				rw = &interceptor{OrigWriter: rw}
			}

			handler(rw, r)

			if debugLevel >= types.DebugLevelSimple {
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

	// The authentication wraps each listener: on the main listener the scope depends
	// on the route, on the admin listener every route except /ready and /metrics
	// requires the admin scope.
	a.handler = a.authenticator.Middleware(auth.ClassifyRoute, http.HandlerFunc(
		func(w http.ResponseWriter, req *http.Request) { a.serveWhenReady(a.router, w, req) },
	))

	if a.adminRouter != nil {
		a.adminHandler = a.authenticator.Middleware(auth.ClassifyAdminRoute, http.HandlerFunc(
			func(w http.ResponseWriter, req *http.Request) { a.serveWhenReady(a.adminRouter, w, req) },
		))
	}

	return nil
}

// registerCommonRoutes registers the routes served on every listener.
func (a *API) registerCommonRoutes(router *route.Router) {
	router.Get("/metrics", promhttp.Handler().ServeHTTP)
	router.Get("/ready", a.readyHandler)
}

// registerAdminRoutes registers the admin endpoints: the troubleshooting and
// maintenance endpoints under /debug/ (including the backend-specific ones) and
// the mutable labels write API. They are served on AdminListenAddress when set.
func (a *API) registerAdminRoutes(router *route.Router, requestScopedStore *requestscoped.Store) {
	router.Get("/debug/", a.debugHelpHandler)
	router.Get("/debug/index_info", a.indexInfoHandler)
	a.registerMutatingRoute(router, "/debug/flush", a.flushHandler, nil)
	// index_verify only changes the index when asked to fix it.
	a.registerMutatingRoute(router, "/debug/index_verify", a.indexVerifyHandler, func(r *http.Request) bool {
		return r.URL.Query().Get("fix") != ""
	})
	router.Get("/debug/index_dump", a.indexDumpHandler)
	router.Get("/debug/index_dump_by_labels", a.indexDumpByLabelsHandler)
	router.Get("/debug/index_dump_by_expiration", a.indexDumpByExpirationDateHandler)
	router.Get("/debug/index_dump_by_shard", a.indexDumpByShardHandler)
	router.Get("/debug/index_dump_by_posting", a.indexDumpByPostingHandler)
	a.registerMutatingRoute(router, "/debug/index_block", a.indexBlockHandler, nil)
	a.registerMutatingRoute(router, "/debug/index_unblock", a.indexUnblockHandler, nil)
	a.registerMutatingRoute(router, "/debug/toggle_debug_query", func(w http.ResponseWriter, _ *http.Request) {
		debugRequest := requestScopedStore.ToggleDebug()

		fmt.Fprintf(w, "defaultDebugRequest is now %v\n", debugRequest)
	}, nil)
	a.registerMutatingRoute(router, "/debug/preaggregate", a.aggregateHandler, nil)
	router.Get("/debug/mutable_dump", a.mutableDumpHandler)
	router.Post("/debug/mutable_import", a.mutableImportHandler)
	router.Get("/debug/pprof/*item", http.DefaultServeMux.ServeHTTP)

	if a.BackendDebug != nil && a.BackendName != "" {
		a.registerBackendDebug(router, a.BackendName, a.BackendDebug)
	}

	for _, name := range slices.Sorted(maps.Keys(a.RoutedBackendDebug)) {
		a.registerBackendDebug(router, name, a.RoutedBackendDebug[name])
	}

	router.Post("/mutable/names", a.mutableLabelNamesWriteHandler)
	router.Del("/mutable/names", a.mutableLabelNamesDeleteHandler)
	router.Post("/mutable/values", a.mutableLabelValuesWriteHandler)
	router.Del("/mutable/values", a.mutableLabelValuesDeleteHandler)
}

// registerMutatingRoute registers an endpoint that changes state on POST. It is
// also registered on GET for backward compatibility, a GET request that changes
// state logging a deprecation warning (once per endpoint). isMutating tells
// whether a request changes state; nil means every request does.
func (a *API) registerMutatingRoute(
	router *route.Router,
	path string,
	handler http.HandlerFunc,
	isMutating func(r *http.Request) bool,
) {
	var warnOnce sync.Once

	router.Post(path, handler)
	router.Get(path, func(w http.ResponseWriter, r *http.Request) {
		if isMutating == nil || isMutating(r) {
			warnOnce.Do(func() {
				a.Logger.Warn().Msgf("GET %s is deprecated because it changes state, use POST instead", path)
			})
		}

		handler(w, r)
	})
}

// registerBackendDebug registers the debug endpoints of a backend under
// /debug/<name>/.
func (a *API) registerBackendDebug(router *route.Router, name string, backend BackendDebugRegistrar) {
	prefix := "/debug/" + name

	backend.RegisterHTTPEndpoints(func(method, path, description string, handler http.HandlerFunc) {
		full := prefix + path

		switch method {
		case http.MethodGet:
			router.Get(full, handler)
		case http.MethodPost:
			router.Post(full, handler)
		default:
			a.Logger.Warn().Msgf("ignoring backend debug endpoint %s: unsupported method %q", full, method)

			return
		}

		a.backendDebugEndpoints = append(a.backendDebugEndpoints,
			backendDebugEndpoint{method: method, path: full, description: description})
	})
}

// Run start the HTTP api server.
// When AdminListenAddress is set, it also starts the admin server. Readiness is
// only signalled once both addresses are bound, and both servers are stopped together.
func (a *API) Run(ctx context.Context, readiness chan error) {
	err := a.init(ctx)
	if err != nil {
		readiness <- err

		return
	}

	servers := []*apiServer{{name: "API", address: a.ListenAddress, handler: a, port: &a.listenPort}}

	if a.adminRouter != nil {
		servers = append(servers, &apiServer{
			name:    "admin",
			address: a.AdminListenAddress,
			handler: http.HandlerFunc(a.serveAdminHTTP),
			port:    &a.adminListenPort,
		})
	}

	for i, srv := range servers {
		if err := srv.listen(ctx); err != nil {
			for _, started := range servers[:i] {
				_ = started.listener.Close()
			}

			readiness <- err

			return
		}
	}

	serverStopped := make(chan error, len(servers))

	for _, srv := range servers {
		go func() {
			defer logger.ProcessPanic()

			if a.tlsConfig != nil {
				// Each server gets its own copy because ServeTLS may update it.
				srv.server.TLSConfig = a.tlsConfig.Clone()
				// The certificate is provided by TLSConfig.
				serverStopped <- srv.server.ServeTLS(srv.listener, "", "")

				return
			}

			serverStopped <- srv.server.Serve(srv.listener)
		}()
	}

	scheme := "HTTP"
	if a.tlsConfig != nil {
		scheme = "HTTPS"
	}

	a.Logger.Info().Msgf(
		"Server listening on %s (%s, authentication %s)",
		a.ListenAddress, scheme, enabledText(a.authenticator.Enabled()),
	)

	for _, srv := range servers {
		a.warnInsecureListener(srv)
	}

	if a.adminRouter != nil {
		a.Logger.Info().Msgf("Admin endpoints (/debug/, /mutable/) served on %s only", a.AdminListenAddress)
	} else {
		a.Logger.Info().Msgf(
			"Admin endpoints (/debug/, /mutable/) served on %s, set admin_listen_address to serve them separately",
			a.ListenAddress,
		)
	}

	readiness <- nil

	// Stop all servers when one of them stops, as if the context was cancelled.
	var stopErrs []error

	select {
	case <-ctx.Done():
	case err := <-serverStopped:
		stopErrs = append(stopErrs, err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpServerShutdownTimeout)
	defer cancel()

	for _, srv := range servers {
		if err := srv.server.Shutdown(shutdownCtx); err != nil { //nolint: contextcheck
			a.Logger.Err(err).Msgf("Failed stop the %s HTTP server", srv.name)
		}
	}

	for len(stopErrs) < len(servers) {
		stopErrs = append(stopErrs, <-serverStopped)
	}

	for _, err := range stopErrs {
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.Logger.Err(err).Msg("HTTP server failed")
		}
	}

	a.Logger.Debug().Msg("Server stopped")
}

// apiServer is one of the HTTP servers started by Run.
type apiServer struct {
	name    string
	address string
	handler http.Handler
	// port is set to the port actually listened on.
	port *int

	listener net.Listener
	server   *http.Server
}

func (s *apiServer) listen(ctx context.Context) error {
	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", s.address)
	if err != nil {
		return fmt.Errorf("%s server: %w", s.name, err)
	}

	if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok {
		*s.port = tcpAddr.Port
	}

	s.listener = ln
	s.server = &http.Server{
		Addr:              s.address,
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return nil
}

// warnInsecureListener warns when a server is reachable from the network without
// authentication, or with authentication but without TLS.
func (a *API) warnInsecureListener(srv *apiServer) {
	if isLoopback(srv.listener.Addr()) {
		return
	}

	switch {
	case !a.authenticator.Enabled():
		a.Logger.Warn().Msgf(
			"Authentication is disabled and the %s server listens on %s, which is reachable from the network: "+
				"anyone who can connect can read and write the metrics of every tenant%s. "+
				"Enable auth or listen on localhost, see docs/authentication.md",
			srv.name, srv.listener.Addr(), adminWarning(srv, a.adminRouter == nil),
		)
	case a.tlsConfig == nil:
		a.Logger.Warn().Msgf(
			"Authentication is enabled without TLS and the %s server listens on %s, which is reachable from "+
				"the network: passwords and tokens are sent in clear text. Configure tls, see docs/authentication.md",
			srv.name, srv.listener.Addr(),
		)
	}
}

func adminWarning(srv *apiServer, adminOnMainListener bool) string {
	if srv.name == "admin" || adminOnMainListener {
		return " and use the admin endpoints (/debug/, /mutable/)"
	}

	return ""
}

func enabledText(enabled bool) string {
	if enabled {
		return "enabled"
	}

	return "disabled"
}

// isLoopback returns whether the address only accepts local connections.
// An unspecified address (e.g. ":9201" or "0.0.0.0:9201") listens on every interface.
func isLoopback(addr net.Addr) bool {
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return false
	}

	return tcpAddr.IP.IsLoopback()
}

// ListenPort return the port listenning on. Should not be used before Run()
// signalled its readiness.
// This is useful for tests that use port "0" to known the actual listenning port.
func (a *API) ListenPort() int {
	return a.listenPort
}

// AdminListenPort return the port the admin server is listening on, or 0 when
// AdminListenAddress is empty. Should not be used before Run() signalled its readiness.
func (a *API) AdminListenPort() int {
	return a.adminListenPort
}

// Ready mark the system as ready to access requests.
func (a *API) Ready() {
	a.ready.Store(1)
}

func (a *API) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	a.handler.ServeHTTP(w, req)
}

// serveAdminHTTP serves the admin endpoints of the admin listener.
func (a *API) serveAdminHTTP(w http.ResponseWriter, req *http.Request) {
	a.adminHandler.ServeHTTP(w, req)
}

func (a *API) serveWhenReady(handler http.Handler, w http.ResponseWriter, req *http.Request) {
	if a.ready.Load() > 0 {
		handler.ServeHTTP(w, req)

		return
	}

	http.Error(w, "Server not yet ready", http.StatusServiceUnavailable)
}

func (a *API) flushHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if a.DebugEndpoint != nil {
		err := a.DebugEndpoint.Flush(r.Context())
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

Endpoints that change state are marked with POST: call them with POST
(e.g. curl -XPOST http://localhost:9201/debug/flush). Most of them still accept GET for backward
compatibility, but this is deprecated.
When admin_listen_address is set, these endpoints are served on that address only.

/debug/index_info
	provide global information about index

/debug/index_info?metricID=XXX
	provide information on given metric ID

/debug/index_verify
	Verify the index consistency. Use "lock" to hold the index lock during the verification and
	"strict" for stricter checks. "now" (format 2006-01-02) overrides the date used by the checks.

POST /debug/index_verify?fix=1
	Same as above but also fix the inconsistencies found.

POST /debug/toggle_debug_query
	Enable or disable some tracing of query submitted to SquirrelDB. This could be very verbose.
	By using the header X-SquirrelDB-Query-Debug you could enable tracing for a single query.

POST /debug/preaggregate?from=2006-01-02&to=2006-01-02
	(Re)-run the preaggragation on specified day(s). If you inserted data with timestamp older than
	ingestion window (8 hours), the pre-aggregation might already have processed that day.
	This endpoints allow to force re-running the pre-aggregations on that day.

/debug/pprof/
	Golang pprof endpoints

POST /debug/flush
	Write metric from the memory store to Cassandra.
	SquirrelDB first write incoming points to a memory store with fast random access (Redis or local memory),
	and after some time write them to Cassandra. This is done to improve compression and reduce number of writes
	to Cassandra.
	You can force flushing this memory store to Cassandra with this endpoints. This only apply to this SquirrelDB
	instance, so in a cluster you need to call this endpoint on all SquirrelDB instance.

/debug/mutable_dump
	Produce a CSV with all known mutable labels.

POST /debug/mutable_import
	Remove all known mutable labels and add all labels provided by input CSV.
	Example of curl to export/import:
	$ curl http://localhost:9201/debug/mutable_dump > file.csv
	$ curl http://localhost:9201/debug/mutable_import --data-binary @file.csv

POST /debug/index_block
	Block write on Cassandra for 5 minutes. Use /debug/index_unblock to
	release the block before the 5 minutes delay. Re-use /debug/index_block
	to extend to block duration.

POST /debug/index_unblock
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

	for _, e := range a.backendDebugEndpoints {
		fmt.Fprintf(w, "\n%s %s\n\t%s\n", e.method, e.path, e.description)
	}
}

func (a *API) getIndexDumperOrReplyError(w http.ResponseWriter) types.IndexDumper {
	var index types.Index

	if a.DebugEndpoint != nil {
		index = a.DebugEndpoint.Index()
	} else {
		http.Error(w, "backend does not implement TSDBDebugEndpoint", http.StatusNotImplemented)

		return nil
	}

	if debugIndex, ok := index.(types.IndexDumper); ok {
		return debugIndex
	}

	http.Error(w, "Index does not implement IndexDumper interface", http.StatusNotImplemented)

	return nil
}

func (a *API) getIndexOrReplyError(w http.ResponseWriter) types.Index {
	var index types.Index

	if a.DebugEndpoint != nil {
		index = a.DebugEndpoint.Index()
	} else {
		http.Error(w, "backend does not implement TSDBDebugEndpoint", http.StatusNotImplemented)

		return nil
	}

	return index
}

func (a *API) indexInfoHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	debugIndex := a.getIndexDumperOrReplyError(w)
	if debugIndex == nil {
		return
	}

	metricIDText := req.URL.Query()["metricID"]

	switch {
	case len(metricIDText) > 0:
		a.indexInfoIDs(ctx, w, debugIndex, metricIDText)
	default:
		if err := debugIndex.InfoGlobal(ctx, w); err != nil {
			http.Error(w, fmt.Sprintf("Index info failed: %v", err), http.StatusInternalServerError)

			return
		}
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

	index := a.getIndexOrReplyError(w)
	if index == nil {
		return
	}

	if idx, ok := index.(types.VerifiableIndex); ok { //nolint:nestif
		doFix := req.URL.Query().Get("fix") != ""
		acquireLock := req.URL.Query().Get("lock") != ""
		strict := req.URL.Query().Get("strict") != ""

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

	idx := a.getIndexDumperOrReplyError(w)
	if idx == nil {
		return
	}

	if err := idx.Dump(ctx, w); err != nil {
		http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

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

	idx := a.getIndexDumperOrReplyError(w)
	if idx == nil {
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

	idx := a.getIndexDumperOrReplyError(w)
	if idx == nil {
		return
	}

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
}

func (a *API) indexDumpByShardHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	idx := a.getIndexDumperOrReplyError(w)
	if idx == nil {
		return
	}

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

	idx := a.getIndexDumperOrReplyError(w)
	if idx == nil {
		return
	}

	date, name, value, errorMessage := getIndexDumpByPostingArgument(req)
	if errorMessage != "" {
		http.Error(w, errorMessage, http.StatusBadRequest)

		return
	}

	if err := idx.DumpByPosting(ctx, w, date, name, value); err != nil {
		http.Error(w, fmt.Sprintf("Index dump failed: %v", err), http.StatusInternalServerError)

		return
	}
}

func (a *API) indexBlockHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	index := a.getIndexOrReplyError(w)
	if index == nil {
		return
	}

	if idx, ok := index.(types.IndexBlocker); ok {
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

	index := a.getIndexOrReplyError(w)
	if index == nil {
		return
	}

	if idx, ok := index.(types.IndexBlocker); ok {
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

func (a *API) aggregateHandler(w http.ResponseWriter, req *http.Request) {
	fromRaw := req.URL.Query().Get("from")
	toRaw := req.URL.Query().Get("to")
	labelsRaw := req.URL.Query().Get("labels")
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

	var matchers []*labels.Matcher

	if labelsRaw != "" {
		labelPairs := strings.Split(labelsRaw, ",")
		matchers = make([]*labels.Matcher, len(labelPairs))

		for i, labelPair := range labelPairs {
			kv := strings.SplitN(labelPair, "=", 2)
			if len(kv) != 2 {
				http.Error(w, fmt.Sprintf("Invalid label pair %q", labelPair), http.StatusBadRequest)

				return
			}

			matchers[i] = labels.MustNewMatcher(labels.MatchEqual, kv[0], kv[1]) // Can't fail with labels.MatchEqual
		}
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
	if a.DebugEndpoint != nil {
		err := a.DebugEndpoint.Flush(req.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf("Flush failed: %v", err), http.StatusInternalServerError)

			return
		}

		err = a.DebugEndpoint.ForcePreAggregation(ctx, thread, from, to, matchers)
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

		if slices.Contains(label.AssociatedValues, "") {
			errMsg := "associated values can't contain an empty string: %#v"
			http.Error(w, fmt.Sprintf(errMsg, label), http.StatusBadRequest)

			return
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

	if rc == http.StatusNoContent {
		// Write it now, because the Write() method won't be called.
		i.OrigWriter.WriteHeader(rc)
	}
}

func (i *interceptor) Write(b []byte) (int, error) {
	if i.status == http.StatusInternalServerError &&
		(regexErrInvalidLabel.Match(b) || regexErrMissingTenantHeader.Match(b)) {
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

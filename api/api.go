package api

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"squirreldb/api/promql"
	"squirreldb/api/remotestorage"
	"squirreldb/types"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
)

const (
	httpServerShutdownTimeout = 10 * time.Second
)

// API it the SquirrelDB HTTP API server.
type API struct {
	ListenAddress        string
	Index                types.Index
	Reader               types.MetricReader
	Writer               types.MetricWriter
	FlushCallback        func() error
	PreAggregateCallback func(from, to time.Time) error
	IndexVerifyCallback  func(ctx context.Context, w io.Writer, doFix bool, acquireLock bool) error

	ready  int32
	logger log.Logger
	router http.Handler
}

// Run start the HTTP api server.
func (a *API) Run(ctx context.Context, readiness chan error) {
	a.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	router := route.New()

	router.Get("/metrics", promhttp.Handler().ServeHTTP)
	router.Get("/flush", a.flushHandler)
	router.Get("/debug/index_verify", a.indexVerifyHandler)

	router.Get("/debug_preaggregate", a.aggregateHandler)

	promql := promql.PromQL{
		Index:  a.Index,
		Reader: a.Reader,
	}
	remote := remotestorage.RemoteStorage{
		Index:  a.Index,
		Reader: a.Reader,
		Writer: a.Writer,
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

	if err != nil && err != http.ErrServerClosed {
		_ = a.logger.Log("msg", "HTTP server failed", "err", err)
	}

	_ = level.Debug(a.logger).Log("msg", "server stopped")
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

	fmt.Fprintf(w, "Flush points (of this SquirrelDB instance) from temporary store to TSDB done in %v\n", time.Since(start))
}

func (a API) indexVerifyHandler(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	ctx := req.Context()

	if a.IndexVerifyCallback != nil {
		doFix := req.FormValue("fix") != ""
		acquireLock := req.FormValue("lock") != ""

		err := a.IndexVerifyCallback(ctx, w, doFix, acquireLock)
		if err != nil {
			http.Error(w, fmt.Sprintf("Index verification failed: %v", err), http.StatusInternalServerError)
			return
		}
	}

	fmt.Fprintf(w, "Index verification took %v\n", time.Since(start))
}

func (a API) aggregateHandler(w http.ResponseWriter, req *http.Request) {
	fromRaw := req.URL.Query().Get("from")
	toRaw := req.URL.Query().Get("to")

	from, err := time.Parse(time.RFC3339, fromRaw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	to, err := time.Parse(time.RFC3339, toRaw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()

	if a.PreAggregateCallback != nil {
		err := a.PreAggregateCallback(from, to)
		if err != nil {
			http.Error(w, "pre-aggregation failed", http.StatusInternalServerError)
			return
		}
	}

	fmt.Fprintf(w, "Pre-aggregation terminated in %v\n", time.Since(start))
}

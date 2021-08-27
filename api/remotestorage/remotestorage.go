package remotestorage

import (
	"context"
	"log"
	"net/http"
	"os"
	"runtime"
	"squirreldb/api/promql"
	"squirreldb/types"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[remotestorage] ", log.LstdFlags)

type RemoteStorage struct {
	Reader                   types.MetricReader
	Writer                   types.MetricWriter
	Index                    types.Index
	MaxConcurrentRemoteWrite int
	MetricRegisty            prometheus.Registerer
	APIRouter                *route.Router

	metrics *metrics
}

func (r *RemoteStorage) Init() {
	if r.metrics != nil {
		return
	}

	reg := r.MetricRegisty

	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	r.metrics = newMetrics(reg)
}

// Register the remote storage endpoints in the given router.
func (r *RemoteStorage) Register(router *route.Router) {
	r.Init()

	// We need to wrap the API router to add the http request to the context so
	// the querier can access the HTTP headers.
	apiRouterWrapper := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		req = req.WithContext(promql.WrapContext(ctx, req))

		// TODO: Remove this fix after the transition.
		// Temporary fix to support both "∕read" and "/api/v1/read"
		if req.URL.Path == "/read" || req.URL.Path == "/write" {
			req.URL.Path = "/api/v1" + req.URL.Path
		}

		r.APIRouter.ServeHTTP(rw, req)
	})

	// Instrument the router to get some metrics.
	router = router.WithInstrumentation(func(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
		operation := strings.Trim(handlerName, "/")

		h := func(rw http.ResponseWriter, req *http.Request) {
			t0 := time.Now()
			defer func() {
				r.metrics.RequestsSeconds.WithLabelValues(operation).Observe(time.Since(t0).Seconds())
			}()

			lrw := newLoggingResponseWriter(rw)
			handler(lrw, req)

			// On success, read always returns StatusOK and write returns StatusNoContent.
			if (operation == "read" && lrw.statusCode != http.StatusOK) ||
				(operation == "write" && lrw.statusCode != http.StatusNoContent) {
				logger.Printf("Error: %v", http.StatusText(lrw.statusCode))
				r.metrics.RequestsError.WithLabelValues(operation).Inc()
			}
		}

		return h
	})

	router.Post("/read", apiRouterWrapper)
	router.Post("/api/v1/read", apiRouterWrapper)
	router.Post("/write", apiRouterWrapper)
	router.Post("/api/v1/write", apiRouterWrapper)
}

// loggingResponseWriter wraps a response writer to get the response status code.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	// WriteHeader(int) is not called if our response implicitly returns 200 OK, so
	// we default to that status code.
	return &loggingResponseWriter{w, http.StatusOK}
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (r RemoteStorage) Appender(ctx context.Context) storage.Appender {
	maxConcurrent := r.MaxConcurrentRemoteWrite
	if maxConcurrent <= 0 {
		maxConcurrent = runtime.GOMAXPROCS(0) * 2
	}

	writeMetrics := &writeMetrics{
		index:    r.Index,
		writer:   r.Writer,
		reqCtxCh: make(chan *requestContext, maxConcurrent),
		metrics:  r.metrics,
	}

	for i := 0; i < maxConcurrent; i++ {
		writeMetrics.reqCtxCh <- &requestContext{
			pb: &prompb.WriteRequest{},
		}
	}

	return writeMetrics
}

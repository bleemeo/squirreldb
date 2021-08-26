package remotestorage

import (
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

	maxConcurrent := r.MaxConcurrentRemoteWrite
	if maxConcurrent <= 0 {
		maxConcurrent = runtime.GOMAXPROCS(0) * 2
	}

	// readMetrics := readMetrics{
	// 	index:   r.Index,
	// 	reader:  r.Reader,
	// 	metrics: r.metrics,
	// }

	writeMetrics := writeMetrics{
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
		handlerName = strings.Trim(handlerName, "/")

		h := func(rw http.ResponseWriter, req *http.Request) {
			t0 := time.Now()
			defer func() {
				r.metrics.RequestsSeconds.WithLabelValues(handlerName).Observe(time.Since(t0).Seconds())
			}()

			lrw := newLoggingResponseWriter(rw)
			handler(lrw, req)

			if lrw.statusCode != http.StatusOK {
				logger.Printf("Error: %v", http.StatusText(lrw.statusCode))
				r.metrics.RequestsError.WithLabelValues(handlerName).Inc()
			}
		}

		return h
	})

	router.Post("/read", apiRouterWrapper)
	router.Post("/api/v1/read", apiRouterWrapper)
	router.Post("/write", writeMetrics.ServeHTTP)
	router.Post("/api/v1/write", writeMetrics.ServeHTTP)
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

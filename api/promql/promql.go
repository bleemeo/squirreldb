package promql

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
)

type PromQL struct {
	MetricRegistry prometheus.Registerer
	APIRouter      *route.Router

	metrics *metrics
}

type RequestContextKey struct{}

// Register the API's endpoints in the given router.
func (p *PromQL) Register(r *route.Router) {
	p.metrics = newMetrics(p.MetricRegistry)

	// We need to wrap the API router to add the http request to the context so
	// the querier can access the HTTP headers.
	apiRouterWrapper := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		r = r.WithContext(WrapContext(ctx, r))

		p.APIRouter.ServeHTTP(rw, r)
	})

	// Instrument the router to get some metrics.
	r = r.WithInstrumentation(func(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
		handlerName = strings.Trim(handlerName, "/")

		h := func(rw http.ResponseWriter, r *http.Request) {
			t0 := time.Now()
			defer func() {
				p.metrics.RequestsSeconds.WithLabelValues(handlerName).Observe(time.Since(t0).Seconds())
			}()

			handler(rw, r)
		}

		return h
	})

	r.Get("/query", apiRouterWrapper)
	r.Post("/query", apiRouterWrapper)
	r.Get("/query_range", apiRouterWrapper)
	r.Post("/query_range", apiRouterWrapper)
	r.Get("/series", apiRouterWrapper)
	r.Post("/series", apiRouterWrapper)

	r.Get("/labels", apiRouterWrapper)
	r.Post("/labels", apiRouterWrapper)
	r.Get("/label/:name/values", apiRouterWrapper)
}

// WrapContext adds a request to the context.
func WrapContext(ctx context.Context, r *http.Request) context.Context {
	return context.WithValue(ctx, RequestContextKey{}, r)
}

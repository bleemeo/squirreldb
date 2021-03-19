package remotestorage

import (
	"log"
	"os"
	"runtime"
	"squirreldb/types"

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

	readMetrics := readMetrics{
		index:   r.Index,
		reader:  r.Reader,
		metrics: r.metrics,
	}
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

	router.Post("/read", readMetrics.ServeHTTP)
	router.Post("/write", writeMetrics.ServeHTTP)
}

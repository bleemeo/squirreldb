package remotestorage

import (
	"runtime"

	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/prompb"

	"log"
	"os"
	"squirreldb/types"
	"time"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[remotestorage] ", log.LstdFlags)

type Options struct {
	ListenAddress        string
	FlushCallback        func() error
	PreAggregateCallback func(from, to time.Time) error
}

type RemoteStorage struct {
	Reader                   types.MetricReader
	Writer                   types.MetricWriter
	Index                    types.Index
	MaxConcurrentRemoteWrite int
}

// Register the remote storage endpoints in the given router.
func (r RemoteStorage) Register(router *route.Router) {
	maxConcurrent := r.MaxConcurrentRemoteWrite
	if maxConcurrent <= 0 {
		maxConcurrent = runtime.GOMAXPROCS(0) * 2
	}

	readMetrics := readMetrics{
		index:  r.Index,
		reader: r.Reader,
	}
	writeMetrics := writeMetrics{
		index:    r.Index,
		writer:   r.Writer,
		reqCtxCh: make(chan *requestContext, maxConcurrent),
	}

	for i := 0; i < maxConcurrent; i++ {
		writeMetrics.reqCtxCh <- &requestContext{
			pb: &prompb.WriteRequest{},
		}
	}

	router.Post("/read", readMetrics.ServeHTTP)
	router.Post("/write", writeMetrics.ServeHTTP)
}

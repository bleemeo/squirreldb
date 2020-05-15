package remotestorage

import (
	"github.com/prometheus/common/route"

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
	Reader types.MetricReader
	Writer types.MetricWriter
	Index  types.Index
}

// Register the remote storage endpoints in the given router.
func (r RemoteStorage) Register(router *route.Router) {
	readMetrics := readMetrics{
		index:  r.Index,
		reader: r.Reader,
	}
	writeMetrics := writeMetrics{
		index:    r.Index,
		writer:   r.Writer,
		reqCtxCh: make(chan *requestContext, 4),
	}

	router.Post("/read", readMetrics.ServeHTTP)
	router.Post("/write", writeMetrics.ServeHTTP)
}

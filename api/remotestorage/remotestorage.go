package remotestorage

import (
	"context"
	"log"
	"os"
	"runtime"
	"squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[remotestorage] ", log.LstdFlags)

type RemoteStorage struct {
	writer                   types.MetricWriter
	index                    types.Index
	maxConcurrentRemoteWrite int
	metrics                  *metrics
}

// New returns a new initialized remote storage.
func New(
	writer types.MetricWriter,
	index types.Index,
	maxConcurrentRemoteWrite int,
	reg prometheus.Registerer,
) *RemoteStorage {
	remoteStorage := RemoteStorage{
		writer:                   writer,
		index:                    index,
		maxConcurrentRemoteWrite: maxConcurrentRemoteWrite,
		metrics:                  newMetrics(reg),
	}

	return &remoteStorage
}

func (r *RemoteStorage) Appender(ctx context.Context) storage.Appender {
	maxConcurrent := r.maxConcurrentRemoteWrite
	if maxConcurrent <= 0 {
		maxConcurrent = runtime.GOMAXPROCS(0) * 2
	}

	writeMetrics := &writeMetrics{
		index:             r.index,
		writer:            r.writer,
		reqCtxCh:          make(chan *requestContext, maxConcurrent),
		metrics:           r.metrics,
		pendingTimeSeries: make(map[uint64]timeSeries),
	}

	// TODO: unused, implement new concurrent requests limiter.
	for i := 0; i < maxConcurrent; i++ {
		writeMetrics.reqCtxCh <- &requestContext{
			pb: &prompb.WriteRequest{},
		}
	}

	return writeMetrics
}

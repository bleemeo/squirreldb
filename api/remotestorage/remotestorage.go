package remotestorage

import (
	"context"
	"fmt"
	"log"
	"os"
	"squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

//nolint:gochecknoglobals
var logger = log.New(os.Stdout, "[remotestorage] ", log.LstdFlags)

type RemoteStorage struct {
	writer          types.MetricWriter
	index           types.Index
	remoteWriteGate *gate.Gate
	metrics         *metrics
}

// New returns a new initialized appendable storage.
func New(
	writer types.MetricWriter,
	index types.Index,
	maxConcurrentRemoteWrite int,
	reg prometheus.Registerer,
) storage.Appendable {
	remoteStorage := RemoteStorage{
		writer:          writer,
		index:           index,
		remoteWriteGate: gate.New(maxConcurrentRemoteWrite),
		metrics:         newMetrics(reg),
	}

	return &remoteStorage
}

func (r *RemoteStorage) Appender(ctx context.Context) storage.Appender {
	// Limit concurrent writes, block here if too many concurrent writes are running.
	if err := r.remoteWriteGate.Start(ctx); err != nil {
		return errAppender{fmt.Errorf("too many concurrent remote write: %w", err)}
	}

	writeMetrics := &writeMetrics{
		index:             r.index,
		writer:            r.writer,
		metrics:           r.metrics,
		pendingTimeSeries: make(map[uint64]timeSeries),
		done:              r.remoteWriteGate.Done,
	}

	return writeMetrics
}

type errAppender struct {
	err error
}

func (a errAppender) Append(uint64, labels.Labels, int64, float64) (uint64, error) { return 0, a.err }
func (a errAppender) AppendExemplar(uint64, labels.Labels, exemplar.Exemplar) (uint64, error) {
	return 0, a.err
}
func (a errAppender) Commit() error   { return a.err }
func (a errAppender) Rollback() error { return a.err }

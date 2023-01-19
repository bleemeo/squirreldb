package remotestorage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/gate"
)

var errMissingRequest = errors.New("HTTP request not found in context")

type RemoteStorage struct {
	writer          types.MetricWriter
	index           types.Index
	remoteWriteGate *gate.Gate
	tenantLabelName string
	metrics         *metrics
}

// New returns a new initialized appendable storage.
func New(
	writer types.MetricWriter,
	index types.Index,
	maxConcurrentRemoteWrite int,
	tenantLabelName string,
	reg prometheus.Registerer,
) storage.Appendable {
	remoteStorage := RemoteStorage{
		writer:          writer,
		index:           index,
		tenantLabelName: tenantLabelName,
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

	// If the tenant header is present, the tenant label is added to all metrics written.
	request, ok := ctx.Value(types.RequestContextKey{}).(*http.Request)
	if !ok {
		return errAppender{errMissingRequest}
	}

	tenant := request.Header.Get(types.HeaderTenant)
	tenantLabel := labels.Label{Name: r.tenantLabelName, Value: tenant}

	writeMetrics := &writeMetrics{
		index:             r.index,
		writer:            r.writer,
		metrics:           r.metrics,
		tenantLabel:       tenantLabel,
		pendingTimeSeries: make(map[uint64]timeSeries),
		done:              r.remoteWriteGate.Done,
	}

	return writeMetrics
}

type errAppender struct {
	err error
}

func (a errAppender) Append(storage.SeriesRef, labels.Labels, int64, float64) (storage.SeriesRef, error) {
	return 0, a.err
}

func (a errAppender) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, a.err
}

func (a errAppender) AppendHistogram(
	storage.SeriesRef, labels.Labels, int64, *histogram.Histogram,
) (storage.SeriesRef, error) {
	return 0, a.err
}

func (a errAppender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, a.err
}

func (a errAppender) Commit() error { return a.err }

func (a errAppender) Rollback() error { return a.err }

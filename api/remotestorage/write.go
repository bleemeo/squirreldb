package remotestorage

import (
	"context"
	"errors"
	"squirreldb/types"

	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
)

var errNotImplemented = errors.New("not implemented")

type writeMetrics struct {
	index    types.Index
	writer   types.MetricWriter
	reqCtxCh chan *requestContext
	metrics  *metrics
}

// Append adds a sample pair for the given series.
func (w *writeMetrics) Append(ref uint64, l labels.Labels, t int64, v float64) (uint64, error) {
	request := []types.LookupRequest{{Labels: l}}

	ids, ttls, err := w.index.LookupIDs(context.Background(), request)
	if err != nil {
		return 0, err
	}

	metrics := []types.MetricData{
		{
			ID: ids[0],
			Points: []types.MetricPoint{
				{
					Timestamp: t,
					Value:     v,
				},
			},
			TimeToLive: ttls[0],
		},
	}

	return 0, w.writer.Write(context.TODO(), metrics)
}

// Commit submits the collected samples and purges the batch, unused.
func (w *writeMetrics) Commit() error {
	return nil
}

// Rollback rolls back all modifications made in the appender so far, unused.
func (w *writeMetrics) Rollback() error {
	return nil
}

// AppendExemplar adds an exemplar for the given series labels, should never be called.
func (w *writeMetrics) AppendExemplar(ref uint64, l labels.Labels, e exemplar.Exemplar) (uint64, error) {
	return 0, errNotImplemented
}

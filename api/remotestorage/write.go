package remotestorage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"squirreldb/cassandra/tsdb"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
)

var errNotImplemented = errors.New("not implemented")

type writeMetrics struct {
	index    types.Index
	writer   types.MetricWriter
	reqCtxCh chan *requestContext
	metrics  *metrics

	// Map of pending timeseries indexed by their labels hash.
	pendingTimeSeries map[uint64]timeSeries
}

// timeSeries represents samples and labels for a single time series.
type timeSeries struct {
	Labels  labels.Labels
	Samples []types.MetricPoint
}

// Append adds a sample pair for the given series.
func (w *writeMetrics) Append(ref uint64, l labels.Labels, t int64, v float64) (uint64, error) {
	labelsHash := l.Hash()
	metricPoint := types.MetricPoint{
		Timestamp: t,
		Value:     v,
	}

	ts, ok := w.pendingTimeSeries[labelsHash]
	if !ok {
		w.pendingTimeSeries[labelsHash] = timeSeries{
			Labels:  l,
			Samples: []types.MetricPoint{metricPoint},
		}
	} else {
		w.pendingTimeSeries[labelsHash] = timeSeries{
			Labels:  ts.Labels,
			Samples: append(ts.Samples, metricPoint),
		}
	}

	return 0, nil
}

// Commit submits the collected samples and purges the batch, unused.
func (w *writeMetrics) Commit() error {
	defer func() { w.pendingTimeSeries = make(map[uint64]timeSeries) }()

	// Convert the time series map to a slice, because metricsFromTimeseries
	// needs to always iterate on it in the same order.
	pendingTimeSeries := make([]timeSeries, 0, len(w.pendingTimeSeries))
	for _, ts := range w.pendingTimeSeries {
		pendingTimeSeries = append(pendingTimeSeries, ts)
	}

	metrics, totalPoints, err := metricsFromTimeseries(context.Background(), pendingTimeSeries, w.index)
	if err != nil {
		return fmt.Errorf("unable to convert metrics: %w", err)
	}

	w.metrics.RequestsPoints.WithLabelValues("write").Add(float64(totalPoints))

	if err := w.writer.Write(context.Background(), metrics); err != nil {
		return fmt.Errorf("unable to write metrics: %w", err)
	}

	return nil
}

// Rollback rolls back all modifications made in the appender so far.
func (w *writeMetrics) Rollback() error {
	w.pendingTimeSeries = make(map[uint64]timeSeries)

	return nil
}

// Returns a metric list generated from a TimeSeries list.
func metricsFromTimeseries(
	ctx context.Context,
	pendingTimeSeries []timeSeries,
	index types.Index,
) ([]types.MetricData, int, error) {
	if len(pendingTimeSeries) == 0 {
		return nil, 0, nil
	}

	idToIndex := make(map[types.MetricID]int, len(pendingTimeSeries))

	totalPoints := 0
	metrics := make([]types.MetricData, 0, len(pendingTimeSeries))

	requests := make([]types.LookupRequest, 0, len(pendingTimeSeries))

	for _, promSeries := range pendingTimeSeries {
		min := int64(math.MaxInt64)
		max := int64(math.MinInt64)

		for _, s := range promSeries.Samples {
			if min > s.Timestamp {
				min = s.Timestamp
			}

			if max < s.Timestamp {
				max = s.Timestamp
			}
		}

		if min < time.Now().Add(-tsdb.MaxPastDelay).Unix()*1000 {
			logger.Printf("warning: points with timestamp %v will be ignored by pre-aggregation", time.Unix(min/1000, 0))
		}

		requests = append(requests, types.LookupRequest{
			Labels: promSeries.Labels,
			End:    time.Unix(max/1000, max%1000),
			Start:  time.Unix(min/1000, min%1000),
		})
	}

	ids, ttls, err := index.LookupIDs(ctx, requests)
	if err != nil {
		return nil, totalPoints, fmt.Errorf("metric ID lookup failed: %w", err)
	}

	for i, promSeries := range pendingTimeSeries {
		data := types.MetricData{
			ID:         ids[i],
			Points:     promSeries.Samples,
			TimeToLive: ttls[i],
		}

		if idx, found := idToIndex[data.ID]; found {
			metrics[idx].Points = append(metrics[idx].Points, data.Points...)
			if metrics[idx].TimeToLive < data.TimeToLive {
				metrics[idx].TimeToLive = data.TimeToLive
			}
		} else {
			metrics = append(metrics, data)
			idToIndex[data.ID] = len(metrics) - 1
		}

		totalPoints += len(data.Points)
	}

	return metrics, totalPoints, nil
}

// AppendExemplar adds an exemplar for the given series labels, should never be called.
func (w *writeMetrics) AppendExemplar(ref uint64, l labels.Labels, e exemplar.Exemplar) (uint64, error) {
	return 0, errNotImplemented
}

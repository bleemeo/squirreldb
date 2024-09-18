package remotestorage

import (
	"context"
	"errors"
	"fmt"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

var (
	ErrInvalidMatcher = errors.New("invalid labels")
	ErrNotImplemented = errors.New("not implemented")
)

type writeMetrics struct {
	index   types.Index
	writer  types.MetricWriter
	metrics *metrics

	// Mutable label detector is used to detect if a label is mutable.
	// It allows to remove mutable labels in the written metrics.
	mutableLabelDetector MutableLabelDetector

	// Tenant label added to all metrics written.
	tenantLabel labels.Label

	// Metrics Time To Live in seconds.
	timeToLiveSeconds int64

	// Map of pending timeseries indexed by their labels hash.
	pendingTimeSeries map[uint64]timeSeries

	// Returns a metric list generated from a TimeSeries list.
	metricsFromTimeSeries func(
		ctx context.Context,
		pendingTimeSeries []timeSeries,
		index types.Index,
		timeToLiveSeconds int64,
	) ([]types.MetricData, int, error)

	// Release a spot in the remote write gate. Must be called.
	done func()

	// Offset in ms by which points have been backdated.
	backdateOffset int64
}

// timeSeries represents samples and labels for a single time series.
type timeSeries struct {
	Labels  labels.Labels
	Samples []types.MetricPoint
}

// Append adds a sample pair for the given series.
func (w *writeMetrics) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if w.tenantLabel.Value != "" {
		// Add or replace the tenant label with the tenant header.
		builder := labels.NewBuilder(l)
		builder.Set(w.tenantLabel.Name, w.tenantLabel.Value)

		// Remove the mutable labels.
		// Mutable labels should not be written because they can't be searched
		// as the index replaces them by real labels when querying a metric.
		for _, label := range l {
			isMutable, err := w.mutableLabelDetector.IsMutableLabel(context.Background(), w.tenantLabel.Value, label.Name)
			if err != nil {
				return 0, err
			}

			if isMutable {
				builder.Del(label.Name)
			}
		}

		l = builder.Labels()
	}

	if err := validateLabels(l); err != nil {
		return 0, err
	}

	l = dropEmptyValue(l)

	labelsHash := l.Hash()
	metricPoint := types.MetricPoint{
		Timestamp: t + w.backdateOffset,
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

// Commit submits the collected samples and purges the batch.
func (w *writeMetrics) Commit() error {
	defer func() {
		w.pendingTimeSeries = make(map[uint64]timeSeries)
		w.done()
	}()

	// Convert the time series map to a slice, because w.metricsFromTimeSeries
	// always needs to iterate on it in the same order.
	pendingTimeSeries := make([]timeSeries, 0, len(w.pendingTimeSeries))
	for _, ts := range w.pendingTimeSeries {
		pendingTimeSeries = append(pendingTimeSeries, ts)
	}

	metrics, totalPoints, err := w.metricsFromTimeSeries(
		context.Background(),
		pendingTimeSeries,
		w.index,
		w.timeToLiveSeconds,
	)
	if err != nil {
		return fmt.Errorf("unable to convert metrics: %w", err)
	}

	w.metrics.RequestsPoints.Observe(float64(totalPoints))

	if err := w.writer.Write(context.Background(), metrics); err != nil {
		return fmt.Errorf("unable to write metrics: %w", err)
	}

	return nil
}

// Rollback rolls back all modifications made in the appender so far.
func (w *writeMetrics) Rollback() error {
	w.pendingTimeSeries = make(map[uint64]timeSeries)
	w.done()

	return nil
}

// validateLabels checks if the metric name and labels are valid.
// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
func validateLabels(ls labels.Labels) error {
	for _, l := range ls {
		if l.Name == model.MetricNameLabel {
			if !model.IsValidMetricName(model.LabelValue(l.Value)) {
				return fmt.Errorf("%w: metric name '%s' should match %s", ErrInvalidMatcher, l.Value, model.MetricNameRE)
			}
		} else if !model.LabelName(l.Name).IsValid() {
			return fmt.Errorf("%w: label name '%s' should match %s", ErrInvalidMatcher, l.Name, model.LabelNameRE)
		}
	}

	return nil
}

func dropEmptyValue(ls labels.Labels) labels.Labels {
	i := 0

	for _, l := range ls {
		if l.Value == "" {
			continue
		}

		ls[i] = l
		i++
	}

	ls = ls[:i]

	return ls
}

// AppendExemplar adds an exemplar for the given series labels, should never be called.
func (w *writeMetrics) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

func (w *writeMetrics) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

func (w *writeMetrics) AppendHistogram(
	storage.SeriesRef, labels.Labels, int64, *histogram.Histogram, *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

func (w *writeMetrics) AppendCTZeroSample(storage.SeriesRef, labels.Labels, int64, int64) (storage.SeriesRef, error) {
	return 0, ErrNotImplemented
}

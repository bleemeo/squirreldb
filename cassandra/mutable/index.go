package mutable

import (
	"context"
	"errors"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

type mutableIndex struct {
	index          types.Index
	labelProcessor *LabelProcessor
}

// NewIndexWrapper returns an index wrapper that handles mutable labels.
func NewIndexWrapper(
	index types.Index,
	labelProcessor *LabelProcessor,
) types.Index {
	mi := mutableIndex{
		index:          index,
		labelProcessor: labelProcessor,
	}

	return &mi
}

// Start the wrapped index.
func (m *mutableIndex) Start(ctx context.Context) error {
	if task, ok := m.index.(types.Task); ok {
		err := task.Start(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop the wrapped index.
func (m *mutableIndex) Stop() error {
	if task, ok := m.index.(types.Task); ok {
		err := task.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}

// AllIDs get all IDs from the wrapped index.
func (m *mutableIndex) AllIDs(ctx context.Context, start time.Time, end time.Time) ([]types.MetricID, error) {
	return m.index.AllIDs(ctx, start, end)
}

// LookupIDs in the wrapped index.
func (m *mutableIndex) LookupIDs(
	ctx context.Context,
	requests []types.LookupRequest,
) ([]types.MetricID, []int64, error) {
	return m.index.LookupIDs(ctx, requests)
}

// Search replace the mutable labels by non mutable labels before searching the wrapped index.
func (m *mutableIndex) Search(
	ctx context.Context,
	start time.Time, end time.Time,
	matchers []*labels.Matcher,
) (types.MetricsSet, error) {
	// Replace mutable labels by non mutable labels.
	matchers, err := m.labelProcessor.ReplaceMutableLabels(matchers)
	if err != nil {
		// Return an empty metric set if the mutable matchers have no match.
		if errors.Is(err, ErrNoResult) {
			return emptyMetricsSet{}, nil
		}

		return nil, err
	}

	metricsSet, err := m.index.Search(ctx, start, end, matchers)
	if err != nil {
		return metricsSet, err
	}

	metricsSetWrapper := &mutableMetricsSet{
		metricsSet:     metricsSet,
		labelProcessor: m.labelProcessor,
	}

	return metricsSetWrapper, nil
}

// LabelValues returns potential values for a label name. Values will have at least
// one metrics matching matchers.
func (m *mutableIndex) LabelValues(
	ctx context.Context,
	start, end time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	return m.index.LabelValues(ctx, start, end, name, matchers)
}

// LabelNames returns the unique label names for metrics matching matchers in sorted order.
func (m *mutableIndex) LabelNames(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) ([]string, error) {
	return m.index.LabelNames(ctx, start, end, matchers)
}

// mutableMetricsSet wraps a metric set to add mutable labels to the results.
type mutableMetricsSet struct {
	metricsSet     types.MetricsSet
	labelProcessor *LabelProcessor
}

func (m *mutableMetricsSet) Next() bool {
	if m.metricsSet == nil {
		return false
	}

	return m.metricsSet.Next()
}

func (m *mutableMetricsSet) At() types.MetricLabel {
	if m.metricsSet == nil {
		return types.MetricLabel{}
	}

	metric := m.metricsSet.At()

	lbls, err := m.labelProcessor.AddMutableLabels(metric.Labels)
	if err != nil {
		// When there is an error, simply skip adding the mutable labels.
		logger.Printf("Failed to add mutable labels: %v\n", err)
	} else {
		metric.Labels = lbls
	}

	return metric
}

func (m *mutableMetricsSet) Err() error {
	if m.metricsSet == nil {
		return nil
	}

	return m.metricsSet.Err()
}

func (m *mutableMetricsSet) Count() int {
	if m.metricsSet == nil {
		return 0
	}

	return m.metricsSet.Count()
}

// emptyMetricsSet is a metrics set that returns no labels.
type emptyMetricsSet struct{}

func (m emptyMetricsSet) Next() bool {
	return false
}

func (m emptyMetricsSet) At() types.MetricLabel {
	return types.MetricLabel{}
}

func (m emptyMetricsSet) Err() error {
	return nil
}

func (m emptyMetricsSet) Count() int {
	return 0
}

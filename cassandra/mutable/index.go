package mutable

import (
	"context"
	"errors"
	"io"
	"sort"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

var errNotImplemented = errors.New("not implemented")

type indexWrapper struct {
	index          types.Index
	labelProcessor *LabelProcessor
}

// NewIndexWrapper returns an index wrapper that handles mutable labels.
func NewIndexWrapper(
	index types.Index,
	labelProcessor *LabelProcessor,
) types.Index {
	mi := indexWrapper{
		index:          index,
		labelProcessor: labelProcessor,
	}

	return &mi
}

// Start the wrapped index.
func (m *indexWrapper) Start(ctx context.Context) error {
	if task, ok := m.index.(types.Task); ok {
		err := task.Start(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop the wrapped index.
func (m *indexWrapper) Stop() error {
	if task, ok := m.index.(types.Task); ok {
		err := task.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}

// AllIDs get all IDs from the wrapped index.
func (m *indexWrapper) AllIDs(ctx context.Context, start time.Time, end time.Time) ([]types.MetricID, error) {
	return m.index.AllIDs(ctx, start, end)
}

// LookupIDs in the wrapped index.
func (m *indexWrapper) LookupIDs(
	ctx context.Context,
	requests []types.LookupRequest,
) ([]types.MetricID, []int64, error) {
	return m.index.LookupIDs(ctx, requests)
}

// Search replace the mutable labels by non mutable labels before searching the wrapped index.
func (m *indexWrapper) Search(
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
// Warning: If the name is a mutable label name, the values are returned without checking the matchers.
func (m *indexWrapper) LabelValues(
	ctx context.Context,
	start, end time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	tenant := m.labelProcessor.tenantFromMatchers(matchers)
	if tenant != "" {
		isMutableLabel, err := m.labelProcessor.IsMutableLabel(tenant, name)
		if err != nil {
			return nil, err
		}

		if isMutableLabel {
			values, err := m.labelProcessor.labelProvider.AllValues(tenant, name)
			if err != nil {
				return nil, err
			}

			sort.Strings(values)

			return values, nil
		}
	}

	values, err := m.index.LabelValues(ctx, start, end, name, matchers)
	if err != nil {
		return nil, err
	}

	return values, nil
}

// LabelNames returns the unique label names for metrics matching matchers in ascending order.
// Warning: mutable label names are added without checking the matchers.
func (m *indexWrapper) LabelNames(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) ([]string, error) {
	names, err := m.index.LabelNames(ctx, start, end, matchers)
	if err != nil {
		return nil, err
	}

	tenant := m.labelProcessor.tenantFromMatchers(matchers)
	if tenant != "" {
		mutableLabelNames, err := m.labelProcessor.MutableLabelNames(tenant)
		if err != nil {
			return nil, err
		}

		names = append(names, mutableLabelNames...)

		sort.Strings(names)
	}

	return names, nil
}

// Verify implements the IndexVerifier interface used by the API.
func (m *indexWrapper) Verify(
	ctx context.Context,
	w io.Writer,
	doFix bool,
	acquireLock bool,
) (hadIssue bool, err error) {
	if verifier, ok := m.index.(types.IndexVerifier); ok {
		return verifier.Verify(ctx, w, doFix, acquireLock)
	}

	return false, errNotImplemented
}

// Dump implements the IndexDumper interface used by the API.
func (m *indexWrapper) Dump(ctx context.Context, w io.Writer) error {
	if dumper, ok := m.index.(types.IndexDumper); ok {
		return dumper.Dump(ctx, w)
	}

	return errNotImplemented
}

// mutableMetricsSet wraps a metric set to add mutable labels to the results.
type mutableMetricsSet struct {
	metricsSet     types.MetricsSet
	currentMetric  types.MetricLabel
	err            error
	labelProcessor *LabelProcessor
}

func (m *mutableMetricsSet) Next() bool {
	if m.metricsSet == nil {
		return false
	}

	if !m.metricsSet.Next() {
		return false
	}

	metric := m.metricsSet.At()

	lbls, err := m.labelProcessor.AddMutableLabels(metric.Labels)
	if err != nil {
		logger.Printf("Failed to add mutable labels: %v\n", err)

		m.err = err

		return false
	}

	metric.Labels = lbls
	m.currentMetric = metric

	return true
}

func (m *mutableMetricsSet) At() types.MetricLabel {
	return m.currentMetric
}

func (m *mutableMetricsSet) Err() error {
	if m.err != nil {
		return m.err
	}

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

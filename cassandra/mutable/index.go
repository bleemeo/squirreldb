package mutable

import (
	"context"
	"errors"
	"io"
	"sort"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

var errNotImplemented = errors.New("not implemented")

type indexWrapper struct {
	index          types.Index
	labelProcessor *LabelProcessor
	logger         zerolog.Logger
}

// NewIndexWrapper returns an index wrapper that handles mutable labels.
func NewIndexWrapper(
	index types.Index,
	labelProcessor *LabelProcessor,
	logger zerolog.Logger,
) types.Index {
	mi := indexWrapper{
		index:          index,
		labelProcessor: labelProcessor,
		logger:         logger,
	}

	return &mi
}

// Start the wrapped index.
func (i *indexWrapper) Start(ctx context.Context) error {
	if task, ok := i.index.(types.Task); ok {
		err := task.Start(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop the wrapped index.
func (i *indexWrapper) Stop() error {
	if task, ok := i.index.(types.Task); ok {
		err := task.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}

// AllIDs get all IDs from the wrapped index.
func (i *indexWrapper) AllIDs(ctx context.Context, start time.Time, end time.Time) ([]types.MetricID, error) {
	return i.index.AllIDs(ctx, start, end)
}

// LookupIDs in the wrapped index.
func (i *indexWrapper) LookupIDs(
	ctx context.Context,
	requests []types.LookupRequest,
) ([]types.MetricID, []int64, error) {
	return i.index.LookupIDs(ctx, requests)
}

// Search replace the mutable labels by non mutable labels before searching the wrapped index.
func (i *indexWrapper) Search(
	ctx context.Context,
	start time.Time, end time.Time,
	matchers []*labels.Matcher,
) (types.MetricsSet, error) {
	// Replace mutable labels by non mutable labels.
	matchers, err := i.labelProcessor.ReplaceMutableLabels(ctx, matchers)
	if err != nil {
		// Return an empty metric set if the mutable matchers have no match.
		if errors.Is(err, errNoResult) {
			return emptyMetricsSet{}, nil
		}

		return nil, err
	}

	metricsSet, err := i.index.Search(ctx, start, end, matchers)
	if err != nil {
		return metricsSet, err
	}

	metricsSetWrapper := &mutableMetricsSet{
		ctx:            ctx,
		metricsSet:     metricsSet,
		labelProcessor: i.labelProcessor,
		logger:         i.logger,
	}

	return metricsSetWrapper, nil
}

// LabelValues returns potential values for a label name. Values will have at least
// one metrics matching matchers.
// Warning: If the name is a mutable label name, the values are returned without checking the matchers.
func (i *indexWrapper) LabelValues(
	ctx context.Context,
	start, end time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	tenant := i.labelProcessor.tenantFromMatchers(matchers)
	if tenant != "" {
		isMutableLabel, err := i.labelProcessor.IsMutableLabel(ctx, tenant, name)
		if err != nil {
			return nil, err
		}

		if isMutableLabel {
			values, err := i.labelProcessor.labelProvider.AllValues(ctx, tenant, name)
			if err != nil {
				return nil, err
			}

			sort.Strings(values)

			return values, nil
		}
	}

	values, err := i.index.LabelValues(ctx, start, end, name, matchers)
	if err != nil {
		return nil, err
	}

	return values, nil
}

// LabelNames returns the unique label names for metrics matching matchers in ascending order.
// Warning: mutable label names are added without checking the matchers.
func (i *indexWrapper) LabelNames(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) ([]string, error) {
	names, err := i.index.LabelNames(ctx, start, end, matchers)
	if err != nil {
		return nil, err
	}

	tenant := i.labelProcessor.tenantFromMatchers(matchers)
	if tenant != "" {
		mutableLabelNames, err := i.labelProcessor.MutableLabelNames(ctx, tenant)
		if err != nil {
			return nil, err
		}

		names = append(names, mutableLabelNames...)

		sort.Strings(names)
	}

	return names, nil
}

// InfoGlobal implements the IndexDumper interface used by the API.
func (i *indexWrapper) InfoGlobal(ctx context.Context, w io.Writer) error {
	if dumper, ok := i.index.(types.IndexDumper); ok {
		return dumper.InfoGlobal(ctx, w)
	}

	return errNotImplemented
}

// InfoByID implements the IndexDumper interface used by the API.
func (i *indexWrapper) InfoByID(ctx context.Context, w io.Writer, id types.MetricID) error {
	if dumper, ok := i.index.(types.IndexDumper); ok {
		return dumper.InfoByID(ctx, w, id)
	}

	return errNotImplemented
}

// DumpByLabels implements the IndexDumper interface used by the API.
func (i *indexWrapper) DumpByLabels(
	ctx context.Context,
	w io.Writer,
	start, end time.Time,
	matchers []*labels.Matcher,
) error {
	if dumper, ok := i.index.(types.IndexDumper); ok {
		return dumper.DumpByLabels(ctx, w, start, end, matchers)
	}

	return errNotImplemented
}

// Verify implements the IndexVerifier interface used by the API.
func (i *indexWrapper) Verifier(w io.Writer) types.IndexVerifier {
	if verifier, ok := i.index.(types.VerifiableIndex); ok {
		return verifier.Verifier(w)
	}

	return notImplementedVerifier{}
}

// Dump implements the IndexDumper interface used by the API.
func (i *indexWrapper) Dump(ctx context.Context, w io.Writer) error {
	if dumper, ok := i.index.(types.IndexDumper); ok {
		return dumper.Dump(ctx, w)
	}

	return errNotImplemented
}

// DumpByExpirationDate implements the IndexDumper interface used by the API.
func (i *indexWrapper) DumpByExpirationDate(ctx context.Context, w io.Writer, expirationDate time.Time) error {
	if dumper, ok := i.index.(types.IndexDumper); ok {
		return dumper.DumpByExpirationDate(ctx, w, expirationDate)
	}

	return errNotImplemented
}

func (i *indexWrapper) DumpByShard(ctx context.Context, w io.Writer, shard time.Time) error {
	if dumper, ok := i.index.(types.IndexDumper); ok {
		return dumper.DumpByShard(ctx, w, shard)
	}

	return errNotImplemented
}

func (i *indexWrapper) DumpByPosting(
	ctx context.Context,
	w io.Writer,
	shard time.Time,
	name string,
	value string,
) error {
	if dumper, ok := i.index.(types.IndexDumper); ok {
		return dumper.DumpByPosting(ctx, w, shard, name, value)
	}

	return errNotImplemented
}

// InternalForceExpirationTimestamp implements the IndexInternalExpirerer interface used in tests.
func (i *indexWrapper) InternalForceExpirationTimestamp(ctx context.Context, value time.Time) error {
	if expirerer, ok := i.index.(types.IndexInternalExpirerer); ok {
		return expirerer.InternalForceExpirationTimestamp(ctx, value)
	}

	return errNotImplemented
}

// RunOnce implements the IndexRunner interface used in tests.
func (i *indexWrapper) RunOnce(ctx context.Context, now time.Time) bool {
	if runner, ok := i.index.(types.IndexRunner); ok {
		return runner.RunOnce(ctx, now)
	}

	return false
}

// mutableMetricsSet wraps a metric set to add mutable labels to the results.
type mutableMetricsSet struct {
	metricsSet     types.MetricsSet
	currentMetric  types.MetricLabel
	ctx            context.Context //nolint:containedctx
	err            error
	labelProcessor *LabelProcessor
	logger         zerolog.Logger
}

func (m *mutableMetricsSet) Next() bool {
	if m.metricsSet == nil {
		return false
	}

	if !m.metricsSet.Next() {
		return false
	}

	metric := m.metricsSet.At()

	lbls, err := m.labelProcessor.AddMutableLabels(m.ctx, metric.Labels)
	if err != nil {
		m.logger.Err(err).Msg("Failed to add mutable labels")

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

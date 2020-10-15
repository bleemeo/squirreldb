package promql

import (
	"context"
	"errors"
	"sort"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// Store implement Prometheus.Queryable and read from SquirrelDB store.
type Store struct {
	Err    error
	Index  types.Index
	Reader types.MetricReader
}

type querier struct {
	ctx        context.Context
	index      types.Index
	reader     types.MetricReader
	mint, maxt int64
}

// Querier returns a storage.Querier to read from SquirrelDB store.
func (s Store) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if s.Err != nil {
		return nil, s.Err
	}

	return querier{ctx: ctx, index: s.Index, reader: s.Reader, mint: mint, maxt: maxt}, nil
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimising select, but it's up to implementation how this is used if used at all.
func (q querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	minT := time.Unix(q.mint/1000, q.mint%1000)
	maxT := time.Unix(q.maxt/1000, q.maxt%1000)

	metrics, err := q.index.Search(minT, maxT, matchers)
	if err != nil {
		return &seriesIter{err: err}
	}

	if len(metrics) == 0 {
		return &seriesIter{}
	}

	if sortSeries {
		sort.Slice(metrics, func(i, j int) bool {
			aLabels := metrics[i].Labels
			bLabels := metrics[j].Labels
			return labels.Compare(aLabels, bLabels) < 0
		})
	}

	id2Labels := make(map[types.MetricID]labels.Labels, len(metrics))
	ids := make([]types.MetricID, len(metrics))

	for i, m := range metrics {
		id2Labels[m.ID] = m.Labels
		ids[i] = m.ID
	}

	req := types.MetricRequest{
		IDs:           ids,
		FromTimestamp: q.mint,
		ToTimestamp:   q.maxt,
	}

	if hints != nil {
		// We don't use hints.Start/End, because PromQL engine will anyway
		// set q.mint/q.maxt to good value. Actually hints.Start/End will make
		// the first point incomplet when using range (like "rate(metric[5m])")
		req.Function = hints.Func
		req.StepMs = hints.Step
	}

	result, err := q.reader.ReadIter(q.ctx, req)

	return &seriesIter{
		list:      result,
		index:     q.index,
		id2Labels: id2Labels,
		err:       err,
	}
}

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifefime of the querier.
func (q querier) LabelValues(name string) ([]string, storage.Warnings, error) {
	return nil, nil, errors.New("not implemented")
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q querier) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, errors.New("not implemented")
}

// Close releases the resources of the Querier.
func (q querier) Close() error {
	return nil
}

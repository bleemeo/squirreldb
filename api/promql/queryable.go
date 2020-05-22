package promql

import (
	"context"
	"errors"
	"sort"
	"squirreldb/types"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// Store implement Prometheus.Queryable and read from SquirrelDB store
type Store struct {
	Index  types.Index
	Reader types.MetricReader
}

type querier struct {
	index      types.Index
	reader     types.MetricReader
	mint, maxt int64
}

// Querier returns a storage.Querier to read from SquirrelDB store
func (s Store) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return querier{index: s.Index, reader: s.Reader, mint: mint, maxt: maxt}, nil
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimising select, but it's up to implementation how this is used if used at all.
func (q querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	ids, err := q.index.Search(matchers)
	if err != nil {
		return nil, nil, err
	}

	var id2Labels map[types.MetricID][]labels.Label

	if sortSeries {
		id2Labels = make(map[types.MetricID][]labels.Label, len(ids))

		for _, id := range ids {
			tmp, err := q.index.LookupLabels(id)
			if err != nil {
				return nil, nil, err
			}

			l := make([]labels.Label, len(tmp))

			for i, x := range tmp {
				l[i] = labels.Label{
					Name:  x.Name,
					Value: x.Value,
				}
			}

			id2Labels[id] = l
		}

		sort.Slice(ids, func(i, j int) bool {
			aLabels := id2Labels[ids[i]]
			bLabels := id2Labels[ids[j]]
			return labels.Compare(aLabels, bLabels) < 0
		})
	}

	req := types.MetricRequest{
		IDs:           ids,
		FromTimestamp: q.mint,
		ToTimestamp:   q.maxt,
	}

	if hints != nil {
		req.Function = hints.Func
		req.StepMs = hints.Step
	}

	result, err := q.reader.ReadIter(req)

	return &seriesIter{
		list:      result,
		index:     q.index,
		id2Labels: id2Labels,
	}, nil, err
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

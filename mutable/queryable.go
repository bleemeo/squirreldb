// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mutable

import (
	"context"
	"errors"
	"sort"

	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/rs/zerolog"
)

// queryableWrapper handles mutable labels on read by decorating a TSDB at the
// storage.Querier seam: it replaces mutable matchers by their non-mutable form
// before querying and re-adds the mutable labels to the results. It is the
// counterpart of indexWrapper for a backend that resolves matchers itself
// (translates them to SQL) instead of exposing a types.Index to wrap.
type queryableWrapper struct {
	types.TSDB

	labelProcessor *LabelProcessor
	logger         zerolog.Logger
}

// NewQueryableWrapper returns a TSDB that handles mutable labels on the read path.
func NewQueryableWrapper(
	inner types.TSDB,
	labelProcessor *LabelProcessor,
	logger zerolog.Logger,
) types.TSDB {
	return &queryableWrapper{
		TSDB:           inner,
		labelProcessor: labelProcessor,
		logger:         logger,
	}
}

func (w *queryableWrapper) QuerierWithOptions(
	opts types.QuerierOptions,
	mint, maxt int64,
) (storage.Querier, error) {
	inner, err := w.TSDB.QuerierWithOptions(opts, mint, maxt)
	if err != nil {
		return nil, err
	}

	return &querierWrapper{
		Querier:        inner,
		labelProcessor: w.labelProcessor,
		logger:         w.logger,
	}, nil
}

func (w *queryableWrapper) ChunkQuerierWithOptions(
	opts types.QuerierOptions,
	mint, maxt int64,
) (storage.ChunkQuerier, error) {
	// The chunk path reuses the sample querier so the mutable translation lives in
	// one place; convert turns its series (labels already rewritten) into chunks.
	q, err := w.QuerierWithOptions(opts, mint, maxt)
	if err != nil {
		return nil, err
	}

	return convert.NewQuerierToChunkQuerier(q), nil
}

type querierWrapper struct {
	storage.Querier

	labelProcessor *LabelProcessor
	logger         zerolog.Logger
}

// Select replaces the mutable matchers before querying and re-adds the mutable
// labels to the resulting series.
func (q *querierWrapper) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	matchers, err := q.labelProcessor.ReplaceMutableLabels(ctx, matchers)
	if err != nil {
		// A mutable matcher with no match resolves to an empty result.
		if errors.Is(err, errNoResult) {
			return storage.EmptySeriesSet()
		}

		return storage.ErrSeriesSet(err)
	}

	set := q.Querier.Select(ctx, sortSeries, hints, matchers...)

	return &seriesSetWrapper{
		SeriesSet:      set,
		ctx:            ctx,
		labelProcessor: q.labelProcessor,
		logger:         q.logger,
	}
}

// LabelValues returns potential values for a label name.
// Warning: if the name is a mutable label name, the values are returned without
// checking the matchers (as in indexWrapper.LabelValues).
func (q *querierWrapper) LabelValues(
	ctx context.Context,
	name string,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	tenant := q.labelProcessor.tenantFromMatchers(matchers)
	if tenant != "" {
		isMutableLabel, err := q.labelProcessor.IsMutableLabel(ctx, tenant, name)
		if err != nil {
			return nil, nil, err
		}

		if isMutableLabel {
			values, err := q.labelProcessor.labelProvider.AllValues(ctx, tenant, name)
			if err != nil {
				return nil, nil, err
			}

			sort.Strings(values)

			return values, nil, nil
		}
	}

	return q.Querier.LabelValues(ctx, name, hints, matchers...)
}

// LabelNames returns the label names for series matching matchers, plus the
// mutable label names (added without checking the matchers, as in
// indexWrapper.LabelNames).
func (q *querierWrapper) LabelNames(
	ctx context.Context,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	names, annots, err := q.Querier.LabelNames(ctx, hints, matchers...)
	if err != nil {
		return nil, annots, err
	}

	tenant := q.labelProcessor.tenantFromMatchers(matchers)
	if tenant != "" {
		mutableLabelNames, err := q.labelProcessor.MutableLabelNames(ctx, tenant)
		if err != nil {
			return nil, annots, err
		}

		names = append(names, mutableLabelNames...)
		sort.Strings(names)
	}

	return names, annots, nil
}

// seriesSetWrapper adds mutable labels to each series of the wrapped set.
type seriesSetWrapper struct {
	storage.SeriesSet

	ctx            context.Context //nolint:containedctx
	labelProcessor *LabelProcessor
	logger         zerolog.Logger
	current        storage.Series
	err            error
}

func (s *seriesSetWrapper) Next() bool {
	if s.err != nil {
		return false
	}

	if !s.SeriesSet.Next() {
		return false
	}

	series := s.SeriesSet.At()

	lbls, err := s.labelProcessor.AddMutableLabels(s.ctx, series.Labels())
	if err != nil {
		s.logger.Err(err).Msg("Failed to add mutable labels")

		s.err = err

		return false
	}

	s.current = &seriesWithLabels{Series: series, labels: lbls}

	return true
}

func (s *seriesSetWrapper) At() storage.Series {
	return s.current
}

func (s *seriesSetWrapper) Err() error {
	if s.err != nil {
		return s.err
	}

	return s.SeriesSet.Err()
}

// seriesWithLabels overrides a series' label set, keeping its samples iterator.
type seriesWithLabels struct {
	storage.Series

	labels labels.Labels
}

func (s *seriesWithLabels) Labels() labels.Labels {
	return s.labels
}

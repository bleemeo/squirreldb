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

package source

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

// Queryable is a storage.Queryable serving a fixed list of series, without
// tenant nor statistics.
type Queryable struct {
	Series []Series
}

func (q Queryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return NewQuerier(q.Series, mint, maxt, QuerierOptions{}), nil
}

// QuerierOptions are the per-querier options of NewQuerier.
type QuerierOptions struct {
	// TenantLabel, when its Name is set, is added to every series (and to the
	// label names/values) the querier returns, so the series appear to belong
	// to that tenant. Matchers on that label are evaluated against it.
	TenantLabel labels.Label
	// OnClose, when set, is called when the querier is closed with the number of
	// points read and series returned by the querier.
	OnClose func(pointsRead uint64, seriesReturned uint64)
}

// NewQuerier returns a storage.Querier serving series between mint and maxt.
// series must be sorted by labels (as Merge returns them).
func NewQuerier(series []Series, mint, maxt int64, opts QuerierOptions) storage.Querier {
	return &querier{
		series: series,
		mint:   mint,
		maxt:   maxt,
		opts:   opts,
	}
}

type querier struct {
	series []Series
	mint   int64
	maxt   int64
	opts   QuerierOptions

	pointsRead     atomic.Uint64
	seriesReturned atomic.Uint64
}

func (q *querier) hasTenant() bool {
	return q.opts.TenantLabel.Name != ""
}

// matches returns whether lbls, once the tenant label is added, matches all matchers.
func (q *querier) matches(lbls labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		var value string

		if q.hasTenant() && m.Name == q.opts.TenantLabel.Name {
			value = q.opts.TenantLabel.Value
		} else {
			value = lbls.Get(m.Name)
		}

		if !m.Matches(value) {
			return false
		}
	}

	return true
}

func (q *querier) matchingSeries(matchers []*labels.Matcher) []Series {
	result := make([]Series, 0)

	for _, s := range q.series {
		if q.matches(s.Labels, matchers) {
			result = append(result, s)
		}
	}

	return result
}

func (q *querier) withTenant(lbls labels.Labels) labels.Labels {
	if !q.hasTenant() {
		return lbls
	}

	builder := labels.NewBuilder(lbls)
	builder.Set(q.opts.TenantLabel.Name, q.opts.TenantLabel.Value)

	return builder.Labels()
}

// Select returns the series matching the matchers. Series are always returned
// sorted, sortSeries is only used to know whether adding the tenant label
// requires sorting them again.
func (q *querier) Select(
	ctx context.Context,
	sortSeries bool,
	_ *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	matching := q.matchingSeries(matchers)
	result := make([]storage.Series, len(matching))

	for i, s := range matching {
		result[i] = &series{
			ctx:        ctx,
			series:     s,
			labels:     q.withTenant(s.Labels),
			mint:       q.mint,
			maxt:       q.maxt,
			pointsRead: &q.pointsRead,
		}
	}

	// q.series is sorted, but adding a label to every series may change their order.
	if sortSeries && q.hasTenant() {
		slices.SortFunc(result, func(a, b storage.Series) int {
			return labels.Compare(a.Labels(), b.Labels())
		})
	}

	q.seriesReturned.Add(uint64(len(result)))

	return &seriesSet{series: result}
}

// LabelValues returns the sorted, deduplicated values of the label name among the
// series matching the matchers.
func (q *querier) LabelValues(
	_ context.Context,
	name string,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	matching := q.matchingSeries(matchers)

	if q.hasTenant() && name == q.opts.TenantLabel.Name {
		if len(matching) == 0 {
			return nil, nil, nil
		}

		return []string{q.opts.TenantLabel.Value}, nil, nil
	}

	values := make(map[string]struct{})

	for _, s := range matching {
		if v := s.Labels.Get(name); v != "" {
			values[v] = struct{}{}
		}
	}

	return sortedKeys(values, hints), nil, nil
}

// LabelNames returns the sorted, deduplicated label names of the series matching
// the matchers.
func (q *querier) LabelNames(
	_ context.Context,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	matching := q.matchingSeries(matchers)
	names := make(map[string]struct{})

	for _, s := range matching {
		s.Labels.Range(func(l labels.Label) {
			names[l.Name] = struct{}{}
		})
	}

	if q.hasTenant() && len(matching) > 0 {
		names[q.opts.TenantLabel.Name] = struct{}{}
	}

	return sortedKeys(names, hints), nil, nil
}

func (q *querier) Close() error {
	if q.opts.OnClose != nil {
		q.opts.OnClose(q.pointsRead.Load(), q.seriesReturned.Load())
	}

	return nil
}

func sortedKeys(set map[string]struct{}, hints *storage.LabelHints) []string {
	result := make([]string, 0, len(set))

	for k := range set {
		result = append(result, k)
	}

	slices.Sort(result)

	if hints != nil && hints.Limit > 0 && len(result) > hints.Limit {
		result = result[:hints.Limit]
	}

	return result
}

type seriesSet struct {
	series  []storage.Series
	nextIdx int
}

func (s *seriesSet) Next() bool {
	if s.nextIdx >= len(s.series) {
		return false
	}

	s.nextIdx++

	return true
}

func (s *seriesSet) At() storage.Series {
	return s.series[s.nextIdx-1]
}

func (s *seriesSet) Err() error {
	return nil
}

func (s *seriesSet) Warnings() annotations.Annotations {
	return nil
}

type series struct {
	ctx        context.Context //nolint:containedctx // values are computed lazily, in the context of the Select.
	series     Series
	labels     labels.Labels
	mint       int64
	maxt       int64
	pointsRead *atomic.Uint64
}

func (s *series) Labels() labels.Labels {
	return s.labels
}

func (s *series) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return newIterator(s.ctx, s.series, s.mint, s.maxt, s.pointsRead)
}

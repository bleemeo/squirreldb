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

package tsdb

import (
	"context"
	"sort"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type querier struct {
	c             *CassandraTSDB
	opts          types.QuerierOptions
	backendOption querierOptions
	mint          int64
	maxt          int64
}

type querierOptions struct {
	ForcePreAggregated bool
	ForceRaw           bool
}

func (q *querier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	minT := time.UnixMilli(q.mint)
	maxT := time.UnixMilli(q.maxt)

	metrics, err := q.c.index.Search(ctx, minT, maxT, matchers)
	if err != nil {
		return &seriesIter{err: err}
	}

	if metrics.Count() == 0 {
		return &seriesIter{}
	}

	if sortSeries {
		metricsList := make([]types.MetricLabel, 0, metrics.Count())
		for metrics.Next() {
			metricsList = append(metricsList, metrics.At())
		}

		if err := metrics.Err(); err != nil {
			return &seriesIter{err: err}
		}

		sort.Slice(metricsList, func(i, j int) bool {
			aLabels := metricsList[i].Labels
			bLabels := metricsList[j].Labels

			return labels.Compare(aLabels, bLabels) < 0
		})

		metrics = &dummy.MetricsLabel{List: metricsList}
	}

	id2Labels := make(map[types.MetricID]labels.Labels, metrics.Count())
	ids := make([]types.MetricID, 0, metrics.Count())

	for metrics.Next() {
		m := metrics.At()
		id2Labels[m.ID] = m.Labels
		ids = append(ids, m.ID)
	}

	if err := metrics.Err(); err != nil {
		return &seriesIter{err: err}
	}

	req := types.MetricRequest{
		IDs:                ids,
		FromTimestamp:      minT.UnixMilli(),
		ToTimestamp:        maxT.UnixMilli(),
		ForcePreAggregated: q.backendOption.ForcePreAggregated,
		ForceRaw:           q.backendOption.ForceRaw,
		EnableDebug:        q.opts.RequestDebug >= types.DebugLevelSimple,
		EnableVerboseDebug: q.opts.RequestDebug >= types.DebugLevelVerbose,
	}

	if hints != nil {
		// We don't use hints.Start/End, because PromQL engine will anyway
		// set q.mint/q.maxt to good value. Actually hints.Start/End will make
		// the first point incomplete when using range (like "rate(metric[5m])")
		req.Function = hints.Func
		req.StepMs = hints.Step
	}

	result := q.c.readIter(ctx, req)

	if q.opts.RequestDebug >= types.DebugLevelSimple {
		q.c.logger.Info().Msgf(
			"Select(...) returned %d metricID. request = {ForcePreAggregated=%v, ForceRaw=%v, Function=%v, StepMs=%d}",
			len(req.IDs),
			req.ForcePreAggregated,
			req.ForceRaw,
			req.Function,
			req.StepMs,
		)
	}

	return &seriesIter{
		c:         q.c,
		list:      result,
		id2Labels: id2Labels,
	}
}

func (q *querier) LabelValues(
	ctx context.Context,
	name string,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	minT := time.UnixMilli(q.mint)
	maxT := time.UnixMilli(q.maxt)

	results, err := q.c.index.LabelValues(ctx, minT, maxT, name, matchers)

	return results, nil, err
}

func (q *querier) LabelNames(
	ctx context.Context,
	_ *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	minT := time.UnixMilli(q.mint)
	maxT := time.UnixMilli(q.maxt)

	results, err := q.c.index.LabelNames(ctx, minT, maxT, matchers)

	return results, nil, err
}

func (q *querier) Close() error {
	return nil
}

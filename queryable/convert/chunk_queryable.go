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

package convert

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func NewQueryableToChunkQueryable(q storage.Queryable) storage.SampleAndChunkQueryable {
	if inner, ok := q.(storage.SampleAndChunkQueryable); ok {
		return inner
	}

	return convertedQueryable{q}
}

func NewQuerierToChunkQuerier(q storage.Querier) storage.ChunkQuerier {
	return convertedQuerier{q}
}

type convertedQueryable struct {
	storage.Queryable
}

func (q convertedQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	inner, err := q.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}

	return NewQuerierToChunkQuerier(inner), nil
}

type convertedQuerier struct {
	storage.Querier
}

func (q convertedQuerier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.ChunkSeriesSet {
	sset := q.Querier.Select(ctx, sortSeries, hints, matchers...)

	return storage.NewSeriesSetToChunkSet(sset)
}

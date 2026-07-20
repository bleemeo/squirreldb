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

package limitqueryable

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type Queryable struct {
	q    types.QueryableWithOptions
	opts Options

	returnedSeries atomic.Uint32
	returnedPoints atomic.Uint64
}

var (
	ErrTooManySeries = errors.New("too many series evaluated by this PromQL")
	ErrTooManyPoints = errors.New("too many points evaluated by this PromQL")
)

type Options struct {
	// Enforce that queried time is between those values.
	MinTime            time.Time
	MaxTime            time.Time
	ForcedMatchers     []*labels.Matcher
	MaxEvaluatedSeries uint32
	MaxEvaluatedPoints uint64
}

func NewLimiterQueryable(q types.QueryableWithOptions, opts Options) *Queryable {
	return &Queryable{
		q:    q,
		opts: opts,
	}
}

func (q *Queryable) fixTime(t int64) int64 {
	if !q.opts.MinTime.IsZero() && t < q.opts.MinTime.UnixMilli() {
		return q.opts.MinTime.UnixMilli()
	}

	if !q.opts.MaxTime.IsZero() && t > q.opts.MaxTime.UnixMilli() {
		return q.opts.MaxTime.UnixMilli()
	}

	return t
}

func (q *Queryable) QuerierWithOptions(opts types.QuerierOptions, mint, maxt int64) (storage.Querier, error) {
	// Surface the limits to the backend so it can reject/bound a broad matcher or a
	// huge read up-front; this wrapper still enforces them cumulatively across the request.
	opts.MaxEvaluatedSeries = q.opts.MaxEvaluatedSeries
	opts.MaxEvaluatedPoints = q.opts.MaxEvaluatedPoints

	qInner, err := q.q.QuerierWithOptions(opts, q.fixTime(mint), q.fixTime(maxt))
	if err != nil {
		return nil, err
	}

	return querier{
		parent: q,
		inner:  qInner,
	}, nil
}

func (q *Queryable) ChunkQuerierWithOptions(opts types.QuerierOptions, mint, maxt int64) (storage.ChunkQuerier, error) {
	// See QuerierWithOptions: surface the limits to the backend.
	opts.MaxEvaluatedSeries = q.opts.MaxEvaluatedSeries
	opts.MaxEvaluatedPoints = q.opts.MaxEvaluatedPoints

	inner, err := q.q.ChunkQuerierWithOptions(opts, q.fixTime(mint), q.fixTime(maxt))
	if err != nil {
		return nil, err
	}

	return chunkQuerier{
		parent: q,
		inner:  inner,
	}, nil
}

func (q *Queryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return q.QuerierWithOptions(types.QuerierOptions{}, mint, maxt)
}

func (q *Queryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return q.ChunkQuerierWithOptions(types.QuerierOptions{}, mint, maxt)
}

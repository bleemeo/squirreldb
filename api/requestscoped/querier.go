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

//nolint:dupl
package requestscoped

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type pendingQuerier struct {
	mint int64
	maxt int64
	// q is nil until first call to any method (which will provide ctx).
	// This assume the contextValue associated stay the same (i.e. context don't change) during whole lifetime
	// of the Querier. An assertion ensure this invariant.
	q          storage.Querier
	ctxValue   *contextValue
	querierErr error
}

func (p *pendingQuerier) getQuerier(ctx context.Context) (storage.Querier, error) {
	ctxValue := getContextValue(ctx)
	if ctxValue == nil {
		return nil, ErrNoContextValue
	}

	if p.ctxValue == nil {
		p.ctxValue = ctxValue
	}

	if p.ctxValue != ctxValue {
		return nil, ErrInconsistentContext
	}

	if p.q == nil && p.querierErr == nil {
		p.q, p.querierErr = ctxValue.makeQuerier(p.mint, p.maxt)
	}

	return p.q, p.querierErr
}

func (p *pendingQuerier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	q, err := p.getQuerier(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	return q.Select(ctx, sortSeries, hints, matchers...)
}

func (p *pendingQuerier) LabelValues(
	ctx context.Context,
	name string,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	q, err := p.getQuerier(ctx)
	if err != nil {
		return nil, nil, err
	}

	return q.LabelValues(ctx, name, hints, matchers...)
}

func (p *pendingQuerier) LabelNames(
	ctx context.Context,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	q, err := p.getQuerier(ctx)
	if err != nil {
		return nil, nil, err
	}

	return q.LabelNames(ctx, hints, matchers...)
}

func (p *pendingQuerier) Close() error {
	if p.q == nil {
		return nil
	}

	return p.q.Close()
}

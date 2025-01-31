// Copyright 2015-2025 Bleemeo
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

package promql

import (
	"context"
	"errors"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

type filteringIndex struct {
	index   types.Index
	matcher *labels.Matcher
}

func (idx filteringIndex) AllIDs(_ context.Context, _ time.Time, _ time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx filteringIndex) LookupIDs(
	_ context.Context,
	_ []types.LookupRequest,
) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx filteringIndex) Search(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) (types.MetricsSet, error) {
	filterMatcher := make([]*labels.Matcher, 0, len(matchers)+1)
	filterMatcher = append(filterMatcher, idx.matcher)
	filterMatcher = append(filterMatcher, matchers...)

	return idx.index.Search(ctx, start, end, filterMatcher)
}

func (idx filteringIndex) LabelValues(
	ctx context.Context,
	start, end time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	filterMatcher := make([]*labels.Matcher, 0, len(matchers)+1)
	filterMatcher = append(filterMatcher, idx.matcher)
	filterMatcher = append(filterMatcher, matchers...)

	return idx.index.LabelValues(ctx, start, end, name, filterMatcher)
}

func (idx filteringIndex) LabelNames(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) ([]string, error) {
	filterMatcher := make([]*labels.Matcher, 0, len(matchers)+1)
	filterMatcher = append(filterMatcher, idx.matcher)
	filterMatcher = append(filterMatcher, matchers...)

	return idx.index.LabelNames(ctx, start, end, filterMatcher)
}

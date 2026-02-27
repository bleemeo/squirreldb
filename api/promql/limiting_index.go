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
	"sync/atomic"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

type limitingIndex struct {
	index          types.Index
	maxTotalSeries uint32
	returnedSeries *uint32
}

func (idx *limitingIndex) AllIDs(_ context.Context, _ time.Time, _ time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx *limitingIndex) LookupIDs(
	_ context.Context,
	_ []types.LookupRequest,
) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx *limitingIndex) Search(
	ctx context.Context,
	start time.Time, end time.Time,
	matchers []*labels.Matcher,
) (types.MetricsSet, error) {
	r, err := idx.index.Search(ctx, start, end, matchers)
	if err != nil {
		return r, err //nolint:wrapcheck
	}

	totalSeries := atomic.AddUint32(idx.returnedSeries, uint32(r.Count())) //nolint:gosec
	if idx.maxTotalSeries != 0 && totalSeries > idx.maxTotalSeries {
		return &dummy.MetricsLabel{}, errors.New("too many series evaluated by this PromQL")
	}

	return r, nil
}

func (idx *limitingIndex) LabelValues(
	ctx context.Context,
	start, end time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	return idx.index.LabelValues(ctx, start, end, name, matchers)
}

func (idx *limitingIndex) LabelNames(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) ([]string, error) {
	return idx.index.LabelNames(ctx, start, end, matchers)
}

func (idx *limitingIndex) SeriesReturned() float64 {
	v := atomic.LoadUint32(idx.returnedSeries)

	return float64(v)
}

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

type reducedTimeRangeIndex struct {
	index types.Index
}

// Those min and max date must be narrowed than min/max from Cassandra index.
//
//nolint:gochecknoglobals
var (
	minTime = time.Unix(0, 0).UTC()
	maxTime = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
)

func fixTime(t time.Time) time.Time {
	if t.Before(minTime) {
		return minTime
	}

	if t.After(maxTime) {
		return maxTime
	}

	return t
}

func (idx reducedTimeRangeIndex) AllIDs(_ context.Context, _ time.Time, _ time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx reducedTimeRangeIndex) LookupIDs(
	_ context.Context,
	_ []types.LookupRequest,
) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx reducedTimeRangeIndex) Search(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) (types.MetricsSet, error) {
	return idx.index.Search(ctx, fixTime(start), fixTime(end), matchers)
}

func (idx reducedTimeRangeIndex) LabelValues(
	ctx context.Context,
	start, end time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	return idx.index.LabelValues(ctx, fixTime(start), fixTime(end), name, matchers)
}

func (idx reducedTimeRangeIndex) LabelNames(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) ([]string, error) {
	return idx.index.LabelNames(ctx, fixTime(start), fixTime(end), matchers)
}

package promql

import (
	"context"
	"errors"
	"squirreldb/types"
	"time"

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

func (idx reducedTimeRangeIndex) AllIDs(ctx context.Context, start time.Time, end time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx reducedTimeRangeIndex) LookupIDs(
	ctx context.Context,
	requests []types.LookupRequest,
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

package promql

import (
	"context"
	"errors"
	"squirreldb/dummy"
	"squirreldb/types"
	"sync/atomic"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

type limitingIndex struct {
	index          types.Index
	maxTotalSeries uint32
	returnedSeries uint32
}

func (idx *limitingIndex) AllIDs(ctx context.Context, start time.Time, end time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx *limitingIndex) LookupIDs(ctx context.Context, requests []types.LookupRequest) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx *limitingIndex) Search(ctx context.Context, start time.Time, end time.Time, matchers []*labels.Matcher) (types.MetricsSet, error) {
	r, err := idx.index.Search(ctx, start, end, matchers)
	if err != nil {
		return r, err //nolint: wrapcheck
	}

	totalSeries := atomic.AddUint32(&idx.returnedSeries, uint32(r.Count()))
	if idx.maxTotalSeries != 0 && totalSeries > idx.maxTotalSeries {
		return &dummy.MetricsLabel{}, errors.New("too many series evaluated by this PromQL")
	}

	return r, nil
}

func (idx *limitingIndex) LabelValues(ctx context.Context, start, end time.Time, name string, matchers []*labels.Matcher) ([]string, error) {
	return idx.index.LabelValues(ctx, start, end, name, matchers)
}

func (idx *limitingIndex) LabelNames(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) ([]string, error) {
	return idx.index.LabelNames(ctx, start, end, matchers)
}

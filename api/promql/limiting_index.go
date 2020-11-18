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

func (idx *limitingIndex) AllIDs(start time.Time, end time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx *limitingIndex) LookupIDs(ctx context.Context, requests []types.LookupRequest) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx *limitingIndex) Search(start time.Time, end time.Time, matchers []*labels.Matcher) (types.MetricsSet, error) {
	r, err := idx.index.Search(start, end, matchers)
	if err != nil {
		return r, err // nolint: wrapcheck
	}

	totalSeries := atomic.AddUint32(&idx.returnedSeries, uint32(r.Count()))
	if totalSeries > idx.maxTotalSeries {
		return &dummy.MetricsLabel{}, errors.New("too many series evaluated by this PromQL")
	}

	return r, nil
}

func (idx *limitingIndex) LabelValues(start, end time.Time, name string, matchers []*labels.Matcher) ([]string, error) {
	return idx.index.LabelValues(start, end, name, matchers)
}

func (idx *limitingIndex) LabelNames(start, end time.Time, matchers []*labels.Matcher) ([]string, error) {
	return idx.index.LabelNames(start, end, matchers)
}

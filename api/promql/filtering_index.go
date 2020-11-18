package promql

import (
	"context"
	"errors"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

type filteringIndex struct {
	index   types.Index
	matcher *labels.Matcher
}

func (idx filteringIndex) AllIDs(start time.Time, end time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx filteringIndex) LookupIDs(ctx context.Context, requests []types.LookupRequest) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx filteringIndex) Search(start time.Time, end time.Time, matchers []*labels.Matcher) (types.MetricsSet, error) {
	filterMatcher := make([]*labels.Matcher, 0, len(matchers)+1)
	filterMatcher = append(filterMatcher, idx.matcher)
	filterMatcher = append(filterMatcher, matchers...)

	return idx.index.Search(start, end, filterMatcher)
}

func (idx filteringIndex) LabelValues(start, end time.Time, name string, matchers []*labels.Matcher) ([]string, error) {
	filterMatcher := make([]*labels.Matcher, 0, len(matchers)+1)
	filterMatcher = append(filterMatcher, idx.matcher)
	filterMatcher = append(filterMatcher, matchers...)

	return idx.index.LabelValues(start, end, name, filterMatcher)
}

func (idx filteringIndex) LabelNames(start, end time.Time, matchers []*labels.Matcher) ([]string, error) {
	filterMatcher := make([]*labels.Matcher, 0, len(matchers)+1)
	filterMatcher = append(filterMatcher, idx.matcher)
	filterMatcher = append(filterMatcher, matchers...)

	return idx.index.LabelNames(start, end, filterMatcher)
}

package promql

import (
	"context"
	"errors"
	"squirreldb/types"

	"github.com/prometheus/prometheus/pkg/labels"
)

type filteringIndex struct {
	index   types.Index
	matcher *labels.Matcher
}

func (idx filteringIndex) AllIDs() ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx filteringIndex) LookupLabels(id types.MetricID) (labels.Labels, error) {
	// LookupLabels is used on result from Search.
	return idx.index.LookupLabels(id)
}

func (idx filteringIndex) LookupIDs(ctx context.Context, labelsList []labels.Labels) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx filteringIndex) Search(matchers []*labels.Matcher) ([]types.MetricID, error) {
	filterMatcher := make([]*labels.Matcher, 0, len(matchers)+1)
	filterMatcher = append(filterMatcher, idx.matcher)
	filterMatcher = append(filterMatcher, matchers...)

	return idx.index.Search(filterMatcher)
}

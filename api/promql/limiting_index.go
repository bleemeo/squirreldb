package promql

import (
	"errors"
	"squirreldb/types"
	"sync/atomic"

	"github.com/prometheus/prometheus/pkg/labels"
)

type limitingIndex struct {
	index          types.Index
	maxTotalSeries uint32
	returnedSeries uint32
}

func (idx *limitingIndex) AllIDs() ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx *limitingIndex) LookupLabels(id types.MetricID) (labels.Labels, error) {
	// LookupLabels is used on result from Search.
	return idx.index.LookupLabels(id)
}

func (idx *limitingIndex) LookupIDs(labelsList []labels.Labels) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx *limitingIndex) Search(matchers []*labels.Matcher) ([]types.MetricID, error) {
	r, err := idx.index.Search(matchers)
	if err != nil {
		return r, err
	}

	totalSeries := atomic.AddUint32(&idx.returnedSeries, uint32(len(r)))
	if totalSeries > idx.maxTotalSeries {
		return nil, errors.New("too many series evaluated by this PromQL")
	}

	return r, err
}

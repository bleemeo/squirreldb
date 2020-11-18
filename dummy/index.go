package dummy

import (
	"context"
	"fmt"
	"log"
	"sort"
	"squirreldb/types"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

type MetricsLabel struct {
	List []types.MetricLabel
	next int
}

func (l *MetricsLabel) Next() bool {
	if l.next >= len(l.List) {
		return false
	}

	l.next++

	return true
}

func (l *MetricsLabel) At() types.MetricLabel {
	return l.List[l.next-1]
}

func (l *MetricsLabel) Err() error {
	return nil
}

func (l *MetricsLabel) Count() int {
	return len(l.List)
}

// MetricsSetEqual tests if two MetricsSet contains the same list in the same order.
func MetricsSetEqual(a, b types.MetricsSet) bool {
	if a.Count() != b.Count() {
		return false
	}

	for {
		an := a.Next()
		bn := b.Next()

		if an != bn {
			return false
		}

		if !an {
			break
		}

		if a.At().ID != b.At().ID || labels.Compare(a.At().Labels, b.At().Labels) != 0 {
			return false
		}
	}

	return (a.Err() == nil) == (b.Err() == nil)
}

// Index implement a non-working index. It only useful for testing/benchmark. See each function for their limitation.
type Index struct {
	StoreMetricIDInMemory bool
	FixedValue            types.MetricID
	mutex                 sync.Mutex
	labelsToID            map[string]types.MetricID
	idToLabels            map[types.MetricID]labels.Labels
}

// AllIDs does not store IDs in any persistent store. So this is lost after every restart. It may even not store them at all!
func (idx *Index) AllIDs(start time.Time, end time.Time) ([]types.MetricID, error) {
	return nil, nil
}

// lookupLabels required StoreMetricIDInMemory (so not persistent after restart).
func (idx *Index) lookupLabels(ids []types.MetricID) ([]labels.Labels, error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	results := make([]labels.Labels, len(ids))

	for i, id := range ids {
		var ok bool

		results[i], ok = idx.idToLabels[id]
		if !ok {
			return nil, fmt.Errorf("labels for metric ID %d not found", id)
		}
	}

	return results, nil
}

// LookupIDs may have collision. If StoreMetricIDInMemory collision are checked (but not across restart).
func (idx *Index) LookupIDs(ctx context.Context, requests []types.LookupRequest) ([]types.MetricID, []int64, error) {
	ids := make([]types.MetricID, len(requests))
	ttls := make([]int64, len(requests))

	if idx.FixedValue != 0 {
		for i := range ids {
			ids[i] = idx.FixedValue
		}

		return ids, ttls, nil
	}

	for i, req := range requests {
		ttls[i] = timeToLiveFromLabels(&req.Labels)
		if ttls[i] == 0 {
			ttls[i] = int64(86400)
		}

		ids[i] = types.MetricID(req.Labels.Hash())
	}

	if !idx.StoreMetricIDInMemory {
		return ids, ttls, nil
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.idToLabels == nil {
		idx.idToLabels = make(map[types.MetricID]labels.Labels)
		idx.labelsToID = make(map[string]types.MetricID)
	}

	for i, id := range ids {
		current := requests[i].Labels
		previous, ok := idx.idToLabels[id]

		sort.Sort(current)

		if ok && !labels.Equal(previous, current) {
			return nil, nil, fmt.Errorf("collision in ID for %v and %v", current, previous)
		}

		idx.idToLabels[id] = current
		idx.labelsToID[current.String()] = id
	}

	return ids, ttls, nil
}

// Search only works when StoreMetricIDInMemory is enabled.
func (idx *Index) Search(queryStart time.Time, queryEnd time.Time, matchers []*labels.Matcher) (types.MetricsSet, error) {
	ids := make([]types.MetricID, 0)

outer:
	for id, lbs := range idx.idToLabels {
		for _, m := range matchers {
			l := lbs.Get(m.Name)
			if !m.Matches(l) {
				continue outer
			}
		}

		ids = append(ids, id)
	}

	labelsList, err := idx.lookupLabels(ids)
	if err != nil {
		return nil, err
	}

	results := make([]types.MetricLabel, len(ids))

	for i, id := range ids {
		results[i] = types.MetricLabel{
			ID:     id,
			Labels: labelsList[i],
		}
	}

	return &MetricsLabel{List: results}, nil
}

// copied from cassandra/index.
func timeToLiveFromLabels(labels *labels.Labels) int64 {
	value, exists := popLabelsValue(labels, "__ttl__")

	var timeToLive int64

	if exists {
		var err error
		timeToLive, err = strconv.ParseInt(value, 10, 64)

		if err != nil {
			log.Printf("Warning: Can't get time to live from labels (%v), using default", err)

			return 0
		}
	}

	return timeToLive
}

// copied from cassandra/index.
func popLabelsValue(labels *labels.Labels, key string) (string, bool) {
	for i, label := range *labels {
		if label.Name == key {
			*labels = append((*labels)[:i], (*labels)[i+1:]...)

			return label.Value, true
		}
	}

	return "", false
}

package dummy

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"squirreldb/types"
	"strconv"
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
)

// Index implement a non-working index. It only useful for testing/benchmark. See each function for their limitation
type Index struct {
	StoreMetricIDInMemory bool
	FixedValue            types.MetricID
	mutex                 sync.Mutex
	labelsToID            map[string]types.MetricID
	idToLabels            map[types.MetricID]labels.Labels
}

// AllIDs does not store IDs in any persistent store. So this is lost after every restart. It may even not store them at all!
func (idx *Index) AllIDs() ([]types.MetricID, error) {
	return nil, nil
}

// LookupLabels required StoreMetricIDInMemory (so not persistent after restart)
func (idx *Index) LookupLabels(id types.MetricID) (labels.Labels, error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	return idx.idToLabels[id], nil
}

// LookupIDs may have collision. If StoreMetricIDInMemory collision are checked (but not across restart).
func (idx *Index) LookupIDs(labelsList []labels.Labels) ([]types.MetricID, []int64, error) {
	ids := make([]types.MetricID, len(labelsList))
	ttls := make([]int64, len(labelsList))

	if idx.FixedValue != 0 {
		for i := range ids {
			ids[i] = idx.FixedValue
		}

		return ids, ttls, nil
	}

	for i, l := range labelsList {
		l := l

		ttls[i] = timeToLiveFromLabels(&l)
		if ttls[i] == 0 {
			ttls[i] = int64(86400)
		}

		ids[i] = types.MetricID(l.Hash())
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
		current := labelsList[i]
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

// Search is not implemented at all
func (idx *Index) Search(matchers []*labels.Matcher) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

// copied from cassandra/index
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

// copied from cassandra/index
func popLabelsValue(labels *labels.Labels, key string) (string, bool) {
	for i, label := range *labels {
		if label.Name == key {
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
			return label.Value, true
		}
	}

	return "", false
}

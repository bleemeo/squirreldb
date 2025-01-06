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

package dummy

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	postinglabelName  = "__label|names__"
	defaultTimeToLive = 24 * time.Hour
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

// NewIndex create a new index pre-filled with metrics.
func NewIndex(metrics []types.MetricLabel) *Index {
	idx := &Index{
		StoreMetricIDInMemory: true,
		labelsToID:            make(map[string]types.MetricID, len(metrics)),
		idToLabels:            make(map[types.MetricID]labels.Labels, len(metrics)),
	}

	for _, m := range metrics {
		lbl := m.Labels.Copy()

		sort.Sort(lbl)

		idx.idToLabels[m.ID] = lbl
		idx.labelsToID[lbl.String()] = m.ID
	}

	return idx
}

// AllIDs does not store IDs in any persistent store.
// So this is lost after every restart. It may even not store them at all!
func (idx *Index) AllIDs(ctx context.Context, _ time.Time, _ time.Time) ([]types.MetricID, error) {
	return nil, ctx.Err()
}

// lookupLabels required StoreMetricIDInMemory (so not persistent after restart).
func (idx *Index) lookupLabels(ids []types.MetricID) ([]labels.Labels, error) {
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
		ttls[i] = req.TTLSeconds
		if ttls[i] == 0 {
			ttls[i] = int64(defaultTimeToLive.Seconds())
		}

		ids[i] = types.MetricID(req.Labels.Hash()) //nolint:gosec
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

	return ids, ttls, ctx.Err()
}

// Search only works when StoreMetricIDInMemory is enabled.
func (idx *Index) Search(
	ctx context.Context,
	_ time.Time,
	_ time.Time,
	matchers []*labels.Matcher,
) (types.MetricsSet, error) {
	ids := make([]types.MetricID, 0)

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

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

	return &MetricsLabel{List: results}, ctx.Err()
}

// LabelValues only works when StoreMetricIDInMemory is enabled.
func (idx *Index) LabelValues(
	ctx context.Context,
	_, _ time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	if name == "" || strings.Contains(name, "|") {
		return nil, fmt.Errorf("invalid label name \"%s\"", name)
	}

	return idx.labelValues(name, matchers), ctx.Err()
}

// LabelNames only works when StoreMetricIDInMemory is enabled.
func (idx *Index) LabelNames(ctx context.Context, _, _ time.Time, matchers []*labels.Matcher) ([]string, error) {
	return idx.labelValues(postinglabelName, matchers), ctx.Err()
}

func (idx *Index) labelValues(name string, matchers []*labels.Matcher) []string {
	results := make(map[string]interface{})

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

outer:
	for _, lbs := range idx.idToLabels {
		for _, m := range matchers {
			l := lbs.Get(m.Name)
			if !m.Matches(l) {
				continue outer
			}
		}

		if name == postinglabelName {
			for _, l := range lbs {
				results[l.Name] = nil
			}
		} else if v := lbs.Get(name); v != "" {
			results[v] = nil
		}
	}

	list := make([]string, 0, len(results))

	for n := range results {
		list = append(list, n)
	}

	sort.Strings(list)

	return list
}

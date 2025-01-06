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
	"sync"

	"github.com/bleemeo/squirreldb/cassandra/tsdb"
	"github.com/bleemeo/squirreldb/types"
)

// cachingReader keeps the last MetricData returned in cache and re-uses it if that query exactly matches the request.
type cachingReader struct {
	reader types.MetricReader

	l                 sync.Mutex
	cachedPointsCount float64
	cachedRequest     types.MetricRequest
	cachedResult      types.MetricData
	cachedCurrentNext int
	cachedMaxNext     int
}

func (rdr *cachingReader) PointsCached() float64 {
	rdr.l.Lock()
	defer rdr.l.Unlock()

	return rdr.cachedPointsCount
}

func (rdr *cachingReader) ReadIter(ctx context.Context, req types.MetricRequest) (types.MetricDataSet, error) {
	return &cachingDataSet{
		rdr:     rdr,
		request: req,
		ctx:     ctx,
	}, nil
}

type cachingDataSet struct {
	request types.MetricRequest
	rdr     *cachingReader
	ctx     context.Context //nolint: containedctx

	nextCount int
	set       types.MetricDataSet
	err       error
	seenIDs   map[types.MetricID]interface{}
	current   types.MetricData
}

// requestSameCache check that the request is the same for the cache.
func requestSameCache(x, y types.MetricRequest) bool {
	if !tsdb.RequestUseAggregatedData(x) && !tsdb.RequestUseAggregatedData(y) {
		// If none of the request use aggregated data, the aggregation function doesn't matter.
		x.Function = ""
		y.Function = ""
	}

	return x.Function == y.Function &&
		x.FromTimestamp == y.FromTimestamp &&
		x.ToTimestamp == y.ToTimestamp &&
		x.ForcePreAggregated == y.ForcePreAggregated &&
		x.ForceRaw == y.ForceRaw &&
		x.StepMs == y.StepMs &&
		idsEqual(x, y)
}

func idsEqual(x, y types.MetricRequest) bool {
	if len(x.IDs) != len(y.IDs) {
		return false
	}

	for i, x := range x.IDs {
		if x != y.IDs[i] {
			return false
		}
	}

	return true
}

func (d *cachingDataSet) Next() bool {
	if d.err != nil {
		return false
	}

	if d.set == nil {
		cacheHit, result := d.nextFromCache()
		if cacheHit {
			return result
		}

		d.filterRequestIDs()
		// We re-created the request, reset nextCounter
		d.nextCount = 0

		d.set, d.err = d.rdr.reader.ReadIter(d.ctx, d.request)
		if d.err != nil {
			return false
		}
	}

	d.nextCount++

	result := d.set.Next()

	if result {
		d.current = d.set.At()

		d.rdr.l.Lock()

		d.rdr.cachedRequest = d.request
		d.rdr.cachedCurrentNext = d.nextCount
		d.rdr.cachedResult = d.current

		d.rdr.l.Unlock()
	} else {
		d.rdr.l.Lock()

		if requestSameCache(d.rdr.cachedRequest, d.request) {
			d.rdr.cachedMaxNext = d.nextCount
		}

		d.rdr.l.Unlock()
	}

	return result
}

func (d *cachingDataSet) nextFromCache() (cacheHit bool, returnValue bool) {
	d.rdr.l.Lock()
	defer d.rdr.l.Unlock()

	d.nextCount++

	if d.nextCount != d.rdr.cachedCurrentNext && d.nextCount != d.rdr.cachedMaxNext {
		return false, false
	}

	reqEqual := requestSameCache(d.rdr.cachedRequest, d.request)

	if reqEqual && d.nextCount == d.rdr.cachedCurrentNext {
		d.current = d.rdr.cachedResult

		if d.seenIDs == nil {
			d.seenIDs = make(map[types.MetricID]interface{}, len(d.request.IDs))
		}

		d.seenIDs[d.current.ID] = nil

		return true, true
	}

	if reqEqual && d.nextCount == d.rdr.cachedMaxNext {
		d.rdr.cachedPointsCount += float64(len(d.current.Points))

		return true, false
	}

	return false, false
}

// filterRequestIDs remove IDs that were already seen using the cache.
// d.request is modified.
func (d *cachingDataSet) filterRequestIDs() {
	// No need to filter when nextCount == 1, because seenIDs will be empty.
	if d.nextCount == 1 {
		return
	}

	i := 0

	for _, id := range d.request.IDs {
		if _, ok := d.seenIDs[id]; ok {
			// This ID was already returned by the cache, skip it
			continue
		}

		d.request.IDs[i] = id
		i++
	}

	d.request.IDs = d.request.IDs[:i]
}

func (d *cachingDataSet) At() types.MetricData {
	return d.current
}

func (d *cachingDataSet) Err() error {
	if d.err != nil {
		return d.err
	}

	if d.set == nil {
		return nil
	}

	return d.set.Err()
}

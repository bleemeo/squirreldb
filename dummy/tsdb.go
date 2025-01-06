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
	"sort"
	"sync"

	"github.com/bleemeo/squirreldb/types"
)

// DiscardTSDB will write metrics to /dev/null.
type DiscardTSDB struct{}

type emptyResult struct{}

// ReadIter return an empty result.
func (d DiscardTSDB) ReadIter(_ context.Context, _ types.MetricRequest) (types.MetricDataSet, error) {
	return emptyResult{}, nil
}

// Write discard metrics.
func (d DiscardTSDB) Write(_ context.Context, _ []types.MetricData) error {
	return nil
}

func (d DiscardTSDB) Run(ctx context.Context, readiness chan error) {
	readiness <- nil

	<-ctx.Done()
}

func (d DiscardTSDB) Flush() {
}

func (r emptyResult) Next() bool {
	return false
}

func (r emptyResult) At() types.MetricData {
	panic("At() shouldn't be called on emptyResult")
}

func (r emptyResult) Err() error {
	return nil
}

// MemoryTSDB store all value in memory. Only useful in unittest.
type MemoryTSDB struct {
	mutex      sync.Mutex
	Data       map[types.MetricID]types.MetricData
	LogRequest bool
	Reads      []types.MetricRequest
	Writes     [][]types.MetricData
}

type readIter struct {
	db      *MemoryTSDB
	current types.MetricData
	request types.MetricRequest
	offset  int
}

// DumpData dump to content of the TSDB. Result is ordered by MetricID.
func (db *MemoryTSDB) DumpData() []types.MetricData {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	result := make([]types.MetricData, 0, len(db.Data))

	for _, v := range db.Data {
		result = append(result, v)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result
}

// ReadIter return an empty result.
func (db *MemoryTSDB) ReadIter(_ context.Context, request types.MetricRequest) (types.MetricDataSet, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.LogRequest {
		db.Reads = append(db.Reads, request)
	}

	return &readIter{
		request: request,
		db:      db,
	}, nil
}

// Write store in memory.
func (db *MemoryTSDB) Write(_ context.Context, metrics []types.MetricData) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.LogRequest {
		db.Writes = append(db.Writes, metrics)
	}

	if db.Data == nil {
		db.Data = make(map[types.MetricID]types.MetricData)
	}

	for _, m := range metrics {
		m.Points = append(db.Data[m.ID].Points, m.Points...)
		db.Data[m.ID] = m
	}

	return nil
}

func (r *readIter) Next() bool {
	r.db.mutex.Lock()
	defer r.db.mutex.Unlock()

	for {
		if r.offset >= len(r.request.IDs) {
			return false
		}

		id := r.request.IDs[r.offset]
		r.offset++
		r.current = r.db.Data[id]

		r.current.Points = filterPoints(r.current.Points, r.request)
		if len(r.current.Points) == 0 {
			continue
		}

		return true
	}
}

func (r *readIter) At() types.MetricData {
	return r.current
}

func (r *readIter) Err() error {
	return nil
}

func filterPoints(points []types.MetricPoint, request types.MetricRequest) []types.MetricPoint {
	// Avoid allocation if all points matches
	needFilter := false

	for _, p := range points {
		if p.Timestamp < request.FromTimestamp || p.Timestamp > request.ToTimestamp {
			needFilter = true

			break
		}
	}

	if !needFilter {
		return points
	}

	result := make([]types.MetricPoint, 0, len(points))

	for _, p := range points {
		if p.Timestamp < request.FromTimestamp || p.Timestamp > request.ToTimestamp {
			continue
		}

		result = append(result, p)
	}

	return result
}

package dummy

import (
	"context"
	"squirreldb/types"
	"sync"
)

// DiscardTSDB will write metrics to /dev/null.
type DiscardTSDB struct{}

type emptyResult struct{}

// ReadIter return an empty result.
func (d DiscardTSDB) ReadIter(request types.MetricRequest) (types.MetricDataSet, error) {
	return emptyResult{}, nil
}

// Write discard metrics.
func (d DiscardTSDB) Write(metrics []types.MetricData) error {
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
	mutex sync.Mutex
	Data  map[types.MetricID]types.MetricData
}

type readIter struct {
	request types.MetricRequest
	offset  int
	db      *MemoryTSDB
	current types.MetricData
}

// ReadIter return an empty result.
func (db *MemoryTSDB) ReadIter(request types.MetricRequest) (types.MetricDataSet, error) {
	return &readIter{
		request: request,
		db:      db,
	}, nil
}

// Write store in memory.
func (db *MemoryTSDB) Write(metrics []types.MetricData) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

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
	if r.offset >= len(r.request.IDs) {
		return false
	}

	id := r.request.IDs[r.offset]
	r.offset++

	r.db.mutex.Lock()
	defer r.db.mutex.Unlock()
	r.current = r.db.Data[id]

	return true
}

func (r *readIter) At() types.MetricData {
	return r.current
}

func (r *readIter) Err() error {
	return nil
}

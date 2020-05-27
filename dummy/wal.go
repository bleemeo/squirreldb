package dummy

import (
	"squirreldb/types"
	"sync"
)

type Wal struct {
	MockReadOther []types.MetricData

	mutex        sync.Mutex
	wal          []types.MetricData
	checkpointed []types.MetricData
}

func (w *Wal) Write(metrics []types.MetricData) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.wal = append(w.wal, metrics...)

	return nil
}

func (w *Wal) Checkpoint() types.WalCheckpoint {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.checkpointed = w.wal
	w.wal = nil

	return checkpoint{w: w}
}

func (w *Wal) ReadWAL() ([]types.MetricData, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.wal, nil
}

func (w *Wal) Flush() {}

type checkpoint struct {
	w *Wal
}

func (c checkpoint) Abort() {
	c.w.mutex.Lock()
	defer c.w.mutex.Unlock()

	c.w.wal = append(c.w.checkpointed, c.w.wal...)
}

func (c checkpoint) Purge() {
	c.w.mutex.Lock()
	defer c.w.mutex.Unlock()

	c.w.checkpointed = nil
}

func (c checkpoint) ReadOther() ([]types.MetricData, error) {
	r := c.w.MockReadOther
	c.w.MockReadOther = nil

	return r, nil
}

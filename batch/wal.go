package batch

import (
	"context"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

const flushDelay = 15 * time.Minute

// WalBatcher will receive write, store them in a WAL and periodically send them to persistent store
//
// The WAL store is assumed to quickly persist data sent to it and allow to re-read written data & delete them.
// The WAL store will receive data as received by batcher, that is a bulk data from multiple metrics.
//
// Periodically, the WalBatcher will re-read from WAL store and merge points of the same metrics to send them to the
// persistent store.
//
// On startup, WalBatcher will ask WAL store to re-read all data in order to pre-fill in-memory cache of data.
//
// Finally the WalBatcher allow to query for it's in-memory data.
type WalBatcher struct {
	WalStore                types.WalStore
	PersitentStore          types.MetricReadWriter
	WriteToPersistentOnStop bool
	stopNewWrite            bool

	writeLock      *sync.Cond
	lastFlush      time.Time
	flushToken     chan interface{}
	pendingWrite   int
	dirty          map[types.MetricID]types.MetricData
	writing        map[types.MetricID]types.MetricData
	pendingSumTS   float64
	pendingPoints  int
	lastAvgAtFlush int64
}

func (w *WalBatcher) Run(ctx context.Context, readiness chan error) {
	w.writeLock = sync.NewCond(&sync.Mutex{})
	w.flushToken = make(chan interface{}, 1)

	var wg sync.WaitGroup

	subCtx, cancel := context.WithCancel(context.Background())

	if task, ok := w.WalStore.(types.Task); ok {
		subReadiness := make(chan error)

		wg.Add(1)

		go func() {
			defer wg.Done()
			task.Run(subCtx, subReadiness)
		}()

		err := <-subReadiness
		if err != nil {
			cancel()
			wg.Wait()
			readiness <- err

			return
		}
	}

	readiness <- nil

	w.initFromWal()

	w.flushToken <- nil

	for ctx.Err() == nil {
		select {
		case <-time.After(time.Minute):
			w.writeLock.L.Lock()
			avgTS := int64(w.pendingSumTS / float64(w.pendingPoints))

			if w.lastAvgAtFlush == 0 && avgTS != 0 {
				w.lastAvgAtFlush = avgTS
			}

			if w.writing == nil && (time.Since(w.lastFlush) > flushDelay || avgTS-w.lastAvgAtFlush > flushDelay.Milliseconds()*3/2) {
				w.writeLock.L.Unlock()
				w.Flush()
			} else {
				w.writeLock.L.Unlock()
			}
		case <-ctx.Done():
		}
	}

	if w.WriteToPersistentOnStop {
		w.Flush()
	}

	cancel()
	wg.Wait()
}

func (w *WalBatcher) initFromWal() {
	var (
		data []types.MetricData
		err  error
	)

	retry.Print(func() error {
		data, err = w.WalStore.ReadWAL()
		return err
	}, retry.NewExponentialBackOff(30*time.Second),
		logger,
		"read WAL",
	)

	var (
		oldestTS int64
	)

	for _, metric := range data {
		for _, point := range metric.Points {
			if oldestTS == 0 || oldestTS > point.Timestamp {
				oldestTS = point.Timestamp
			}
		}
	}

	w.writeLock.L.Lock()

	w.dirty, w.pendingSumTS, w.pendingPoints = merge(w.dirty, data)

	if oldestTS != 0 {
		age := time.Since(time.Unix(oldestTS/1000, 0))
		w.lastFlush = time.Now().Add(-age)
	} else {
		w.lastFlush = time.Now()
	}

	w.writeLock.L.Unlock()
}

// Flush write.
func (w *WalBatcher) Flush() {
	start := time.Now()

	<-w.flushToken

	defer func() {
		requestsSecondsFlush.Observe(time.Since(start).Seconds())
	}()

	w.writeLock.L.Lock()
	w.stopNewWrite = true

	for w.pendingWrite > 0 {
		w.writeLock.Wait()
	}

	w.writing = w.dirty
	w.dirty = make(map[types.MetricID]types.MetricData)
	checkpoint := w.WalStore.Checkpoint()
	w.lastFlush = time.Now()

	if w.pendingPoints > 0 {
		w.lastAvgAtFlush = int64(w.pendingSumTS / float64(w.pendingPoints))
		w.pendingPoints = 0
		w.pendingSumTS = 0
	}

	w.writeLock.L.Unlock()

	var (
		tmp []types.MetricData
		err error
	)

	retry.Print(func() error {
		tmp, err = checkpoint.ReadOther()
		return err
	},
		retry.NewExponentialBackOff(30*time.Second),
		logger,
		"Read WAL from Cassandra",
	)

	w.writeLock.L.Lock()

	w.writing, _, _ = merge(w.writing, tmp)

	dataList := make([]types.MetricData, len(w.writing))
	i := 0

	for _, v := range w.writing {
		v.Points = types.DeduplicatePoints(v.Points)
		dataList[i] = v
		i++
	}

	w.writeLock.L.Unlock()

	retry.Print(func() error {
		return w.PersitentStore.Write(dataList)
	},
		retry.NewExponentialBackOff(30*time.Second),
		logger,
		"write to persistent store",
	)

	checkpoint.Purge()

	w.writeLock.L.Lock()

	w.writing = nil
	w.stopNewWrite = false

	w.writeLock.Broadcast()
	w.writeLock.L.Unlock()

	w.flushToken <- nil
}

func (w *WalBatcher) ReadIter(request types.MetricRequest) (types.MetricDataSet, error) {
	return &walReadIter{
		w:       w,
		request: request,
	}, nil
}

func (w *WalBatcher) Write(metrics []types.MetricData) error {
	start := time.Now()

	defer func() {
		requestsSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	w.writeLock.L.Lock()
	for w.stopNewWrite {
		w.writeLock.Wait()
	}
	w.pendingWrite++
	w.writeLock.L.Unlock()

	err := w.WalStore.Write(metrics)
	if err != nil {
		return err
	}

	w.writeLock.L.Lock()
	defer w.writeLock.L.Unlock()

	var (
		sumTS       float64
		countPoints int
	)

	w.dirty, sumTS, countPoints = merge(w.dirty, metrics)
	w.pendingSumTS += sumTS
	w.pendingPoints += countPoints
	w.pendingWrite--

	if w.pendingWrite == 0 {
		w.writeLock.Broadcast()
	}

	return nil
}

func merge(data map[types.MetricID]types.MetricData, metrics []types.MetricData) (map[types.MetricID]types.MetricData, float64, int) {
	if data == nil {
		data = make(map[types.MetricID]types.MetricData)
	}

	var (
		sumTS      float64
		countPoint int
	)

	for _, m := range metrics {
		entry := data[m.ID]
		entry.ID = m.ID
		entry.Points = append(entry.Points, m.Points...)

		for _, p := range m.Points {
			sumTS += float64(p.Timestamp)
		}

		countPoint += len(m.Points)

		if entry.TimeToLive < m.TimeToLive {
			entry.TimeToLive = m.TimeToLive
		}

		data[m.ID] = entry
	}

	return data, sumTS, countPoint
}

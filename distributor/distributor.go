package distributor

import (
	"context"
	"errors"
	"log"
	"os"
	"squirreldb/types"
	"sync"
	"sync/atomic"
	"time"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[distributor] ", log.LstdFlags)

type FlushableStore interface {
	types.MetricReadWriter
	types.Task
	Flush()
}

// Distributor will send metric to the current SquirrelDB master.
//
// The SquirrelDB cluster split all metrics by shard. The shard is choose based
// the metric ID and a modulo (e.g. shard = metricID % shardCount). The
// number of shard is currently fixed.
// Then each shard is assigned to one SquirrelDB node, once more we use a simple
// shard % numberOfOnline SquirrelDB.
// Every metric request (read or write) will be forwarded to the SquirrelDB
// which is the owner of the shard. Since one request (on Prometheus remote store)
// could contains multiple metrics, the distributor can split on request in multiple
// request for each shard.
//
// The processing done by owner of a shard is to quickly persist metric into
// Cassandra (in a WAL), and every owner will check in the Cassandra if another
// SquirrelDB didn't write for the shard. This means that even if we sent metrics
// to the "wrong" owner (e.g. memberlist is not consistent or another transient
// situation), the cluster will eventually heal.
type Distributor struct {
	Memberlist   types.Memberlist
	StoreFactory func(shard int) FlushableStore
	ShardCount   int
	mutex        sync.Mutex
	activeStore  map[int]*runningStore
}

type runningStore struct {
	shardID        int
	Store          FlushableStore
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	pendingRequest int32
}

func (d *Distributor) Run(ctx context.Context, readiness chan error) {
	d.activeStore = make(map[int]*runningStore)
	readiness <- nil

	<-ctx.Done()

	d.shutdown()
}

func (d *Distributor) Flush() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, st := range d.activeStore {
		st.Store.Flush()
	}

	return nil
}

func (d *Distributor) shutdown() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, st := range d.activeStore {
		d.stopStore(st)
	}

	d.activeStore = nil
}

func (d *Distributor) stopStore(st *runningStore) {
	try := 0

	for atomic.LoadInt32(&st.pendingRequest) > 0 {
		time.Sleep(time.Second)

		try++
		if try == 3 {
			logger.Printf("Trying to close store for shard %d, but it still busy", st.shardID)
		}

		if try == 15 {
			logger.Printf("Store for shard %d still busy, try to force its shutdown", st.shardID)
			break
		}
	}
	st.cancel()
	st.wg.Wait()

	activeShard.Dec()

	if try == 15 {
		logger.Printf("Store for shard %d stopped", st.shardID)
	}
}

func (d *Distributor) Write(metrics []types.MetricData) error {
	start := time.Now()

	defer func() {
		requestsSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	if d.Memberlist == nil {
		return d.writeToSelf(0, metrics)
	}

	metricsByShard := splitWriteByShards(metrics, d.ShardCount)
	members := d.Memberlist.Nodes()

	if len(members) == 0 {
		return errors.New("the cluster contains 0 node, can't write")
	}

	var (
		wg  sync.WaitGroup
		err error
		l   sync.Mutex
	)

	for i := range metricsByShard {
		i := i

		if len(metricsByShard[i]) == 0 {
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			err2 := d.writeShardPart(members, i, metricsByShard[i])
			if err2 != nil {
				l.Lock()
				if err != nil {
					err = err2
				}
				l.Unlock()
			}
		}()
	}

	wg.Wait()

	return err
}

func (d *Distributor) ReadIter(request types.MetricRequest) (types.MetricDataSet, error) {
	start := time.Now()

	defer func() {
		requestsSecondsRead.Observe(time.Since(start).Seconds())
	}()

	if d.Memberlist == nil {
		d.mutex.Lock()
		if len(d.activeStore) == 0 {
			if err := d.startStore(0); err != nil {
				d.mutex.Unlock()
				return nil, err
			}
		}

		st := d.activeStore[0]
		atomic.AddInt32(&st.pendingRequest, 1)
		d.mutex.Unlock()

		iter, err := st.Store.ReadIter(request)

		atomic.AddInt32(&st.pendingRequest, -1)

		return iter, err
	}

	members := d.Memberlist.Nodes()
	requestByShard := splitReadByShards(request, d.ShardCount)

	if len(members) == 0 {
		return nil, errors.New("the cluster contains 0 node, can't write")
	}

	return &readIter{
		d:              d,
		members:        members,
		requestByShard: requestByShard,
	}, nil
}

func (d *Distributor) startStore(shardID int) error {
	ctx, cancel := context.WithCancel(context.Background())
	readiness := make(chan error)
	st := &runningStore{
		shardID: shardID,
		Store:   d.StoreFactory(shardID),
		cancel:  cancel,
	}

	st.wg.Add(1)

	go func() {
		defer st.wg.Done()

		st.Store.Run(ctx, readiness)
	}()

	err := <-readiness
	if err != nil {
		return err
	}

	activeShard.Inc()

	d.activeStore[shardID] = st

	return nil
}

func (d *Distributor) writeToSelf(shardID int, metrics []types.MetricData) error {
	d.mutex.Lock()
	if d.activeStore[shardID] == nil {
		if err := d.startStore(shardID); err != nil {
			d.mutex.Unlock()
			return err
		}
	}

	st := d.activeStore[shardID]
	atomic.AddInt32(&st.pendingRequest, 1)
	d.mutex.Unlock()

	err := st.Store.Write(metrics)
	atomic.AddInt32(&st.pendingRequest, -1)

	return err
}

func (d *Distributor) readFromSelf(shardID int, request types.MetricRequest) (types.MetricDataSet, error) {
	d.mutex.Lock()
	if d.activeStore[shardID] == nil {
		if err := d.startStore(shardID); err != nil {
			d.mutex.Unlock()
			return nil, err
		}
	}

	st := d.activeStore[shardID]
	atomic.AddInt32(&st.pendingRequest, 1)
	d.mutex.Unlock()

	metrics, err := st.Store.ReadIter(request)
	atomic.AddInt32(&st.pendingRequest, -1)

	return metrics, err
}

func (d *Distributor) writeShardPart(members []types.Node, shardID int, metrics []types.MetricData) error {
	targetNode := members[shardID%len(members)]
	if targetNode.IsSelf() {
		return d.writeToSelf(shardID, metrics)
	}

	return errors.New("not implemented")
}

func (d *Distributor) readShardPart(members []types.Node, shardID int, request types.MetricRequest) (types.MetricDataSet, error) {
	targetNode := members[shardID%len(members)]
	if targetNode.IsSelf() {
		return d.readFromSelf(shardID, request)
	}

	return nil, errors.New("not implemented")
}

func splitWriteByShards(metrics []types.MetricData, shardCount int) [][]types.MetricData {
	results := make([][]types.MetricData, shardCount)

	defaultSize := len(metrics) / shardCount
	if defaultSize < 1 {
		defaultSize = 1
	}

	for _, m := range metrics {
		shard := int64(m.ID) % int64(shardCount)
		if len(results[shard]) == 0 {
			results[shard] = make([]types.MetricData, 0, defaultSize)
		}

		results[shard] = append(results[shard], m)
	}

	return results
}

func splitReadByShards(requests types.MetricRequest, shardCount int) []types.MetricRequest {
	results := make([]types.MetricRequest, shardCount)

	defaultSize := len(requests.IDs) / shardCount
	if defaultSize < 1 {
		defaultSize = 1
	}

	for _, id := range requests.IDs {
		shard := int64(id) % int64(shardCount)
		if len(results[shard].IDs) == 0 {
			results[shard] = types.MetricRequest{
				IDs:           make([]types.MetricID, 0, defaultSize),
				FromTimestamp: requests.FromTimestamp,
				ToTimestamp:   requests.ToTimestamp,
				Function:      requests.Function,
				StepMs:        requests.StepMs,
			}
		}

		results[shard].IDs = append(results[shard].IDs, id)
	}

	return results
}

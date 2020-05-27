package wal

import (
	"context"
	"log"
	"os"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/snappy"
)

const (
	flushSizeBytes = 8000000
	flushTime      = 10 * time.Second
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[wal] ", log.LstdFlags)

type Cassandra struct {
	Session    *gocql.Session
	SchemaLock sync.Locker
	ShardID    int

	flushDone     *sync.Cond
	flushChan     chan []byte
	flushedTxnIDs map[gocql.UUID]interface{}
	encoder       pool
}

type Checkpoint struct {
	txnIDs      map[gocql.UUID]interface{}
	extraTxnIDs []gocql.UUID
	c           *Cassandra
}

func (c *Cassandra) init() error {
	c.SchemaLock.Lock()
	defer c.SchemaLock.Unlock()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS wal_log (
			shard_id smallint,
			txn_id uuid,
			values blob,
			PRIMARY KEY (shard_id, txn_id)
		)`,
	}

	for _, query := range queries {
		if err := c.Session.Query(query).Consistency(gocql.All).Exec(); err != nil {
			return err
		}
	}

	c.encoder.Init(4)
	c.flushChan = make(chan []byte)
	c.flushDone = sync.NewCond(&sync.Mutex{})
	c.flushedTxnIDs = make(map[gocql.UUID]interface{})

	return nil
}

func (c *Cassandra) Run(ctx context.Context, readiness chan error) {
	err := c.init()
	readiness <- err

	if err != nil {
		return
	}

	ticker := time.NewTicker(flushTime)
	defer ticker.Stop()

	var buffer []byte

	for ctx.Err() == nil {
		buffer = nil

		select {
		case <-ctx.Done():
			buffer = c.encoder.BytesAndReset()
		case <-ticker.C:
			if c.encoder.Len() > 0 {
				buffer = c.encoder.BytesAndReset()
			}
		case buffer = <-c.flushChan:
		}

		if len(buffer) > 0 {
			c.flush(buffer)
		}
	}
}

func (c *Cassandra) Flush() {
	c.flushChan <- c.encoder.BytesAndReset()
	// Send another value to make sure previous one where processed. This
	// allow to wait for completion
	c.flushChan <- nil
}

func (c *Cassandra) Write(metrics []types.MetricData) error {
	start := time.Now()

	defer func() {
		requestsSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	newSize, _, err := c.encoder.EncodeEx(metrics)

	if newSize >= flushSizeBytes {
		c.flushDone.L.Lock()
		for c.encoder.Len() >= flushSizeBytes {
			buffer := c.encoder.BytesAndReset()
			c.flushChan <- buffer
			c.flushDone.Wait()
		}
		c.flushDone.L.Unlock()
	}

	if err != nil {
		return err
	}

	return nil
}

func (c *Cassandra) flush(buffer []byte) {
	start := time.Now()

	defer func() {
		requestsSecondsFlush.Observe(time.Since(start).Seconds())
	}()

	u, err := gocql.RandomUUID()
	if err != nil {
		log.Panicf("Generation random UUID failed: %v", err)
	}

	data := snappy.Encode(nil, buffer)

	retry.Print(func() error {
		return c.Session.Query(
			"INSERT INTO wal_log(shard_id, txn_id, values) VALUES(?, ?, ?) USING TTL 3600",
			c.ShardID,
			u,
			data,
		).Exec()
	}, retry.NewExponentialBackOff(30*time.Second),
		logger,
		"write WAL in Cassandra",
	)

	c.flushDone.L.Lock()
	c.flushedTxnIDs[u] = nil
	c.flushDone.Broadcast()
	c.flushDone.L.Unlock()
}

// Checkpoint create a checkpoint. A checkpoint is a snapshot of all writes done by this instance which could be
// either Aborted (undo the Checkpoint call) or Purged which delete all writes covered by the snapshot.
//
// You can NOT call Checkpoint if a previous checkpoint were not either Aborted or Purged
//
// All call to Write should be stopped before calling Checkpoint. If not you can not known if a currently running Write
// will be included in the Checkpoint or not
// Write may resume as soon as Checkpoint finished. Only Write that happened BEFORE call to checkpoint will be deleted.
func (c *Cassandra) Checkpoint() types.WalCheckpoint {
	start := time.Now()

	defer func() {
		requestsSecondsCheckpoint.Observe(time.Since(start).Seconds())
	}()

	c.Flush()
	c.flushDone.L.Lock()
	defer c.flushDone.L.Unlock()

	txnIDs := c.flushedTxnIDs
	c.flushedTxnIDs = make(map[gocql.UUID]interface{})

	return &Checkpoint{
		txnIDs: txnIDs,
		c:      c,
	}
}

// ReadWAL read all WAL for this shard and mark those WAL as belonging to this instance (e.g. ReadOther from checkpoint won't return them)
func (c *Cassandra) ReadWAL() ([]types.MetricData, error) {
	var (
		txnIDs           []gocql.UUID
		bufferCompressed []byte
		buffer           []byte
		current          []types.MetricData
		allData          []types.MetricData
		u                gocql.UUID
		err              error
	)

	iter := c.Session.Query(
		"SELECT txn_id, values FROM wal_log WHERE shard_id = ?",
		c.ShardID,
	).PageSize(1).Iter()

	for iter.Scan(&u, &bufferCompressed) {
		u := u
		txnIDs = append(txnIDs, u)

		buffer, err = snappy.Decode(buffer[:0], bufferCompressed)
		if err != nil {
			return nil, err
		}

		current, err = decode(buffer)
		if err != nil {
			return nil, err
		}

		allData = append(allData, current...)
	}

	err = iter.Close()
	if err != nil {
		return nil, err
	}

	c.flushDone.L.Lock()

	for _, u := range txnIDs {
		c.flushedTxnIDs[u] = nil
	}

	c.flushDone.L.Unlock()

	return allData, err
}

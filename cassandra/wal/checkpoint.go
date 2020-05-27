package wal

import (
	"squirreldb/retry"
	"squirreldb/types"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/snappy"
)

// Abort undo a checkpoint which re-add all write to pending list
func (chk *Checkpoint) Abort() {
	chk.c.flushDone.L.Lock()
	defer chk.c.flushDone.L.Unlock()

	for id := range chk.txnIDs {
		chk.c.flushedTxnIDs[id] = nil
	}
}

// Purge delete all write stored in Cassandra WAL that are covered by this Checkpoint
func (chk *Checkpoint) Purge() {
	start := time.Now()
	allIDs := chk.extraTxnIDs

	for u := range chk.txnIDs {
		allIDs = append(allIDs, u)
	}

	if len(allIDs) > 0 {
		retry.Print(func() error {
			return chk.c.Session.Query(
				"DELETE FROM wal_log WHERE shard_id = ? AND txn_id IN ?",
				chk.c.ShardID,
				allIDs,
			).Exec()
		},
			retry.NewExponentialBackOff(30*time.Second),
			logger,
			"purge blob",
		)
	}

	requestsSecondsPurge.Observe(time.Since(start).Seconds())
}

// ReadOther search in Cassandra for write done by other SquirrelDB
func (chk *Checkpoint) ReadOther() ([]types.MetricData, error) {
	var (
		txnIDs []gocql.UUID
		u      gocql.UUID
	)

	start := time.Now()

	defer func() {
		requestsSecondsReadOther.Observe(time.Since(start).Seconds())
	}()

	chk.c.flushDone.L.Lock()

	iter := chk.c.Session.Query(
		"SELECT txn_id FROM wal_log WHERE shard_id = ?",
		chk.c.ShardID,
	).Iter()

	for iter.Scan(&u) {
		_, ok := chk.c.flushedTxnIDs[u]
		if ok {
			continue
		}

		_, ok = chk.txnIDs[u]
		if ok {
			continue
		}

		txnIDs = append(txnIDs, u)
	}

	chk.c.flushDone.L.Unlock()

	err := iter.Close()
	if err != nil {
		return nil, err
	}

	if len(txnIDs) == 0 {
		return nil, nil
	}

	writeByOther.Add(float64(len(txnIDs)))

	iter = chk.c.Session.Query(
		"SELECT txn_id, values FROM wal_log WHERE shard_id = ? AND txn_id IN ?",
		chk.c.ShardID,
		txnIDs,
	).PageSize(1).Iter()

	var (
		bufferCompressed []byte
		buffer           []byte
		current          []types.MetricData
		allData          []types.MetricData
	)

	txnIDs = txnIDs[:0]

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

	chk.extraTxnIDs = append(chk.extraTxnIDs, txnIDs...)

	return allData, err
}

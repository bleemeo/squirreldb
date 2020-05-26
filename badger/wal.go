package badger

import (
	"bytes"
	"encoding/gob"
	"squirreldb/dummy"
	"squirreldb/types"
	"strconv"

	"github.com/dgraph-io/badger/v2"
)

// Badger implement MetricWriter
type Badger struct {
	dummy.DiscardTSDB
	db  *badger.DB
	seq *badger.Sequence
}

// Init open the DB
func (b *Badger) Init() error {
	db, err := badger.Open(badger.DefaultOptions("./data"))
	if err != nil {
		return err
	}

	b.seq, err = db.GetSequence([]byte("wal-seq"), 100)
	if err != nil {
		db.Close()
		return err
	}

	b.db = db

	return nil
}

// Close the database. No write should happen after that
func (b *Badger) Close() {
	_ = b.seq.Release()
	b.seq = nil

	_ = b.db.Close()
	b.db = nil
}

func (b *Badger) Write(metrics []types.MetricData) error {
	var buffer bytes.Buffer

	enc := gob.NewEncoder(&buffer)

	err := enc.Encode(metrics)
	if err != nil {
		return err
	}

	return b.db.Update(func(txn *badger.Txn) error {
		n, err := b.seq.Next()
		if err != nil {
			return err
		}

		return txn.Set([]byte("wal-"+strconv.FormatInt(int64(n), 10)), buffer.Bytes())
	})
}

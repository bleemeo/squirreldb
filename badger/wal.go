package badger

import (
	"bytes"
	"encoding/gob"
	"log"
	"squirreldb/dummy"
	"squirreldb/types"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
)

// Badger implement MetricWriter
type Badger struct {
	dummy.DiscardTSDB

	mutex     sync.Mutex
	encoder   *gob.Encoder
	buffer    bytes.Buffer
	lastFlush time.Time
	db        *badger.DB
	seq       *badger.Sequence
}

// Init open the DB
func (b *Badger) Init() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

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
	b.lastFlush = time.Now()
	b.encoder = gob.NewEncoder(&b.buffer)

	return nil
}

// Close the database. No write should happen after that
func (b *Badger) Close() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.flush()

	_ = b.seq.Release()
	b.seq = nil

	_ = b.db.Close()
	b.db = nil
}

func (b *Badger) Write(metrics []types.MetricData) error {
	err := b.encoder.Encode(metrics)
	if err != nil {
		return err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if time.Since(b.lastFlush) > 10*time.Second || b.buffer.Len() > 10000000 {
		b.flush()
	}

	return nil
}

func (b *Badger) flush() {
	err := b.db.Update(func(txn *badger.Txn) error {
		n, err := b.seq.Next()
		if err != nil {
			return err
		}

		return txn.Set([]byte("wal-"+strconv.FormatInt(int64(n), 10)), b.buffer.Bytes())
	})

	if err != nil {
		log.Fatal(err)
	}

	b.buffer.Reset()
	b.lastFlush = time.Now()
}

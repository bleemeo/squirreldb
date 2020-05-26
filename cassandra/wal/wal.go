package wal

import (
	"bytes"
	"encoding/gob"
	"log"
	"squirreldb/dummy"
	"squirreldb/types"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

type Cassandra struct {
	dummy.DiscardTSDB
	Session    *gocql.Session
	SchemaLock sync.Locker

	mutex     sync.Mutex
	encoder   *gob.Encoder
	buffer    bytes.Buffer
	lastFlush time.Time
}

func (c *Cassandra) Init() error {
	c.SchemaLock.Lock()
	defer c.SchemaLock.Unlock()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS wal_log (
			insert_time timeuuid,
			values blob,
			PRIMARY KEY (insert_time)
		)`,
	}

	for _, query := range queries {
		if err := c.Session.Query(query).Consistency(gocql.All).Exec(); err != nil {
			return err
		}
	}

	c.encoder = gob.NewEncoder(&c.buffer)

	return nil
}

func (c *Cassandra) Close() {
	c.mutex.Lock()
	c.mutex.Unlock()

	c.flush()
}

func (c *Cassandra) Write(metrics []types.MetricData) error {
	err := c.encoder.Encode(metrics)
	if err != nil {
		return err
	}

	c.mutex.Lock()
	c.mutex.Unlock()

	if time.Since(c.lastFlush) > 10*time.Second || c.buffer.Len() > 10000000 {
		c.flush()
	}

	return nil
}

func (c *Cassandra) flush() {
	err := c.Session.Query(
		"INSERT INTO wal_log(insert_time, values) VALUES(now(), ?) USING TTL 3600",
		c.buffer.Bytes(),
	).Exec()
	if err != nil {
		log.Panic(err)
	}

	c.buffer.Reset()
	c.lastFlush = time.Now()
}

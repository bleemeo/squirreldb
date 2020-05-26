package wal

import (
	"bytes"
	"encoding/gob"
	"squirreldb/dummy"
	"squirreldb/types"
	"sync"

	"github.com/gocql/gocql"
)

type Cassandra struct {
	dummy.DiscardTSDB
	Session    *gocql.Session
	SchemaLock sync.Locker
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

	return nil
}

func (c *Cassandra) Write(metrics []types.MetricData) error {
	var buffer bytes.Buffer

	enc := gob.NewEncoder(&buffer)

	err := enc.Encode(metrics)
	if err != nil {
		return err
	}

	return c.Session.Query(
		"INSERT INTO wal_log(insert_time, values) VALUES(now(), ?) USING TTL 3600",
		buffer.Bytes(),
	).Exec()
}

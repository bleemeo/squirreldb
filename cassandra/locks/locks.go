package locks

import (
	"github.com/gocql/gocql"

	"strings"
)

const (
	tableName = "locks"
)

type CassandraLocks struct {
	session    *gocql.Session
	locksTable string
}

// New creates a new CassandraLocks object
func New(session *gocql.Session, keyspace string) (*CassandraLocks, error) {
	locksTable := keyspace + "." + tableName

	locksTableCreateQuery := locksTableCreateQuery(session, locksTable)

	if err := locksTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	locks := &CassandraLocks{
		session:    session,
		locksTable: locksTable,
	}

	return locks, nil
}

func (c *CassandraLocks) Lock(name string) (bool, error) {
	locksTableInsertLockQuery := c.locksTableInsertLockQuery(name)

	locksTableInsertLockQuery.SerialConsistency(gocql.LocalSerial)

	applied, err := locksTableInsertLockQuery.ScanCAS()

	return applied, err
}

func (c *CassandraLocks) Unlock(name string) error {
	locksTableDeleteLockQuery := c.locksTableDeleteLockQuery(name)

	err := locksTableDeleteLockQuery.Exec()

	return err
}

func (c *CassandraLocks) locksTableDeleteLockQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		DELETE FROM $LOCKS_TABLE
		WHERE name = ?
	`), name)

	return query
}

func (c *CassandraLocks) locksTableInsertLockQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $LOCKS_TABLE (name, lock_time)
		VALUES (?, now())
		IF NOT EXISTS
	`), name)

	return query
}

// Returns locks table create Query
func locksTableCreateQuery(session *gocql.Session, locksTable string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", locksTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $LOCKS_TABLE (
			name text,
			lock_time time,
			PRIMARY KEY (name)
		)
	`))

	return query
}

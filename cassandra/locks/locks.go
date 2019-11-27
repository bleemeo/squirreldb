package locks

import (
	"github.com/gocql/gocql"

	"strings"
	"time"
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

func (c *CassandraLocks) Delete(name string) error {
	locksTableDeleteLockQuery := c.locksTableDeleteLockQuery(name)

	err := locksTableDeleteLockQuery.Exec()

	return err
}

func (c *CassandraLocks) Write(name string, duration time.Duration) (bool, error) {
	locksTableInsertLockQuery := c.locksTableInsertLockQuery(name, duration)

	locksTableInsertLockQuery.SerialConsistency(gocql.LocalSerial)

	applied, err := locksTableInsertLockQuery.ScanCAS(nil, nil, nil)

	return applied, err
}

func (c *CassandraLocks) Update(name string, duration time.Duration) error {
	locksTableUpdateLockQuery := c.locksTableUpdateLockQuery(name)

	err := locksTableUpdateLockQuery.Exec()

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

func (c *CassandraLocks) locksTableInsertLockQuery(name string, duration time.Duration) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $LOCKS_TABLE (name, timestamp, duration)
		VALUES (?, toUnixTimestamp(now()), ?)
		IF NOT EXISTS
	`), name, duration)

	return query
}

func (c *CassandraLocks) locksTableUpdateLockQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		UPDATE $LOCKS_TABLE
		SET timestamp = toUnixTimestamp(now())
		WHERE name = ?
	`), name)

	return query
}

// Returns locks table create Query
func locksTableCreateQuery(session *gocql.Session, locksTable string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", locksTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $LOCKS_TABLE (
			name text,
			timestamp timestamp,
			duration duration,
			PRIMARY KEY (name)
		)
	`))

	return query
}

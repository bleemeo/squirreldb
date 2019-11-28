package locks

import (
	"github.com/gocql/gocql"
	"strings"
)

const tableName = "locks"

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

// Delete deletes a lock
func (c *CassandraLocks) Delete(name string) error {
	locksTableDeleteLockQuery := c.locksTableDeleteLockQuery(name)

	err := locksTableDeleteLockQuery.Exec()

	return err
}

// Write writes a lock
func (c *CassandraLocks) Write(name, instanceHostname, instanceUUID string, timeToLive int64) (bool, error) {
	locksTableInsertLockQuery := c.locksTableInsertLockQuery(name, instanceHostname, instanceUUID, timeToLive)

	locksTableInsertLockQuery.SerialConsistency(gocql.LocalSerial)

	applied, err := locksTableInsertLockQuery.ScanCAS(nil, nil, nil)

	return applied, err
}

// Update updates a lock
func (c *CassandraLocks) Update(name, instanceHostname, instanceUUID string, timeToLive int64) error {
	locksTableUpdateLockQuery := c.locksTableUpdateLockQuery(name, instanceHostname, instanceUUID, timeToLive)

	locksTableUpdateLockQuery.SerialConsistency(gocql.LocalSerial)

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

func (c *CassandraLocks) locksTableInsertLockQuery(name, instanceHostname, instanceUUID string, timeToLive int64) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $LOCKS_TABLE (name, instance_hostname, instance_uuid, timestamp)
		VALUES (?, ?, ?, toUnixTimestamp(now()))
		IF NOT EXISTS
		USING TTL ?
	`), name, instanceHostname, instanceUUID, timeToLive)

	return query
}

func (c *CassandraLocks) locksTableUpdateLockQuery(name, instanceHostname, instanceUUID string, timeToLive int64) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		UPDATE $LOCKS_TABLE USING TTL ?
		SET instance_hostname = ?, instance_uuid = ?, timestamp = toUnixTimestamp(now())
		WHERE name = ?
		IF EXISTS
	`), timeToLive, instanceHostname, instanceUUID, name)

	return query
}

// Returns locks table create Query
func locksTableCreateQuery(session *gocql.Session, locksTable string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", locksTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $LOCKS_TABLE (
			name text,
			instance_hostname text,
			instance_uuid uuid,
			timestamp timestamp,
			PRIMARY KEY (name)
		)
	`))

	return query
}

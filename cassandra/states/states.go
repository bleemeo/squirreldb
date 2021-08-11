package states

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/gocql/gocql"
)

type CassandraStates struct {
	session *gocql.Session
}

// New creates a new CassandraStates object.
func New(session *gocql.Session, lock sync.Locker) (*CassandraStates, error) {
	lock.Lock()
	defer lock.Unlock()

	statesTableCreateQuery := statesTableCreateQuery(session)
	statesTableCreateQuery.Consistency(gocql.All)

	if err := statesTableCreateQuery.Exec(); err != nil {
		return nil, fmt.Errorf("create tables: %w", err)
	}

	states := &CassandraStates{
		session: session,
	}

	return states, nil
}

// Read reads value of the state from the states table.
func (c *CassandraStates) Read(name string, value interface{}) (found bool, err error) {
	statesTableSelectStateQuery := c.statesTableSelectStateQuery(name)

	var valueString string

	err = statesTableSelectStateQuery.Scan(&valueString)

	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("read %s: %w", name, err)
	}

	switch v := value.(type) {
	case *float64:
		valueFloat64, _ := strconv.ParseFloat(valueString, 64)
		*v = valueFloat64
	case *int:
		valueInt, _ := strconv.Atoi(valueString)
		*v = valueInt
	case *int64:
		valueInt64, _ := strconv.ParseInt(valueString, 10, 64)
		*v = valueInt64
	case *string:
		*v = valueString
	default:
		return false, fmt.Errorf("unknown type")
	}

	return true, nil
}

// Write updates the state in the states table.
func (c *CassandraStates) Write(name string, value interface{}) error {
	valueString := fmt.Sprint(value)
	statesTableUpdateStateQuery := c.statesTableInsertStateQuery(name, valueString)

	if err := statesTableUpdateStateQuery.Exec(); err != nil {
		return fmt.Errorf("update Cassandra: %w", err)
	}

	return nil
}

// Returns states table insert state Query.
func (c *CassandraStates) statesTableInsertStateQuery(name string, value string) *gocql.Query {
	query := c.session.Query(`
		INSERT INTO states (name, value)
		VALUES (?, ?)
	`, name, value)

	return query
}

// Returns states table select state Query.
func (c *CassandraStates) statesTableSelectStateQuery(name string) *gocql.Query {
	query := c.session.Query(`
		SELECT value FROM states
		WHERE name = ?
	`, name)

	return query
}

// Returns states table create Query.
func statesTableCreateQuery(session *gocql.Session) *gocql.Query {
	query := session.Query(`
		CREATE TABLE IF NOT EXISTS states (
			name text,
			value text,
			PRIMARY KEY (name)
		)
		WITH memtable_flush_period_in_ms = 300000
	`)

	return query
}

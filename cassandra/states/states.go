package states

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"squirreldb/cassandra/connection"
	"strconv"
	"sync"

	"github.com/gocql/gocql"
)

type CassandraStates struct {
	connection *connection.Connection
}

// New creates a new CassandraStates object.
func New(ctx context.Context, connection *connection.Connection, lock sync.Locker) (*CassandraStates, error) {
	lock.Lock()
	defer lock.Unlock()

	if err := statesTableCreate(ctx, connection); err != nil {
		return nil, fmt.Errorf("create tables: %w", err)
	}

	states := &CassandraStates{
		connection: connection,
	}

	return states, nil
}

// Read reads value of the state from the states table.
func (c *CassandraStates) Read(ctx context.Context, name string, value interface{}) (found bool, err error) {
	valueString, err := c.statesTableSelectState(ctx, name)

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
		err := json.Unmarshal([]byte(valueString), value)
		if err != nil {
			return false, fmt.Errorf("failed to unmarshal value: %w", err)
		}

		return true, nil
	}

	return true, nil
}

// Write updates the state in the states table.
func (c *CassandraStates) Write(ctx context.Context, name string, value interface{}) error {
	var valueString string

	switch value.(type) {
	case float64, int, int64, string:
		valueString = fmt.Sprint(value)
	default:
		marshalled, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}

		valueString = string(marshalled)
	}

	if err := c.statesTableInsertState(ctx, name, valueString); err != nil {
		return fmt.Errorf("update Cassandra: %w", err)
	}

	return nil
}

// Returns states table insert state Query.
func (c *CassandraStates) statesTableInsertState(ctx context.Context, name string, value string) error {
	session, err := c.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		INSERT INTO states (name, value)
		VALUES (?, ?)`,
		name, value,
	).WithContext(ctx)

	return query.Exec()
}

// Returns states table select state Query.
func (c *CassandraStates) statesTableSelectState(ctx context.Context, name string) (string, error) {
	var valueString string

	session, err := c.connection.Session()
	if err != nil {
		return "", err
	}

	defer session.Close()

	query := session.Query(`
		SELECT value FROM states
		WHERE name = ?`,
		name,
	).WithContext(ctx)

	err = query.Scan(&valueString)

	return valueString, err
}

func statesTableCreate(ctx context.Context, connection *connection.Connection) error {
	session, err := connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		CREATE TABLE IF NOT EXISTS states (
			name text,
			value text,
			PRIMARY KEY (name)
		)
		WITH memtable_flush_period_in_ms = 300000`,
	).Consistency(gocql.All).WithContext(ctx)

	return query.Exec()
}

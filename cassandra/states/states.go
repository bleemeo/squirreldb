package states

import (
	"fmt"
	"github.com/gocql/gocql"
	"strconv"
	"strings"
)

const (
	table = "states"
)

type CassandraStates struct {
	session     *gocql.Session
	statesTable string
}

// New creates a new CassandraStates object
func New(session *gocql.Session, keyspace string) (*CassandraStates, error) {
	statesTable := keyspace + "." + table

	createStatesTable := createStatesTableQuery(session, statesTable)

	if err := createStatesTable.Exec(); err != nil {
		session.Close()
		return nil, err
	}

	states := &CassandraStates{
		session:     session,
		statesTable: statesTable,
	}

	return states, nil
}

// Read assigns the value from the specified state
func (c *CassandraStates) Read(name string, value interface{}) error {
	valueString, err := c.readStatesTable(name)

	if err != nil {
		return err
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
		return fmt.Errorf("unknown type")
	}

	return nil
}

// Write writes the specified state
func (c *CassandraStates) Write(name string, value interface{}) error {
	valueString := fmt.Sprint(value)

	if err := c.writeStatesTable(name, valueString); err != nil {
		return err
	}

	return nil
}

// Reads the value of the specified state from the states table
func (c *CassandraStates) readStatesTable(name string) (string, error) {
	replacer := strings.NewReplacer("$STATES_TABLE", c.statesTable)
	iterator := c.session.Query(replacer.Replace(`
		SELECT value FROM $STATES_TABLE
		WHERE name = ?
	`), name)

	var value string

	if err := iterator.Scan(&value); err != nil {
		return "", err
	}

	return value, nil
}

// Writes the specified state to the states table
func (c *CassandraStates) writeStatesTable(name string, value string) error {
	replacer := strings.NewReplacer("$STATES_TABLE", c.statesTable)
	update := c.session.Query(replacer.Replace(`
		UPDATE $STATES_TABLE
		SET value = ?
		WHERE name = ?
	`), value, name)

	if err := update.Exec(); err != nil {
		return err
	}

	return nil
}

// Returns states table create query
func createStatesTableQuery(session *gocql.Session, statesTable string) *gocql.Query {
	replacer := strings.NewReplacer("$STATES_TABLE", statesTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $STATES_TABLE (
			name text,
			value text,
			PRIMARY KEY (name)
		)
	`))

	return query
}

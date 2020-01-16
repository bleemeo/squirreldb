package states

import (
	"github.com/gocql/gocql"

	"fmt"
	"strconv"
	"strings"
)

const tableName = "states"

type CassandraStates struct {
	session     *gocql.Session
	statesTable string
}

// New creates a new CassandraStates object
func New(session *gocql.Session, keyspace string) (*CassandraStates, error) {
	statesTable := keyspace + "." + tableName

	statesTableCreateQuery := statesTableCreateQuery(session, statesTable)

	if err := statesTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	states := &CassandraStates{
		session:     session,
		statesTable: statesTable,
	}

	return states, nil
}

// Read reads value of the state from the states table
func (c *CassandraStates) Read(name string, value interface{}) (found bool, err error) {
	statesTableSelectStateQuery := c.statesTableSelectStateQuery(name)

	var valueString string

	err = statesTableSelectStateQuery.Scan(&valueString)

	if err == gocql.ErrNotFound {
		return false, nil
	}

	if err != nil {
		return false, err
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

// Update updates the state in the states table
func (c *CassandraStates) Write(name string, value interface{}) error {
	valueString := fmt.Sprint(value)
	statesTableUpdateStateQuery := c.statesTableInsertStateQuery(name, valueString)

	err := statesTableUpdateStateQuery.Exec()

	return err
}

// Returns states table insert state Query
func (c *CassandraStates) statesTableInsertStateQuery(name string, value string) *gocql.Query {
	replacer := strings.NewReplacer("$STATES_TABLE", c.statesTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $STATES_TABLE (name, value)
		VALUES (?, ?)
	`), name, value)

	return query
}

// Returns states table select state Query
func (c *CassandraStates) statesTableSelectStateQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$STATES_TABLE", c.statesTable)
	query := c.session.Query(replacer.Replace(`
		SELECT value FROM $STATES_TABLE
		WHERE name = ?
	`), name)

	return query
}

// Returns states table create Query
func statesTableCreateQuery(session *gocql.Session, statesTable string) *gocql.Query {
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

package cassandra

import (
	"fmt"
	"strconv"
	"strings"
)

func (c *Cassandra) readState(name string, value interface{}) error {
	valueString, err := c.readStateQuery(name)

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
	}

	return nil
}

func (c *Cassandra) writeState(name string, value interface{}) error {
	valueString := fmt.Sprint(value)

	if err := c.writeStateQuery(name, valueString); err != nil {
		return err
	}

	return nil
}

// Reads a state value from the states table
func (c *Cassandra) readStateQuery(name string) (string, error) {
	replacer := strings.NewReplacer("$STATES_TABLE", c.options.statesTable)
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
func (c *Cassandra) writeStateQuery(name string, value string) error {
	replacer := strings.NewReplacer("$STATES_TABLE", c.options.statesTable)
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

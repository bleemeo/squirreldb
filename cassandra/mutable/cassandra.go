package mutable

import (
	"fmt"

	"github.com/gocql/gocql"
)

type cassandra struct {
	session *gocql.Session
}

type Store interface {
	AssociatedNames(tenant string) (map[string]string, error)
	AssociatedValues(tenant, name string) (map[string][]string, error)
	DeleteAssociatedName(tenant, name string) error
	DeleteAssociatedValues(label Label) error
	SetAssociatedName(label LabelWithName) error
	SetAssociatedValues(label LabelWithValues) error
}

// NewCassandraStore returns a new Cassandra mutable label store.
func NewCassandraStore(session *gocql.Session) (Store, error) {
	cp := &cassandra{
		session: session,
	}

	if err := cp.createTables(); err != nil {
		return nil, err
	}

	return cp, nil
}

// createTables creates the mutable labels table if it doesn't exist.
func (cp *cassandra) createTables() error {
	queryMutableLabelValues := cp.session.Query(`
		CREATE TABLE IF NOT EXISTS mutable_label_values (
			tenant text,
			name text,
			value text,
			associated_values frozen<list<text>>,
			PRIMARY KEY ((tenant, name), value)
		)
	`)

	queryMutableLabelNames := cp.session.Query(`
		CREATE TABLE IF NOT EXISTS mutable_label_names (
			tenant text,
			name text,
			associated_name text,
			PRIMARY KEY (tenant, name)
		)
	`)

	for _, query := range []*gocql.Query{queryMutableLabelValues, queryMutableLabelNames} {
		query.Consistency(gocql.All)

		if err := query.Exec(); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
	}

	return nil
}

// AssociatedNames return the associated names map[mutable label name] -> non mutable label name.
func (cp *cassandra) AssociatedNames(tenant string) (map[string]string, error) {
	iter := cp.session.Query(`
		SELECT name, associated_name FROM mutable_label_names
		WHERE tenant = ?
	`, tenant).Iter()

	var (
		associatedNames = make(map[string]string)
		name            string
		associatedName  string
	)

	for iter.Scan(&name, &associatedName) {
		associatedNames[name] = associatedName
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select labels: %w", err)
	}

	return associatedNames, nil
}

// AssociatedValues returns the associated values for a mutable label name and tenant.
func (cp *cassandra) AssociatedValues(tenant, name string) (map[string][]string, error) {
	iter := cp.session.Query(`
		SELECT value, associated_values FROM mutable_label_values
		WHERE tenant = ? AND name = ?
	`, tenant, name).Iter()

	var (
		values           = make(map[string][]string)
		value            string
		associatedValues []string
	)

	for iter.Scan(&value, &associatedValues) {
		values[value] = associatedValues
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select values: %w", err)
	}

	return values, nil
}

// SetAssociatedValues sets the non mutable label values associated to a mutable label.
func (cp *cassandra) SetAssociatedValues(label LabelWithValues) error {
	query := cp.session.Query(`
		INSERT INTO mutable_label_values (tenant, name, value, associated_values)
		VALUES (?, ?, ?, ?)
	`, label.Tenant, label.Name, label.Value, label.AssociatedValues)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated values: %w", err)
	}

	return nil
}

// DeleteAssociatedValues deletes the non mutable label values associated to a mutable label.
func (cp *cassandra) DeleteAssociatedValues(label Label) error {
	query := cp.session.Query(`
		DELETE FROM mutable_label_values
		WHERE tenant = ? AND name = ? AND value = ?
	`, label.Tenant, label.Name, label.Value)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("delete associated values: %w", err)
	}

	return nil
}

// SetAssociatedName sets the non mutable label name associated to a mutable label name.
func (cp *cassandra) SetAssociatedName(label LabelWithName) error {
	query := cp.session.Query(`
		INSERT INTO mutable_label_names (tenant, name, associated_name)
		VALUES (?, ?, ?)
	`, label.Tenant, label.Name, label.AssociatedName)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated name: %w", err)
	}

	return nil
}

// DeleteAssociatedName deletes a mutable label name.
func (cp *cassandra) DeleteAssociatedName(tenant, name string) error {
	queryDeleteLabelNames := cp.session.Query(`
		DELETE FROM mutable_label_names
		WHERE tenant = ? AND name = ?
	`, tenant, name)

	if err := queryDeleteLabelNames.Exec(); err != nil {
		return fmt.Errorf("delete label name from mutable_label_names: %w", err)
	}

	queryDeleteLabelValues := cp.session.Query(`
		DELETE FROM squirreldb.mutable_label_values
		WHERE tenant = ? AND name = ?
	`, tenant, name)

	if err := queryDeleteLabelValues.Exec(); err != nil {
		return fmt.Errorf("delete label name from mutable_label_values: %w", err)
	}

	return nil
}

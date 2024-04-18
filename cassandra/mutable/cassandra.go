package mutable

import (
	"context"
	"fmt"

	"github.com/bleemeo/squirreldb/cassandra/connection"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

type Options struct {
	Connection *connection.Connection
	ReadOnly   bool
	Logger     zerolog.Logger
}

type cassandra struct {
	options Options
}

type Store interface {
	AssociatedNames(ctx context.Context, tenant string) (map[string]string, error)
	AssociatedValues(ctx context.Context, tenant, name string) (map[string][]string, error)
	DeleteAssociatedName(ctx context.Context, tenant, name string) error
	DeleteAssociatedValues(ctx context.Context, label Label) error
	SetAssociatedName(ctx context.Context, label LabelWithName) error
	SetAssociatedValues(ctx context.Context, label LabelWithValues) error
	AllMutableLabels(ctx context.Context) ([]LabelWithName, error)
}

// NewCassandraStore returns a new Cassandra mutable label store.
func NewCassandraStore(ctx context.Context, options Options) (Store, error) {
	cp := &cassandra{
		options: options,
	}

	if err := cp.createTables(ctx); err != nil {
		return nil, err
	}

	return cp, nil
}

// createTables creates the mutable labels table if it doesn't exist.
func (cp *cassandra) createTables(ctx context.Context) error {
	if cp.options.ReadOnly {
		cp.options.Logger.Debug().Msg("Read-only mode is activated. Not trying to create tables and assuming they exist")

		return nil
	}

	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	queryMutableLabelValues := session.Query(`
		CREATE TABLE IF NOT EXISTS mutable_label_values (
			tenant text,
			name text,
			value text,
			associated_values frozen<list<text>>,
			PRIMARY KEY ((tenant, name), value)
		)`,
	).WithContext(ctx)

	queryMutableLabelNames := session.Query(`
		CREATE TABLE IF NOT EXISTS mutable_label_names (
			tenant text,
			name text,
			associated_name text,
			PRIMARY KEY (tenant, name)
		)`,
	).WithContext(ctx)

	for _, query := range []*gocql.Query{queryMutableLabelValues, queryMutableLabelNames} {
		query.Consistency(gocql.All)

		if err := query.Exec(); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
	}

	return nil
}

// AssociatedNames return the associated names map[mutable label name] -> non mutable label name.
func (cp *cassandra) AssociatedNames(ctx context.Context, tenant string) (map[string]string, error) {
	session, err := cp.options.Connection.SessionReadOnly()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	iter := session.Query(`
		SELECT name, associated_name FROM mutable_label_names
		WHERE tenant = ?`,
		tenant,
	).WithContext(ctx).Iter()

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
func (cp *cassandra) AssociatedValues(ctx context.Context, tenant, name string) (map[string][]string, error) {
	session, err := cp.options.Connection.SessionReadOnly()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	iter := session.Query(`
		SELECT value, associated_values FROM mutable_label_values
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	).WithContext(ctx).Iter()

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
func (cp *cassandra) SetAssociatedValues(ctx context.Context, label LabelWithValues) error {
	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		INSERT INTO mutable_label_values (tenant, name, value, associated_values)
		VALUES (?, ?, ?, ?)`,
		label.Tenant, label.Name, label.Value, label.AssociatedValues,
	).WithContext(ctx)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated values: %w", err)
	}

	return nil
}

// DeleteAssociatedValues deletes the non mutable label values associated to a mutable label.
func (cp *cassandra) DeleteAssociatedValues(ctx context.Context, label Label) error {
	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		DELETE FROM mutable_label_values
		WHERE tenant = ? AND name = ? AND value = ?`,
		label.Tenant, label.Name, label.Value,
	).WithContext(ctx)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("delete associated values: %w", err)
	}

	return nil
}

// SetAssociatedName sets the non mutable label name associated to a mutable label name.
func (cp *cassandra) SetAssociatedName(ctx context.Context, label LabelWithName) error {
	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		INSERT INTO mutable_label_names (tenant, name, associated_name)
		VALUES (?, ?, ?)`,
		label.Tenant, label.Name, label.AssociatedName,
	).WithContext(ctx)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated name: %w", err)
	}

	return nil
}

// DeleteAssociatedName deletes a mutable label name.
func (cp *cassandra) DeleteAssociatedName(ctx context.Context, tenant, name string) error {
	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	queryDeleteLabelNames := session.Query(`
		DELETE FROM mutable_label_names
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	).WithContext(ctx)

	if err := queryDeleteLabelNames.Exec(); err != nil {
		return fmt.Errorf("delete label name from mutable_label_names: %w", err)
	}

	queryDeleteLabelValues := session.Query(`
		DELETE FROM squirreldb.mutable_label_values
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	).WithContext(ctx)

	if err := queryDeleteLabelValues.Exec(); err != nil {
		return fmt.Errorf("delete label name from mutable_label_values: %w", err)
	}

	return nil
}

// AllMutableLabels return the list of all known mutable labels from all tenants.
// Currently this function is not optimized (it does a Cassandra full scan) and should be avoided if possible.
func (cp *cassandra) AllMutableLabels(ctx context.Context) ([]LabelWithName, error) {
	session, err := cp.options.Connection.SessionReadOnly()
	if err != nil {
		return nil, err
	}

	result := make([]LabelWithName, 0)
	iter := session.Query("SELECT tenant, name, associated_name FROM mutable_label_names").WithContext(ctx).Iter()

	var currentLabel LabelWithName

	for iter.Scan(&currentLabel.Tenant, &currentLabel.Name, &currentLabel.AssociatedName) {
		result = append(result, currentLabel)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select tenants: %w", err)
	}

	return result, ctx.Err()
}

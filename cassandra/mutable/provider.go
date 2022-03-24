package mutable

import (
	"errors"
	"fmt"

	"github.com/gocql/gocql"
)

var (
	errTooManyRows  = errors.New("too many rows")
	errNoRowMatched = errors.New("no row matched query")
)

// LabelProvider allows to get non mutable labels from a mutable label.
type LabelProvider interface {
	Get(tenant, name, value string) (NonMutableLabels, error)
	AllValues(tenant, name string) ([]string, error)
	IsMutableLabel(name string) (bool, error)
	IsTenantLabel(name string) bool
}

// CassandraProvider is a labelProvider thats gets the labels from Cassandra.
type CassandraProvider struct {
	session *gocql.Session
}

// LabelWithValues represents a mutable label with its associated non mutable labels value.
type LabelWithValues struct {
	Tenant           string   `json:"tenant"`
	Name             string   `json:"name"`
	Value            string   `json:"value"`
	AssociatedValues []string `json:"associated_values"`
}

// LabelWithName represents a mutable label name with its associated non mutable label name.
type LabelWithName struct {
	Name           string `json:"name"`
	AssociatedName string `json:"associated_name"`
}

// NonMutableLabels represents a list of labels with a single name.
type NonMutableLabels struct {
	Name   string
	Values []string
}

// NewCassandraProvider returns a new Cassandra mutable label provider.
func NewCassandraProvider(session *gocql.Session) (*CassandraProvider, error) {
	cp := &CassandraProvider{
		session: session,
	}

	if err := cp.createTables(); err != nil {
		return nil, err
	}

	return cp, nil
}

// createTables creates the mutable labels table if it doesn't exist.
func (cp *CassandraProvider) createTables() error {
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
			name text PRIMARY KEY,
			associated_name text,
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

// Get returns the non mutable labels corresponding to a mutable label name and value.
func (cp *CassandraProvider) Get(tenant, name, value string) (NonMutableLabels, error) {
	associatedName, err := cp.selectAssociatedName(name)
	if err != nil {
		return NonMutableLabels{}, err
	}

	associatedValues, err := cp.selectAssociatedValues(tenant, name, value)
	if err != nil {
		return NonMutableLabels{}, err
	}

	lbls := NonMutableLabels{
		Name:   associatedName,
		Values: associatedValues,
	}

	return lbls, nil
}

// selectAssociatedValues selects the non mutable label values associated to a mutable label name and value.
func (cp *CassandraProvider) selectAssociatedValues(tenant, name, value string) ([]string, error) {
	iter := cp.session.Query(`
		SELECT associated_values FROM mutable_label_values 
		WHERE tenant = ? AND name = ? AND value = ?
	`, tenant, name, value).Iter()

	var associatedValues []string

	if !iter.Scan(&associatedValues) {
		errMsg := "%w: no result for tenant=%s, name=%s, value=%s"

		return nil, fmt.Errorf(errMsg, errNoRowMatched, tenant, name, value)
	}

	if iter.Scan(nil) {
		errMsg := "%w: expected a single row for tenant=%s, name=%s, value=%s"

		return nil, fmt.Errorf(errMsg, errTooManyRows, tenant, name, value)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select labels: %w", err)
	}

	return associatedValues, nil
}

// selectAssociatedName selects the non mutable label name associated to a mutable label name and value.
func (cp *CassandraProvider) selectAssociatedName(name string) (string, error) {
	iter := cp.session.Query(`
		SELECT associated_name FROM mutable_label_names
		WHERE name = ?
	`, name).Iter()

	var associatedName string

	if !iter.Scan(&associatedName) {
		return "", fmt.Errorf("%w: no result for name=%s", errNoRowMatched, name)
	}

	if iter.Scan(nil) {
		return "", fmt.Errorf("%w: expected a single row for name=%s", errTooManyRows, name)
	}

	if err := iter.Close(); err != nil {
		return "", fmt.Errorf("select labels: %w", err)
	}

	return associatedName, nil
}

// AllValues returns all possible mutable label values.
func (cp *CassandraProvider) AllValues(tenant, name string) ([]string, error) {
	return cp.selectValues(tenant, name)
}

// selectValues selects all possible values for a mutable label name.
func (cp *CassandraProvider) selectValues(tenant, name string) ([]string, error) {
	iter := cp.session.Query(`
		SELECT value FROM mutable_label_values
		WHERE tenant = ? AND name = ?
	`, tenant, name).Iter()

	var (
		values []string
		value  string
	)

	for iter.Scan(&value) {
		values = append(values, value)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select values: %w", err)
	}

	return values, nil
}

// IsMutableLabel returns whether the label is mutable.
func (cp *CassandraProvider) IsMutableLabel(name string) (bool, error) {
	mutableLabelNames, err := cp.selectMutableLabelNames()
	if err != nil {
		return false, err
	}

	for _, mutName := range mutableLabelNames {
		if name == mutName {
			return true, nil
		}
	}

	return false, nil
}

func (cp *CassandraProvider) selectMutableLabelNames() ([]string, error) {
	iter := cp.session.Query(`
		SELECT name FROM mutable_label_names
	`).Iter()

	var (
		names []string
		name  string
	)

	for iter.Scan(&name) {
		names = append(names, name)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select names: %w", err)
	}

	return names, nil
}

// IsTenantLabel returns whether this label identifies the tenant.
func (cp *CassandraProvider) IsTenantLabel(name string) bool {
	// TODO: Don't hardcode this value.
	return name == "__account_id"
}

// WriteLabelValues writes the label values to Cassandra.
func (cp *CassandraProvider) WriteLabelValues(lbls []LabelWithValues) error {
	for _, label := range lbls {
		if err := cp.insertAssociatedValues(label); err != nil {
			return err
		}
	}

	return nil
}

// insertAssociatedValues inserts or modifies the non mutable label values associated to a mutable label.
func (cp *CassandraProvider) insertAssociatedValues(label LabelWithValues) error {
	query := cp.session.Query(`
		INSERT INTO mutable_label_values (tenant, name, value, associated_values)
		VALUES (?, ?, ?, ?)
	`, label.Tenant, label.Name, label.Value, label.AssociatedValues)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert labels: %w", err)
	}

	return nil
}

// WriteLabelNames writes the label names to Cassandra.
func (cp *CassandraProvider) WriteLabelNames(lbls []LabelWithName) error {
	for _, label := range lbls {
		if err := cp.insertAssociatedName(label); err != nil {
			return err
		}
	}

	return nil
}

// insertAssociatedName inserts or modifies the non mutable label name associated to a mutable label name.
func (cp *CassandraProvider) insertAssociatedName(label LabelWithName) error {
	query := cp.session.Query(`
		INSERT INTO mutable_label_names (name, associated_name)
		VALUES (?, ?)
	`, label.Name, label.AssociatedName)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert labels: %w", err)
	}

	return nil
}

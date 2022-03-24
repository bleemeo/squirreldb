package mutable

import (
	"errors"
	"fmt"

	"github.com/gocql/gocql"
)

var errTooManyRows = errors.New("too many rows")

// LabelProvider allows to get non mutable labels from a mutable label.
type LabelProvider interface {
	Get(tenant, name, value string) (LabelValues, error)
	AllValues(tenant, name string) ([]string, error)
	IsMutableLabel(name string) bool
	IsTenantLabel(name string) bool
}

// CassandraProvider is a labelProvider thats gets the labels from Cassandra.
type CassandraProvider struct {
	session *gocql.Session
}

// Label represents a mutable label with its associated non mutable labels.
type Label struct {
	Tenant string
	Name   string
	Value  string
	Labels LabelValues
}

// LabelValues represents a list of labels with a single name.
type LabelValues struct {
	Name   string
	Values []string
}

// NewCassandraProvider returns a new Cassandra mutable label provider.
func NewCassandraProvider(session *gocql.Session) (*CassandraProvider, error) {
	cp := &CassandraProvider{
		session: session,
	}

	if err := cp.createTable(); err != nil {
		return nil, err
	}

	return cp, nil
}

// createTable creates the mutable labels table if it doesn't exist.
func (cp *CassandraProvider) createTable() error {
	query := cp.session.Query(`
		CREATE TABLE IF NOT EXISTS mutable_labels (
			tenant text,
			name text,
			value text,
			labels tuple<text, frozen<list<text>>>,
			PRIMARY KEY ((tenant, name), value)
		)
	`)

	query.Consistency(gocql.All)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	return nil
}

// Get returns the non mutable labels corresponding to a mutable label name and value.
func (cp *CassandraProvider) Get(tenant, name, value string) (LabelValues, error) {
	return cp.selectLabels(tenant, name, value)
}

// selectLabels selects the non mutable labels associated to a mutable label name and value.
func (cp *CassandraProvider) selectLabels(tenant, name, value string) (LabelValues, error) {
	iter := cp.session.Query(`
		SELECT labels FROM mutable_labels 
		WHERE tenant = ? AND name = ? AND value = ?
	`, tenant, name, value).Iter()

	var lbls LabelValues

	iter.Scan(&lbls)

	if iter.Scan(nil) {
		errMsg := "%w: expected a single row for tenant=%s, name=%s, value=%s"

		return LabelValues{}, fmt.Errorf(errMsg, errTooManyRows, tenant, name, value)
	}

	if err := iter.Close(); err != nil {
		return LabelValues{}, fmt.Errorf("select labels: %w", err)
	}

	return lbls, nil
}

// AllValues returns all possible mutable label values.
func (cp *CassandraProvider) AllValues(tenant, name string) ([]string, error) {
	return cp.selectValues(tenant, name)
}

// selectValues selects all possible values for a mutable label name.
func (cp *CassandraProvider) selectValues(tenant, name string) ([]string, error) {
	iter := cp.session.Query(`
		SELECT value FROM mutable_labels 
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

// isMutableLabel returns whether the label is mutable.
func (cp *CassandraProvider) IsMutableLabel(name string) bool {
	// TODO: We should not hardcode any value here and retrieve these labels from cassandra.
	return name == "group"
}

// IsTenantLabel returns whether this label identifies the tenant.
func (cp *CassandraProvider) IsTenantLabel(name string) bool {
	// TODO: Don't hardcode this value.
	return name == "__account_id"
}

func (cp *CassandraProvider) WriteLabels(mutLabels []Label) error {
	for _, mutLabel := range mutLabels {
		if err := cp.insertMutableLabel(mutLabel); err != nil {
			return err
		}
	}

	return nil
}

// insertMutableLabel inserts or modifies the non mutable labels associated to a mutable label name and value.
func (cp *CassandraProvider) insertMutableLabel(mutLabel Label) error {
	query := cp.session.Query(`
		INSERT INTO mutable_labels (tenant, name, value, labels)
		VALUES (?, ?, ?, ?)
	`, mutLabel.Tenant, mutLabel.Name, mutLabel.Value, mutLabel.Labels)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert labels: %w", err)
	}

	return nil
}

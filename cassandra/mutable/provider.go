package mutable

import (
	"errors"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/prometheus/prometheus/model/labels"
)

var errTooManyRows = errors.New("too many rows")

// LabelProvider allows to get non mutable labels from a mutable label.
type LabelProvider interface {
	Get(tenant, name, value string) ([]labels.Label, error)
	AllValues(tenant, name string) ([]string, error)
	IsMutableLabel(name string) bool
	IsTenantLabel(name string) bool
}

// cassandraProvider is a labelProvider thats gets the labels from Cassandra.
type cassandraProvider struct {
	session *gocql.Session
}

// NewCassandraProvider returns a new Cassandra mutable label provider.
func NewCassandraProvider(session *gocql.Session) (LabelProvider, error) {
	cp := &cassandraProvider{
		session: session,
	}

	if err := cp.createTable(); err != nil {
		return nil, err
	}

	return cp, nil
}

// createTable creates the mutable labels table if it doesn't exist.
func (cp *cassandraProvider) createTable() error {
	query := cp.session.Query(`
		CREATE TABLE IF NOT EXISTS mutable_labels (
			tenant text,
			name text,
			value text,
			labels frozen<list<tuple<text, text>>>,
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
func (cp *cassandraProvider) Get(tenant, name, value string) ([]labels.Label, error) {
	return cp.selectLabels(tenant, name, value)
}

// selectLbales selects the non mutable labels associated to a mutable label name and value.
func (cp *cassandraProvider) selectLabels(tenant, name, value string) ([]labels.Label, error) {
	iter := cp.session.Query(`
		SELECT labels FROM mutable_labels 
		WHERE tenant = ? AND name = ? AND value = ?
	`, tenant, name, value).Iter()

	var lbls []labels.Label

	iter.Scan(&lbls)

	if iter.Scan(nil) {
		errMsg := "%w: expected a single row for tenant=%s, name=%s, value=%s"

		return nil, fmt.Errorf(errMsg, errTooManyRows, tenant, name, value)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select labels: %w", err)
	}

	return lbls, nil
}

// AllValues returns all possible mutable label values.
func (cp *cassandraProvider) AllValues(tenant, name string) ([]string, error) {
	return cp.selectValues(tenant, name)
}

// selectValues selects all possible values for a mutable label name.
func (cp *cassandraProvider) selectValues(tenant, name string) ([]string, error) {
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
func (cp *cassandraProvider) IsMutableLabel(name string) bool {
	// TODO: We should not hardcode any value here and retrieve these labels from cassandra.
	return name == "group"
}

// IsTenantLabel returns whether this label identifies the tenant.
func (cp *cassandraProvider) IsTenantLabel(name string) bool {
	// TODO: Don't hardcode this value.
	return name == "__account_id"
}

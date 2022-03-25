package mutable

import (
	"context"
	"errors"
	"fmt"
	"squirreldb/types"

	"github.com/gocql/gocql"
)

var errNoRowMatched = errors.New("no row matched query")

// LabelProvider allows to get non mutable labels from a mutable label.
type LabelProvider interface {
	Get(tenant, name, value string) (NonMutableLabels, error)
	AllValues(tenant, name string) ([]string, error)
	IsMutableLabel(name string) (bool, error)
}

// CassandraProvider is a labelProvider thats gets the labels from Cassandra.
type CassandraProvider struct {
	session *gocql.Session
	cache   *cache
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

// Label represents a mutable label.
type Label struct {
	Tenant string `json:"tenant"`
	Name   string `json:"name"`
	Value  string `json:"value"`
}

// NonMutableLabels represents a list of labels with a single name.
type NonMutableLabels struct {
	Name   string
	Values []string
}

// NewCassandraProvider returns a new Cassandra mutable label provider.
func NewCassandraProvider(session *gocql.Session, cluster types.Cluster) (*CassandraProvider, error) {
	cp := &CassandraProvider{
		session: session,
		cache:   newCache(cluster),
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
	associatedName, err := cp.associatedName(name)
	if err != nil {
		return NonMutableLabels{}, err
	}

	associatedValues, err := cp.associatedValuesByNameAndValue(tenant, name, value)
	if err != nil {
		return NonMutableLabels{}, err
	}

	lbls := NonMutableLabels{
		Name:   associatedName,
		Values: associatedValues,
	}

	return lbls, nil
}

// associatedName returns the non mutable label name associated to a mutable label name and value.
func (cp *CassandraProvider) associatedName(name string) (string, error) {
	associatedName, found := cp.cache.AssociatedName(name)
	if found {
		return associatedName, nil
	}

	// List all the associated names once to set it in cache as the table only has a few rows.
	associatedNames, err := cp.selectAssociatedNames()
	if err != nil {
		return "", err
	}

	associatedName, ok := associatedNames[name]
	if !ok {
		return "", fmt.Errorf("%w: no result for name=%s", errNoRowMatched, name)
	}

	return associatedName, nil
}

// selectAssociatedNames returns a map of the non mutable names index by the mutable name.
func (cp *CassandraProvider) selectAssociatedNames() (map[string]string, error) {
	iter := cp.session.Query("SELECT name, associated_name FROM mutable_label_names").Iter()

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

	cp.cache.SetAllAssociatedNames(associatedNames)

	return associatedNames, nil
}

func (cp *CassandraProvider) associatedValuesByNameAndValue(tenant, name, value string) ([]string, error) {
	associatedValues, found := cp.cache.AssociatedValues(tenant, name, value)
	if found {
		return associatedValues, nil
	}

	values, err := cp.selectAssociatedValues(tenant, name)
	if err != nil {
		return nil, err
	}

	associatedValues, found = values[value]
	if !found {
		return nil, fmt.Errorf("%w: no result for tenant=%s, %s=%s", errNoRowMatched, tenant, name, value)
	}

	return associatedValues, nil
}

// AllValues returns all possible mutable label values.
func (cp *CassandraProvider) AllValues(tenant, name string) ([]string, error) {
	values, found := cp.cache.Values(tenant, name)
	if found {
		return values, nil
	}

	associatedValues, err := cp.selectAssociatedValues(tenant, name)
	if err != nil {
		return nil, err
	}

	for value := range associatedValues {
		values = append(values, value)
	}

	return values, nil
}

// selectAssociatedValues selects all possible values for a mutable label name.
func (cp *CassandraProvider) selectAssociatedValues(tenant, name string) (map[string][]string, error) {
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

	cp.cache.SetAllAssociatedValues(tenant, name, values)

	return values, nil
}

// IsMutableLabel returns whether the label is mutable.
func (cp *CassandraProvider) IsMutableLabel(name string) (bool, error) {
	mutableLabelNames, found := cp.cache.MutableLabelNames()
	if !found {
		associatedNames, err := cp.selectAssociatedNames()
		if err != nil {
			return false, err
		}

		for name := range associatedNames {
			mutableLabelNames = append(mutableLabelNames, name)
		}
	}

	for _, mutName := range mutableLabelNames {
		if name == mutName {
			return true, nil
		}
	}

	return false, nil
}

// WriteLabelValues writes the label values to Cassandra.
func (cp *CassandraProvider) WriteLabelValues(ctx context.Context, lbls []LabelWithValues) error {
	keys := make([]cacheKey, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.insertAssociatedValues(label); err != nil {
			return err
		}

		keys = append(keys, cacheKey{Tenant: label.Tenant, Name: label.Name})
	}

	cp.cache.InvalidateAssociatedValues(ctx, keys)

	return nil
}

// insertAssociatedValues inserts or modifies the non mutable label values associated to a mutable label.
func (cp *CassandraProvider) insertAssociatedValues(label LabelWithValues) error {
	query := cp.session.Query(`
		INSERT INTO mutable_label_values (tenant, name, value, associated_values)
		VALUES (?, ?, ?, ?)
	`, label.Tenant, label.Name, label.Value, label.AssociatedValues)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated values: %w", err)
	}

	return nil
}

// DeleteLabelValues deletes label values in Cassandra.
func (cp *CassandraProvider) DeleteLabelValues(ctx context.Context, lbls []Label) error {
	keys := make([]cacheKey, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.deleteAssociatedValues(label); err != nil {
			return err
		}

		keys = append(keys, cacheKey{Tenant: label.Tenant, Name: label.Name})
	}

	cp.cache.InvalidateAssociatedValues(ctx, keys)

	return nil
}

// deleteAssociatedValues deletes the non mutable label values associated to a mutable label.
func (cp *CassandraProvider) deleteAssociatedValues(label Label) error {
	query := cp.session.Query(`
		DELETE FROM mutable_label_values
		WHERE tenant = ? AND name = ? AND value = ?
	`, label.Tenant, label.Name, label.Value)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("delete associated values: %w", err)
	}

	return nil
}

// WriteLabelNames writes the label names to Cassandra.
func (cp *CassandraProvider) WriteLabelNames(ctx context.Context, lbls []LabelWithName) error {
	for _, label := range lbls {
		if err := cp.insertAssociatedName(label); err != nil {
			return err
		}
	}

	cp.cache.InvalidateAssociatedNames(ctx)

	return nil
}

// insertAssociatedName inserts or modifies the non mutable label name associated to a mutable label name.
func (cp *CassandraProvider) insertAssociatedName(label LabelWithName) error {
	query := cp.session.Query(`
		INSERT INTO mutable_label_names (name, associated_name)
		VALUES (?, ?)
	`, label.Name, label.AssociatedName)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated name: %w", err)
	}

	return nil
}

// DeleteLabelNames deletes mutable label names in Cassandra.
func (cp *CassandraProvider) DeleteLabelNames(ctx context.Context, names []string) error {
	for _, name := range names {
		if err := cp.deleteAssociatedName(name); err != nil {
			return err
		}
	}

	cp.cache.InvalidateAssociatedNames(ctx)

	return nil
}

// deleteAssociatedName deletes a mutable label name.
func (cp *CassandraProvider) deleteAssociatedName(name string) error {
	query := cp.session.Query(`
		DELETE FROM mutable_label_names
		WHERE name = ?
	`, name)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("delete associated name: %w", err)
	}

	return nil
}

package mutable

import (
	"context"
	"errors"
	"fmt"
	"squirreldb/types"

	"github.com/gocql/gocql"
)

var errNoResult = errors.New("no result")

// LabelProvider allows to get non mutable labels from a mutable label.
type LabelProvider interface {
	Get(tenant, name, value string) (NonMutableLabels, error)
	AllValues(tenant, name string) ([]string, error)
	IsMutableLabel(tenant, name string) (bool, error)
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
	Tenant         string `json:"tenant"`
	Name           string `json:"name"`
	AssociatedName string `json:"associated_name"`
}

// LabelKey is a label name and its tenant.
type LabelKey struct {
	Tenant string `json:"tenant"`
	Name   string `json:"name"`
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
func NewCassandraProvider(
	ctx context.Context,
	session *gocql.Session,
	cluster types.Cluster,
) (*CassandraProvider, error) {
	cp := &CassandraProvider{
		session: session,
		cache:   newCache(ctx, cluster),
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

// Get returns the non mutable labels corresponding to a mutable label name and value.
func (cp *CassandraProvider) Get(tenant, name, value string) (NonMutableLabels, error) {
	associatedName, err := cp.associatedName(tenant, name)
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
func (cp *CassandraProvider) associatedName(tenant, name string) (string, error) {
	associatedName, found := cp.cache.AssociatedName(tenant, name)
	if found {
		return associatedName, nil
	}

	// List all the associated names once to set it in cache as the table only has a few rows.
	associatedNames, err := cp.selectAssociatedNames(tenant)
	if err != nil {
		return "", err
	}

	associatedName, found = associatedNames[name]
	if !found {
		return "", fmt.Errorf("%w: no result for name=%s", errNoResult, name)
	}

	return associatedName, nil
}

// selectAssociatedNames returns a map of the non mutable names index by the mutable name.
func (cp *CassandraProvider) selectAssociatedNames(tenant string) (map[string]string, error) {
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

	cp.cache.SetAllAssociatedNames(tenant, associatedNames)

	return associatedNames, nil
}

func (cp *CassandraProvider) associatedValuesByNameAndValue(tenant, name, value string) ([]string, error) {
	associatedValues, found := cp.cache.AssociatedValues(tenant, name, value)
	if found {
		return associatedValues, nil
	}

	err := cp.updateAssociatedValuesCache(tenant, name)
	if err != nil {
		return nil, err
	}

	associatedValues, _ = cp.cache.AssociatedValues(tenant, name, value)

	return associatedValues, nil
}

// AllValues returns all possible mutable label values.
func (cp *CassandraProvider) AllValues(tenant, name string) ([]string, error) {
	values, found := cp.cache.Values(tenant, name)
	if found {
		return values, nil
	}

	err := cp.updateAssociatedValuesCache(tenant, name)
	if err != nil {
		return nil, err
	}

	values, _ = cp.cache.Values(tenant, name)

	return values, nil
}

// updateAssociatedValuesCache updates the associated values cache for a mutable label name and tenant.
func (cp *CassandraProvider) updateAssociatedValuesCache(tenant, name string) error {
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
		return fmt.Errorf("select values: %w", err)
	}

	cp.cache.SetAllAssociatedValues(tenant, name, values)

	return nil
}

// IsMutableLabel returns whether the label is mutable.
func (cp *CassandraProvider) IsMutableLabel(tenant, name string) (bool, error) {
	mutableLabelNames, found := cp.cache.MutableLabelNames(tenant)
	if !found {
		associatedNames, err := cp.selectAssociatedNames(tenant)
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
	// We use a map to to append only distinct values in the label keys.
	used := make(map[string]struct{})
	keys := make([]LabelKey, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.insertAssociatedValues(label); err != nil {
			return err
		}

		if _, found := used[label.Tenant]; !found {
			used[label.Tenant] = struct{}{}

			keys = append(keys, LabelKey{Tenant: label.Tenant, Name: label.Name})
		}
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
	// We use a map to to append only distinct values in the label keys.
	used := make(map[string]struct{})
	keys := make([]LabelKey, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.deleteAssociatedValues(label); err != nil {
			return err
		}

		if _, found := used[label.Tenant]; !found {
			used[label.Tenant] = struct{}{}

			keys = append(keys, LabelKey{Tenant: label.Tenant, Name: label.Name})
		}
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
	// We use a map to to append only distinct values in the tenants.
	used := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.insertAssociatedName(label); err != nil {
			return err
		}

		if _, found := used[label.Tenant]; !found {
			used[label.Tenant] = struct{}{}

			tenants = append(tenants, label.Tenant)
		}
	}

	cp.cache.InvalidateAssociatedNames(ctx, tenants)

	return nil
}

// insertAssociatedName inserts or modifies the non mutable label name associated to a mutable label name.
func (cp *CassandraProvider) insertAssociatedName(label LabelWithName) error {
	query := cp.session.Query(`
		INSERT INTO mutable_label_names (tenant, name, associated_name)
		VALUES (?, ?, ?)
	`, label.Tenant, label.Name, label.AssociatedName)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated name: %w", err)
	}

	return nil
}

// DeleteLabelNames deletes mutable label names in Cassandra.
func (cp *CassandraProvider) DeleteLabelNames(ctx context.Context, labelKeys []LabelKey) error {
	// We use a map to to append only distinct values in the tenants.
	used := make(map[string]struct{})
	tenants := make([]string, 0, len(labelKeys))

	for _, name := range labelKeys {
		if err := cp.deleteAssociatedName(name.Tenant, name.Name); err != nil {
			return err
		}

		if _, found := used[name.Tenant]; !found {
			used[name.Tenant] = struct{}{}

			tenants = append(tenants, name.Tenant)
		}
	}

	cp.cache.InvalidateAssociatedNames(ctx, tenants)

	return nil
}

// deleteAssociatedName deletes a mutable label name.
func (cp *CassandraProvider) deleteAssociatedName(tenant, name string) error {
	query := cp.session.Query(`
		DELETE FROM mutable_label_names
		WHERE tenant = ? AND name = ?
	`, tenant, name)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("delete associated name: %w", err)
	}

	return nil
}

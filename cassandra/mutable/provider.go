package mutable

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"squirreldb/types"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
)

// LabelProvider allows to get non mutable labels from a mutable label.
type LabelProvider interface {
	GetMutable(tenant, name, value string) (labels.Labels, error)
	GetNonMutable(tenant, name, value string) (NonMutableLabels, error)
	AllValues(tenant, name string) ([]string, error)
	MutableLabelNames(tenant string) ([]string, error)
}

// LabelWriter allows to add and delete mutable labels.
type LabelWriter interface {
	WriteLabelValues(ctx context.Context, lbls []LabelWithValues) error
	DeleteLabelValues(ctx context.Context, lbls []Label) error
	WriteLabelNames(ctx context.Context, lbls []LabelWithName) error
	DeleteLabelNames(ctx context.Context, names []LabelKey) error
}

// ProviderAndWriter allows to get and write mutable labels.
type ProviderAndWriter interface {
	LabelProvider
	LabelWriter
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
	reg prometheus.Registerer,
	session *gocql.Session,
	cluster types.Cluster,
) (*CassandraProvider, error) {
	cp := &CassandraProvider{
		session: session,
		cache:   newCache(ctx, reg, cluster),
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

// GetNonMutable returns the non mutable labels corresponding to a mutable label name and value.
func (cp *CassandraProvider) GetNonMutable(tenant, name, value string) (NonMutableLabels, error) {
	associatedName, err := cp.nonMutableName(tenant, name)
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

// nonMutableName returns the non mutable label name associated to a mutable label name and tenant.
func (cp *CassandraProvider) nonMutableName(tenant, name string) (string, error) {
	nonMutableName, found := cp.cache.NonMutableName(tenant, name)
	if found {
		if nonMutableName == "" {
			return "", fmt.Errorf("%w: tenant=%s, name=%s", ErrNoResult, tenant, name)
		}

		return nonMutableName, nil
	}

	// List all the associated names for a tenant once to set it in cache as the table only has a few rows.
	if err := cp.updateAssociatedNames(tenant); err != nil {
		return "", err
	}

	nonMutableName, _ = cp.cache.NonMutableName(tenant, name)
	if nonMutableName == "" {
		return "", fmt.Errorf("%w: tenant=%s, name=%s", ErrNoResult, tenant, name)
	}

	return nonMutableName, nil
}

// updateAssociatedNames updates the associated names cache.
func (cp *CassandraProvider) updateAssociatedNames(tenant string) error {
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
		return fmt.Errorf("select labels: %w", err)
	}

	cp.cache.SetAllAssociatedNames(tenant, associatedNames)

	return nil
}

func (cp *CassandraProvider) associatedValuesByNameAndValue(tenant, name, value string) ([]string, error) {
	associatedValues, found := cp.cache.AssociatedValues(tenant, name, value)
	if found {
		if len(associatedValues) == 0 {
			return nil, fmt.Errorf("%w: tenant=%s, name=%s, value=%s", ErrNoResult, tenant, name, value)
		}

		return associatedValues, nil
	}

	err := cp.updateAssociatedValuesCache(tenant, name)
	if err != nil {
		return nil, err
	}

	associatedValues, _ = cp.cache.AssociatedValues(tenant, name, value)
	if len(associatedValues) == 0 {
		return nil, fmt.Errorf("%w: tenant=%s, name=%s, value=%s", ErrNoResult, tenant, name, value)
	}

	return associatedValues, nil
}

// AllValues returns all possible mutable label values for a tenant and a label name.
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

// MutableLabelNames returns all the mutable label names possible for a tenant.
func (cp *CassandraProvider) MutableLabelNames(tenant string) ([]string, error) {
	mutableLabelNames, found := cp.cache.AllMutableLabelNames(tenant)
	if !found {
		if err := cp.updateAssociatedNames(tenant); err != nil {
			return nil, err
		}

		mutableLabelNames, _ = cp.cache.AllMutableLabelNames(tenant)
	}

	return mutableLabelNames, nil
}

// WriteLabelValues writes the label values to Cassandra.
func (cp *CassandraProvider) WriteLabelValues(ctx context.Context, lbls []LabelWithValues) error {
	// We use a map to to append only distinct values in the label keys.
	usedKeys := make(map[LabelKey]struct{})
	keys := make([]LabelKey, 0, len(lbls))
	usedTenants := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.insertAssociatedValues(label); err != nil {
			return err
		}

		key := LabelKey{Tenant: label.Tenant, Name: label.Name}
		if _, found := usedKeys[key]; !found {
			usedKeys[key] = struct{}{}

			keys = append(keys, key)
		}

		if _, found := usedTenants[label.Tenant]; !found {
			usedTenants[label.Tenant] = struct{}{}

			tenants = append(tenants, label.Tenant)
		}
	}

	cp.cache.InvalidateAssociatedValues(ctx, keys)
	cp.cache.InvalidateMutableLabels(ctx, tenants)

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
	usedKeys := make(map[LabelKey]struct{})
	keys := make([]LabelKey, 0, len(lbls))
	usedTenants := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.deleteAssociatedValues(label); err != nil {
			return err
		}

		key := LabelKey{Tenant: label.Tenant, Name: label.Name}
		if _, found := usedKeys[key]; !found {
			usedKeys[key] = struct{}{}

			keys = append(keys, key)
		}

		if _, found := usedTenants[label.Tenant]; !found {
			usedTenants[label.Tenant] = struct{}{}

			tenants = append(tenants, label.Tenant)
		}
	}

	cp.cache.InvalidateAssociatedValues(ctx, keys)
	cp.cache.InvalidateMutableLabels(ctx, tenants)

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
	cp.cache.InvalidateMutableLabels(ctx, tenants)

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
	cp.cache.InvalidateMutableLabels(ctx, tenants)

	return nil
}

// deleteAssociatedName deletes a mutable label name.
func (cp *CassandraProvider) deleteAssociatedName(tenant, name string) error {
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

// GetMutable returns the mutable labels corresponding to a non mutable label name and value.
func (cp *CassandraProvider) GetMutable(tenant, name, value string) (labels.Labels, error) {
	mutableLabels, found := cp.cache.MutableLabels(tenant, name, value)
	if found {
		return mutableLabels, nil
	}

	mutableNames, err := cp.mutableNames(tenant, name)
	if err != nil {
		return nil, err
	}

	// TODO: Maybe the rest of the function could be done by the label processor instead so it can be tested.
	for _, mutableName := range mutableNames {
		mutableValue, err := cp.getMutableValue(tenant, mutableName, value)
		if err != nil {
			if errors.Is(err, ErrNoResult) {
				continue
			}

			return nil, err
		}

		mutableLabel := labels.Label{
			Name:  mutableName,
			Value: mutableValue,
		}

		mutableLabels = append(mutableLabels, mutableLabel)
	}

	cp.cache.SetMutableLabels(tenant, name, value, mutableLabels)

	return mutableLabels, nil
}

// getMutableValue returns the first mutable label value the given value belongs to.
func (cp *CassandraProvider) getMutableValue(tenant, mutableName, nonMutableValue string) (string, error) {
	mutableValues, err := cp.AllValues(tenant, mutableName)
	if err != nil {
		return "", err
	}

	// Sort the values to make sure we always loop through the values in the same order
	// so we return the same value while the mutable labels don't change.
	sort.Strings(mutableValues)

	// For each possible mutable value, search if the value is in the non mutable values associated.
	for _, mutableValue := range mutableValues {
		nonMutableLabels, err := cp.GetNonMutable(tenant, mutableName, mutableValue)
		if err != nil {
			return "", err
		}

		for _, value := range nonMutableLabels.Values {
			if nonMutableValue == value {
				return mutableValue, nil
			}
		}
	}

	return "", ErrNoResult
}

// mutableName returns the mutable label names associated to a non mutable label name.
func (cp *CassandraProvider) mutableNames(tenant, name string) ([]string, error) {
	mutableNames, found := cp.cache.MutableLabelNames(tenant, name)
	if found {
		return mutableNames, nil
	}

	// List all the associated names for a tenant once to set it in cache as the table only has a few rows.
	if err := cp.updateAssociatedNames(tenant); err != nil {
		return nil, err
	}

	mutableNames, _ = cp.cache.MutableLabelNames(tenant, name)

	return mutableNames, nil
}

package mutable

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"squirreldb/types"

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

// Provider allows to get mutable labels.
type Provider struct {
	store Store
	cache *cache
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

// NewProvider returns a new mutable label provider.
func NewProvider(
	ctx context.Context,
	reg prometheus.Registerer,
	cluster types.Cluster,
	store Store,
) *Provider {
	cp := &Provider{
		store: store,
		cache: newCache(ctx, reg, cluster),
	}

	return cp
}

// GetNonMutable returns the non mutable labels corresponding to a mutable label name and value.
func (cp *Provider) GetNonMutable(tenant, name, value string) (NonMutableLabels, error) {
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
func (cp *Provider) nonMutableName(tenant, name string) (string, error) {
	nonMutableName, found := cp.cache.NonMutableName(tenant, name)
	if found {
		if nonMutableName == "" {
			return "", fmt.Errorf("%w: tenant=%s, name=%s", ErrNoResult, tenant, name)
		}

		return nonMutableName, nil
	}

	// List all the associated names for a tenant once to set it in cache as the table only has a few rows.
	if err := cp.updateAssociatedNamesCache(tenant); err != nil {
		return "", err
	}

	nonMutableName, _ = cp.cache.NonMutableName(tenant, name)
	if nonMutableName == "" {
		return "", fmt.Errorf("%w: tenant=%s, name=%s", ErrNoResult, tenant, name)
	}

	return nonMutableName, nil
}

func (cp *Provider) updateAssociatedNamesCache(tenant string) error {
	associatedNames, err := cp.store.AssociatedNames(tenant)
	if err != nil {
		return err
	}

	cp.cache.SetAllAssociatedNames(tenant, associatedNames)

	return nil
}

func (cp *Provider) associatedValuesByNameAndValue(tenant, name, value string) ([]string, error) {
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

func (cp *Provider) updateAssociatedValuesCache(tenant, name string) error {
	associatedValues, err := cp.store.AssociatedValues(tenant, name)
	if err != nil {
		return err
	}

	cp.cache.SetAllAssociatedValues(tenant, name, associatedValues)

	return nil
}

// AllValues returns all possible mutable label values for a tenant and a label name.
func (cp *Provider) AllValues(tenant, name string) ([]string, error) {
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

// MutableLabelNames returns all the mutable label names possible for a tenant.
func (cp *Provider) MutableLabelNames(tenant string) ([]string, error) {
	mutableLabelNames, found := cp.cache.AllMutableLabelNames(tenant)
	if !found {
		if err := cp.updateAssociatedNamesCache(tenant); err != nil {
			return nil, err
		}

		mutableLabelNames, _ = cp.cache.AllMutableLabelNames(tenant)
	}

	return mutableLabelNames, nil
}

// WriteLabelValues writes the label values to the store.
//nolint:dupl // DeleteLabelValues doesn't use the same type of labels.
func (cp *Provider) WriteLabelValues(ctx context.Context, lbls []LabelWithValues) error {
	// We use a map to to append only distinct values in the label keys.
	usedKeys := make(map[LabelKey]struct{})
	keys := make([]LabelKey, 0, len(lbls))
	usedTenants := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.store.SetAssociatedValues(label); err != nil {
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

// DeleteLabelValues deletes label values in the store.
//nolint:dupl // WriteLabelValues doesn't use the same type of labels.
func (cp *Provider) DeleteLabelValues(ctx context.Context, lbls []Label) error {
	// We use a map to to append only distinct values in the label keys.
	usedKeys := make(map[LabelKey]struct{})
	keys := make([]LabelKey, 0, len(lbls))
	usedTenants := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.store.DeleteAssociatedValues(label); err != nil {
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

// WriteLabelNames writes the label names to the store.
func (cp *Provider) WriteLabelNames(ctx context.Context, lbls []LabelWithName) error {
	// We use a map to to append only distinct values in the tenants.
	used := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.store.SetAssociatedName(label); err != nil {
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

// DeleteLabelNames deletes mutable label names in the store.
func (cp *Provider) DeleteLabelNames(ctx context.Context, labelKeys []LabelKey) error {
	// We use a map to to append only distinct values in the tenants.
	used := make(map[string]struct{})
	tenants := make([]string, 0, len(labelKeys))

	for _, name := range labelKeys {
		if err := cp.store.DeleteAssociatedName(name.Tenant, name.Name); err != nil {
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

// GetMutable returns the mutable labels corresponding to a non mutable label name and value.
func (cp *Provider) GetMutable(tenant, name, value string) (labels.Labels, error) {
	mutableLabels, found := cp.cache.MutableLabels(tenant, name, value)
	if found {
		return mutableLabels, nil
	}

	mutableNames, err := cp.mutableNames(tenant, name)
	if err != nil {
		return nil, err
	}

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
func (cp *Provider) getMutableValue(tenant, mutableName, nonMutableValue string) (string, error) {
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
func (cp *Provider) mutableNames(tenant, name string) ([]string, error) {
	mutableNames, found := cp.cache.MutableLabelNames(tenant, name)
	if found {
		return mutableNames, nil
	}

	// List all the associated names for a tenant once to set it in cache as the table only has a few rows.
	if err := cp.updateAssociatedNamesCache(tenant); err != nil {
		return nil, err
	}

	mutableNames, _ = cp.cache.MutableLabelNames(tenant, name)

	return mutableNames, nil
}

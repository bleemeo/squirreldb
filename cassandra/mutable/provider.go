package mutable

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

var errWrongCSVFormat = errors.New("wrong CSV format")

// LabelProvider allows to get non mutable labels from a mutable label.
type LabelProvider interface {
	GetMutable(ctx context.Context, tenant, name, value string) (labels.Labels, error)
	GetNonMutable(ctx context.Context, tenant, name, value string) (NonMutableLabels, error)
	AllValues(ctx context.Context, tenant, name string) ([]string, error)
	MutableLabelNames(ctx context.Context, tenant string) ([]string, error)
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

// Label represents a mutable label with its value.
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
	logger zerolog.Logger,
) *Provider {
	cp := &Provider{
		store: store,
		cache: newCache(ctx, reg, cluster, logger),
	}

	return cp
}

// GetNonMutable returns the non mutable labels corresponding to a mutable label name and value.
func (cp *Provider) GetNonMutable(ctx context.Context, tenant, name, value string) (NonMutableLabels, error) {
	associatedName, err := cp.nonMutableName(ctx, tenant, name)
	if err != nil {
		return NonMutableLabels{}, err
	}

	associatedValues, err := cp.associatedValuesByNameAndValue(ctx, tenant, name, value)
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
func (cp *Provider) nonMutableName(ctx context.Context, tenant, name string) (string, error) {
	nonMutableName, found := cp.cache.NonMutableName(tenant, name)
	if found {
		if nonMutableName == "" {
			return "", fmt.Errorf("%w: tenant=%s, name=%s", errNoResult, tenant, name)
		}

		return nonMutableName, nil
	}

	// List all the associated names for a tenant once to set it in cache as the table only has a few rows.
	if err := cp.updateAssociatedNamesCache(ctx, tenant); err != nil {
		return "", err
	}

	nonMutableName, _ = cp.cache.NonMutableName(tenant, name)
	if nonMutableName == "" {
		return "", fmt.Errorf("%w: tenant=%s, name=%s", errNoResult, tenant, name)
	}

	return nonMutableName, nil
}

func (cp *Provider) updateAssociatedNamesCache(ctx context.Context, tenant string) error {
	associatedNames, err := cp.store.AssociatedNames(ctx, tenant)
	if err != nil {
		return err
	}

	cp.cache.SetAllAssociatedNames(tenant, associatedNames)

	return nil
}

func (cp *Provider) associatedValuesByNameAndValue(ctx context.Context, tenant, name, value string) ([]string, error) {
	associatedValues, found := cp.cache.AssociatedValues(tenant, name, value)
	if found {
		return associatedValues, nil
	}

	err := cp.updateAssociatedValuesCache(ctx, tenant, name)
	if err != nil {
		return nil, err
	}

	associatedValues, _ = cp.cache.AssociatedValues(tenant, name, value)

	return associatedValues, nil
}

func (cp *Provider) updateAssociatedValuesCache(ctx context.Context, tenant, name string) error {
	associatedValues, err := cp.store.AssociatedValues(ctx, tenant, name)
	if err != nil {
		return err
	}

	cp.cache.SetAllAssociatedValues(tenant, name, associatedValues)

	return nil
}

// AllValues returns all possible mutable label values for a tenant and a label name.
func (cp *Provider) AllValues(ctx context.Context, tenant, name string) ([]string, error) {
	values, found := cp.cache.Values(tenant, name)
	if found {
		return values, nil
	}

	err := cp.updateAssociatedValuesCache(ctx, tenant, name)
	if err != nil {
		return nil, err
	}

	values, _ = cp.cache.Values(tenant, name)

	return values, nil
}

// MutableLabelNames returns all the mutable label names possible for a tenant.
func (cp *Provider) MutableLabelNames(ctx context.Context, tenant string) ([]string, error) {
	mutableLabelNames, found := cp.cache.AllMutableLabelNames(tenant)
	if !found {
		if err := cp.updateAssociatedNamesCache(ctx, tenant); err != nil {
			return nil, err
		}

		mutableLabelNames, _ = cp.cache.AllMutableLabelNames(tenant)
	}

	return mutableLabelNames, nil
}

// WriteLabelValues writes the label values to the store.
func (cp *Provider) WriteLabelValues(ctx context.Context, lbls []LabelWithValues) error {
	// We use a map to append only distinct values in the label keys.
	usedKeys := make(map[LabelKey]struct{})
	keys := make([]LabelKey, 0, len(lbls))
	usedTenants := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		// Delete the associated values if the list is empty.
		if len(label.AssociatedValues) == 0 {
			labelToDelete := Label{
				Tenant: label.Tenant,
				Name:   label.Name,
				Value:  label.Value,
			}

			if err := cp.store.DeleteAssociatedValues(ctx, labelToDelete); err != nil {
				return err
			}
		} else {
			if err := cp.store.SetAssociatedValues(ctx, label); err != nil {
				return err
			}
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
func (cp *Provider) DeleteLabelValues(ctx context.Context, lbls []Label) error {
	// We use a map to append only distinct values in the label keys.
	usedKeys := make(map[LabelKey]struct{})
	keys := make([]LabelKey, 0, len(lbls))
	usedTenants := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.store.DeleteAssociatedValues(ctx, label); err != nil {
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
	// We use a map to append only distinct values in the tenants.
	used := make(map[string]struct{})
	tenants := make([]string, 0, len(lbls))

	for _, label := range lbls {
		if err := cp.store.SetAssociatedName(ctx, label); err != nil {
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
	// We use a map to append only distinct values in the tenants.
	used := make(map[string]struct{})
	tenants := make([]string, 0, len(labelKeys))

	for _, name := range labelKeys {
		if err := cp.store.DeleteAssociatedName(ctx, name.Tenant, name.Name); err != nil {
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
func (cp *Provider) GetMutable(ctx context.Context, tenant, name, value string) (labels.Labels, error) {
	mutableLabels, found := cp.cache.MutableLabels(tenant, name, value)
	if found {
		return mutableLabels, nil
	}

	mutableNames, err := cp.mutableNames(ctx, tenant, name)
	if err != nil {
		return nil, err
	}

	for _, mutableName := range mutableNames {
		mutableValue, err := cp.getMutableValue(ctx, tenant, mutableName, value)
		if err != nil {
			if errors.Is(err, errNoResult) {
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
func (cp *Provider) getMutableValue(ctx context.Context, tenant, mutableName, nonMutableValue string) (string, error) {
	mutableValues, err := cp.AllValues(ctx, tenant, mutableName)
	if err != nil {
		return "", err
	}

	// Sort the values to make sure we always loop through the values in the same order
	// so we return the same value while the mutable labels don't change.
	sort.Strings(mutableValues)

	// For each possible mutable value, search if the value is in the non mutable values associated.
	for _, mutableValue := range mutableValues {
		nonMutableLabels, err := cp.GetNonMutable(ctx, tenant, mutableName, mutableValue)
		if err != nil {
			return "", err
		}

		for _, value := range nonMutableLabels.Values {
			if nonMutableValue == value {
				return mutableValue, nil
			}
		}
	}

	return "", errNoResult
}

// mutableName returns the mutable label names associated to a non mutable label name.
func (cp *Provider) mutableNames(ctx context.Context, tenant, name string) ([]string, error) {
	mutableNames, found := cp.cache.MutableLabelNames(tenant, name)
	if found {
		return mutableNames, nil
	}

	// List all the associated names for a tenant once to set it in cache as the table only has a few rows.
	if err := cp.updateAssociatedNamesCache(ctx, tenant); err != nil {
		return nil, err
	}

	mutableNames, _ = cp.cache.MutableLabelNames(tenant, name)

	return mutableNames, nil
}

// Dump writes a CSV with all mutable labels known.
func (cp *Provider) Dump(ctx context.Context, w io.Writer) error {
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	allMutableLabels, err := cp.store.AllMutableLabels(ctx)
	if err != nil {
		return err
	}

	for _, mutableLabels := range allMutableLabels {
		associatedValues, err := cp.store.AssociatedValues(ctx, mutableLabels.Tenant, mutableLabels.Name)
		if err != nil {
			return err
		}

		// Make sure we always print at least one line. This will allow to create entry in mutable_label_names, which
		// is significant.
		if len(associatedValues) == 0 {
			err := csvWriter.Write([]string{
				mutableLabels.Tenant,
				mutableLabels.Name,
				mutableLabels.AssociatedName,
				"",
				"",
			})
			if err != nil {
				return err
			}
		}

		for mutableValue, nonMutableValues := range associatedValues {
			if len(nonMutableValues) == 0 {
				// This one is also needed:
				// * at least to make previous statement right (we need at least one row per mutableLabels)
				// * create an entry in mutable_label_values with empty list to have same content, even if not significant
				//   (in this table, empty list or no row result in same behavior)
				err := csvWriter.Write([]string{
					mutableLabels.Tenant,
					mutableLabels.Name,
					mutableLabels.AssociatedName,
					mutableValue,
					"",
				})
				if err != nil {
					return err
				}
			}

			for _, nonMutableValue := range nonMutableValues {
				err := csvWriter.Write([]string{
					mutableLabels.Tenant,
					mutableLabels.Name,
					mutableLabels.AssociatedName,
					mutableValue,
					nonMutableValue,
				})
				if err != nil {
					return err
				}
			}
		}
	}

	return ctx.Err()
}

// Import read a CSV with all mutable labels to known. Existing labels are dropped then import is done.
// In output, show some information about import.
func (cp *Provider) Import(ctx context.Context, r io.Reader, w io.Writer, dryRun bool) error {
	start := time.Now()
	csvReader := csv.NewReader(r)
	labelsWithValues := make(map[Label]LabelWithValues)
	uniqueLabelNames := make(map[LabelWithName]bool)

	for {
		row, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		if len(row) != 5 {
			return fmt.Errorf("%w: %v", errWrongCSVFormat, row)
		}

		lwn := LabelWithName{
			Tenant:         row[0],
			Name:           row[1],
			AssociatedName: row[2],
		}
		uniqueLabelNames[lwn] = true

		lbl := Label{
			Tenant: lwn.Tenant,
			Name:   lwn.Name,
			Value:  row[3],
		}
		associatedValue := row[4]

		if lbl.Value == "" {
			// only entry in mutable_label_names tables
			continue
		}

		lwv, ok := labelsWithValues[lbl]
		if !ok {
			lwv = LabelWithValues{
				Tenant: lbl.Tenant,
				Name:   lbl.Name,
				Value:  lbl.Value,
			}
			labelsWithValues[lbl] = lwv
		}

		if associatedValue == "" {
			// empty list entry in mutable_label_values
			continue
		}

		lwv.AssociatedValues = append(lwv.AssociatedValues, associatedValue)
		labelsWithValues[lbl] = lwv
	}

	allMutableLabels, err := cp.store.AllMutableLabels(ctx)
	if err != nil {
		return err
	}

	namesToDeleteMap := make(map[LabelKey]bool)

	for _, k := range allMutableLabels {
		key := LabelKey{
			Tenant: k.Tenant,
			Name:   k.Name,
		}

		namesToDeleteMap[key] = true
	}

	namesToDelete := make([]LabelKey, 0, len(namesToDeleteMap))

	for k := range namesToDeleteMap {
		namesToDelete = append(namesToDelete, k)
	}

	if dryRun {
		fmt.Fprintf(w, "dry-run is enabled, not modification will be done\n")
	}

	fmt.Fprintf(w, "Dropping existing labels, %d couple tenant/label to delete\n", len(namesToDelete))

	if !dryRun {
		if err := cp.DeleteLabelNames(ctx, namesToDelete); err != nil {
			return err
		}
	}

	{
		fmt.Fprintf(w, "Adding %d label names\n", len(uniqueLabelNames))

		workList := make([]LabelWithName, 0, len(uniqueLabelNames))

		for k := range uniqueLabelNames {
			workList = append(workList, k)
		}

		if !dryRun {
			if err := cp.WriteLabelNames(ctx, workList); err != nil {
				return err
			}
		}
	}

	{
		fmt.Fprintf(w, "Adding %d label values\n", len(labelsWithValues))

		workList := make([]LabelWithValues, 0, len(labelsWithValues))

		for _, v := range labelsWithValues {
			workList = append(workList, v)
		}

		if !dryRun {
			if err := cp.WriteLabelValues(ctx, workList); err != nil {
				return err
			}
		}
	}

	fmt.Fprintf(w, "Everything is done in %v\n", time.Since(start))

	return nil
}

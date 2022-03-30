package dummy

import (
	"context"
	"errors"
	"sort"
	"squirreldb/cassandra/mutable"

	"github.com/prometheus/prometheus/model/labels"
)

// MockLabelProvider is a label provider which gets its labels from hardcoded values.
type MockLabelProvider struct {
	labels MutableLabels
}

// MutableLabels stores all mutable labels in the dummy label provider.
type MutableLabels map[mutable.LabelKey]map[string]mutable.NonMutableLabels

// DefaultMutableLabels contains some mutable labels that can be used in tests.
var DefaultMutableLabels = MutableLabels{ //nolint:gochecknoglobals
	{
		Tenant: "1234",
		Name:   "group",
	}: {
		"group1": mutable.NonMutableLabels{
			Name:   "instance",
			Values: []string{"server1", "server2", "server3"},
		},
		"group2": mutable.NonMutableLabels{
			Name:   "instance",
			Values: []string{"server2", "server3"},
		},
		"group3": mutable.NonMutableLabels{
			Name:   "instance",
			Values: []string{"server4"},
		},
	},
	{
		Tenant: "1234",
		Name:   "environment",
	}: {
		"prod": mutable.NonMutableLabels{
			Name:   "instance",
			Values: []string{"server4"},
		},
	},
	{
		Tenant: "5678",
		Name:   "group",
	}: {
		"group10": mutable.NonMutableLabels{
			Name:   "instance",
			Values: []string{"server10", "server11"},
		},
	},
}

// NewMutableLabelProvider returns a mock label provider.
func NewMutableLabelProvider(lbls MutableLabels) MockLabelProvider {
	if lbls == nil {
		lbls = make(MutableLabels)
	}

	return MockLabelProvider{labels: lbls}
}

// GetNonMutable returns the non mutable labels corresponding to a mutable label name and value.
func (lp MockLabelProvider) GetNonMutable(tenant, name, value string) (mutable.NonMutableLabels, error) {
	key := mutable.LabelKey{
		Tenant: tenant,
		Name:   name,
	}

	lbls, found := lp.labels[key]
	if !found {
		return mutable.NonMutableLabels{}, mutable.ErrNoResult
	}

	nonMutableLabels, found := lbls[value]
	if !found {
		return mutable.NonMutableLabels{}, mutable.ErrNoResult
	}

	return nonMutableLabels, nil
}

// GetMutable returns the mutable labels corresponding to a non mutable label name and value.
func (lp MockLabelProvider) GetMutable(tenant, name, value string) (labels.Labels, error) {
	var mutableLabels labels.Labels

	type label struct {
		mutableValue     string
		nonMutableLabels mutable.NonMutableLabels
	}

outer:

	for labelKey, labelsMap := range lp.labels {
		if labelKey.Tenant != tenant {
			continue
		}

		// We need to sort the labels in a slice to loop over them always in the same order.
		lbls := make([]label, 0, len(labelsMap))
		for mutableValue, nonMutableLabels := range labelsMap {
			l := label{
				mutableValue:     mutableValue,
				nonMutableLabels: nonMutableLabels,
			}

			lbls = append(lbls, l)
		}

		sort.Slice(lbls, func(i, j int) bool {
			return lbls[i].mutableValue < lbls[j].mutableValue
		})

		for _, l := range lbls {
			if name != l.nonMutableLabels.Name {
				continue
			}

			for _, nonMutableValue := range l.nonMutableLabels.Values {
				if value == nonMutableValue {
					mutableLabel := labels.Label{
						Name:  labelKey.Name,
						Value: l.mutableValue,
					}

					mutableLabels = append(mutableLabels, mutableLabel)

					continue outer
				}
			}
		}
	}

	return mutableLabels, nil
}

// AllValues returns all possible mutable label values for a tenant and a label name.
func (lp MockLabelProvider) AllValues(tenant, name string) ([]string, error) {
	key := mutable.LabelKey{
		Tenant: tenant,
		Name:   name,
	}

	lbls, found := lp.labels[key]
	if !found {
		return nil, mutable.ErrNoResult
	}

	keys := make([]string, 0, len(lbls))
	for k := range lbls {
		keys = append(keys, k)
	}

	// Always return the keys in the same orders.
	sort.Strings(keys)

	return keys, nil
}

// IsMutableLabel returns whether the label is mutable.
func (lp MockLabelProvider) IsMutableLabel(tenant, name string) (bool, error) {
	key := mutable.LabelKey{
		Tenant: tenant,
		Name:   name,
	}
	_, found := lp.labels[key]

	return found, nil
}

// MutableLabelNames returns all the mutable label names possible for a tenant.
func (lp MockLabelProvider) MutableLabelNames(tenant string) ([]string, error) {
	var names []string

	for key := range lp.labels {
		if key.Tenant == tenant {
			names = append(names, key.Name)
		}
	}

	return names, nil
}

// Implement LabelWriter methods.
var errNotImplemented = errors.New("not implemented")

func (lp MockLabelProvider) WriteLabelValues(ctx context.Context, lbls []mutable.LabelWithValues) error {
	return errNotImplemented
}

func (lp MockLabelProvider) DeleteLabelValues(ctx context.Context, lbls []mutable.Label) error {
	return errNotImplemented
}

func (lp MockLabelProvider) WriteLabelNames(ctx context.Context, lbls []mutable.LabelWithName) error {
	return errNotImplemented
}

func (lp MockLabelProvider) DeleteLabelNames(ctx context.Context, names []mutable.LabelKey) error {
	return errNotImplemented
}

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

func (lp MockLabelProvider) GetMutable(tenant, name, value string) (labels.Label, error) {
	// TODO: Implement and test.
	return labels.Label{}, errNotImplemented
}

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

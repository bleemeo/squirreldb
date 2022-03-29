package dummy

import (
	"sort"
	"squirreldb/cassandra/mutable"
)

// MockLabelProvider is a label provider which gets its labels from hardcoded values.
type MockLabelProvider struct {
	labels MutableLabels
}

// MutableLabels stores all mutable labels in the dummy label provider.
type MutableLabels map[mutable.LabelKey]map[string]mutable.NonMutableLabels

//nolint:gochecknoglobals
var DefaultMutableLabels = MutableLabels{
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

// Get returns the non mutable labels corresponding to a mutable label name and value.
func (lp MockLabelProvider) Get(tenant, name, value string) (mutable.NonMutableLabels, error) {
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
	return name == "group", nil
}

package dummy

import (
	"sort"
	"squirreldb/cassandra/mutable"
)

// MockLabelProvider is a label provider which gets its labels from hardcoded values.
type MockLabelProvider struct{}

// NewMutableLabelProvider returns a mock label provider.
func NewMutableLabelProvider() MockLabelProvider {
	return MockLabelProvider{}
}

func (lp MockLabelProvider) mutableLabels(tenant, name string) map[string]mutable.NonMutableLabels {
	type key struct {
		tenant string
		name   string
	}

	lbls := map[key]map[string]mutable.NonMutableLabels{
		{
			tenant: "1234",
			name:   "group",
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

	return lbls[key{tenant: tenant, name: name}]
}

func (lp MockLabelProvider) Get(tenant, name, value string) (mutable.NonMutableLabels, error) {
	ls := lp.mutableLabels(tenant, name)[value]

	return ls, nil
}

func (lp MockLabelProvider) AllValues(tenant, name string) ([]string, error) {
	ls := lp.mutableLabels(tenant, name)

	keys := make([]string, 0, len(ls))
	for k := range ls {
		keys = append(keys, k)
	}

	// Always return the keys in the same orders.
	sort.Strings(keys)

	return keys, nil
}

// IsMutableLabel returns whether the label is mutable.
func (lp MockLabelProvider) IsMutableLabel(name string) (bool, error) {
	return name == "group", nil
}

// IsTenantLabel returns whether this label identifies the tenant.
func (lp MockLabelProvider) IsTenantLabel(name string) bool {
	return name == "__account_id"
}

package dummy

import (
	"errors"
	"squirreldb/cassandra/mutable"
)

// mutableLabelStore is a label provider which gets its labels from hardcoded values.
type mutableLabelStore struct {
	labels MutableLabels
}

// MutableLabels stores all mutable labels in the dummy label provider.
type MutableLabels struct {
	// map[tenant][mutable label name] -> associated non mutable label name.
	associatedNames map[string]map[string]string
	// map[tenant, mutable label name][mutable label value] -> associated non mutable label values.
	associatedValues map[mutable.LabelKey]map[string][]string
}

var errNotImplemented = errors.New("not implemented")

// DefaultMutableLabels contains some mutable labels that can be used in tests.
var DefaultMutableLabels = MutableLabels{ //nolint:gochecknoglobals
	associatedNames: map[string]map[string]string{
		"1234": {
			"group":       "instance",
			"environment": "instance",
		},
		"5678": {
			"group": "instance",
		},
	},
	associatedValues: map[mutable.LabelKey]map[string][]string{
		{
			Tenant: "1234",
			Name:   "group",
		}: {
			"group1": []string{"server1", "server2", "server3"},
			"group2": []string{"server2", "server3"},
			"group3": []string{"server4"},
		},
		{
			Tenant: "1234",
			Name:   "environment",
		}: {
			"prod": []string{"server4"},
		},
		{
			Tenant: "5678",
			Name:   "group",
		}: {
			"group10": []string{"server10", "server11"},
		},
	},
}

// NewMutableLabelStore returns a mock label store pre filled with labels.
func NewMutableLabelStore(lbls MutableLabels) mutable.Store {
	if lbls.associatedNames == nil {
		lbls.associatedNames = make(map[string]map[string]string)
	}

	if lbls.associatedValues == nil {
		lbls.associatedValues = make(map[mutable.LabelKey]map[string][]string)
	}

	return mutableLabelStore{labels: lbls}
}

func (s mutableLabelStore) AssociatedNames(tenant string) (map[string]string, error) {
	associatedNames := s.labels.associatedNames[tenant]

	return associatedNames, nil
}

func (s mutableLabelStore) AssociatedValues(tenant, name string) (map[string][]string, error) {
	key := mutable.LabelKey{
		Tenant: tenant,
		Name:   name,
	}

	associatedValues := s.labels.associatedValues[key]

	return associatedValues, nil
}

func (s mutableLabelStore) DeleteAssociatedName(tenant, name string) error {
	return errNotImplemented
}

func (s mutableLabelStore) DeleteAssociatedValues(label mutable.Label) error {
	return errNotImplemented
}

func (s mutableLabelStore) SetAssociatedName(label mutable.LabelWithName) error {
	return errNotImplemented
}

func (s mutableLabelStore) SetAssociatedValues(label mutable.LabelWithValues) error {
	return errNotImplemented
}

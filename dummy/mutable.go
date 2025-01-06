// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dummy

import (
	"context"
	"errors"

	"github.com/bleemeo/squirreldb/cassandra/mutable"
)

// mutableLabelStore is a label provider which gets its labels from hardcoded values.
type mutableLabelStore struct {
	labels MutableLabels
}

// MutableLabels stores all mutable labels in the dummy label provider.
type MutableLabels struct {
	// map[tenant][mutable label name] -> associated non mutable label name.
	AssociatedNames map[string]map[string]string
	// map[tenant, mutable label name][mutable label value] -> associated non mutable label values.
	AssociatedValues map[mutable.LabelKey]map[string][]string
}

var errNotImplemented = errors.New("not implemented")

// DefaultMutableLabels contains some mutable labels that can be used in tests.
var DefaultMutableLabels = MutableLabels{ //nolint:gochecknoglobals
	AssociatedNames: map[string]map[string]string{
		"1234": {
			"group":       "instance",
			"environment": "instance",
		},
		"5678": {
			"group": "instance",
		},
	},
	AssociatedValues: map[mutable.LabelKey]map[string][]string{
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
	if lbls.AssociatedNames == nil {
		lbls.AssociatedNames = make(map[string]map[string]string)
	}

	if lbls.AssociatedValues == nil {
		lbls.AssociatedValues = make(map[mutable.LabelKey]map[string][]string)
	}

	return mutableLabelStore{labels: lbls}
}

func (s mutableLabelStore) AssociatedNames(_ context.Context, tenant string) (map[string]string, error) {
	associatedNames := s.labels.AssociatedNames[tenant]

	return associatedNames, nil
}

func (s mutableLabelStore) AssociatedValues(_ context.Context, tenant, name string) (map[string][]string, error) {
	key := mutable.LabelKey{
		Tenant: tenant,
		Name:   name,
	}

	associatedValues := s.labels.AssociatedValues[key]

	return associatedValues, nil
}

func (s mutableLabelStore) DeleteAssociatedName(_ context.Context, _, _ string) error {
	return errNotImplemented
}

func (s mutableLabelStore) DeleteAssociatedValues(_ context.Context, _ mutable.Label) error {
	return errNotImplemented
}

func (s mutableLabelStore) SetAssociatedName(_ context.Context, _ mutable.LabelWithName) error {
	return errNotImplemented
}

func (s mutableLabelStore) SetAssociatedValues(_ context.Context, _ mutable.LabelWithValues) error {
	return errNotImplemented
}

func (s mutableLabelStore) AllMutableLabels(_ context.Context) ([]mutable.LabelWithName, error) {
	result := make([]mutable.LabelWithName, 0)

	for tenant, subMap := range s.labels.AssociatedNames {
		for labelName, associatedName := range subMap {
			result = append(result, mutable.LabelWithName{
				Tenant:         tenant,
				Name:           labelName,
				AssociatedName: associatedName,
			})
		}
	}

	return result, nil
}

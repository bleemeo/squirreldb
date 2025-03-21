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

package mutable_test

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/mutable"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/logger"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func BenchmarkGetMutableSmall(b *testing.B)  { benchmarkGetMutable(b, 100, 10, 10) }
func BenchmarkGetMutableMedium(b *testing.B) { benchmarkGetMutable(b, 100, 50, 50) }
func BenchmarkGetMutableBig(b *testing.B)    { benchmarkGetMutable(b, 100, 100, 100) }

func benchmarkGetMutable(b *testing.B, nbUsers, nbLabelsPerUser, nbValuesPerLabel int) {
	b.Helper()

	registry := prometheus.NewRegistry()
	initialData := generateData(nbUsers, nbLabelsPerUser, nbValuesPerLabel)
	store := dummy.NewMutableLabelStore(initialData)
	provider := mutable.NewProvider(b.Context(),
		registry,
		&dummy.LocalCluster{},
		store,
		logger.NewTestLogger(true),
	)

	var searchedTenant, searchedNonMutableName, searchedMutableName string
	for tenant, names := range initialData.AssociatedNames {
		searchedTenant = tenant

		for mutableName, nonMutableName := range names {
			searchedMutableName = mutableName
			searchedNonMutableName = nonMutableName

			break
		}

		break
	}

	var searchedMutableValue, searchedNonMutableValue string

	for key, values := range initialData.AssociatedValues {
		if key.Tenant == searchedTenant && key.Name == searchedMutableName {
			for mutableValue, nonMutableValues := range values {
				searchedMutableValue = mutableValue
				searchedNonMutableValue = nonMutableValues[rand.Intn(len(nonMutableValues))]
			}
		}
	}

	// Don't benchmark the initial data generation.
	b.ResetTimer()

	for range b.N {
		lbls, err := provider.GetMutable(
			b.Context(),
			searchedTenant,
			searchedNonMutableName,
			searchedNonMutableValue,
		)
		if err != nil {
			b.Fatal(err)
		}

		if len(lbls) != 1 {
			b.Fatalf("expected 1 label, got %d", len(lbls))
		}

		if lbls[0].Name != searchedMutableName {
			b.Fatalf("expected name %s, got %s", searchedMutableName, lbls[0].Name)
		}

		if lbls[0].Value != searchedMutableValue {
			b.Fatalf("expected value %s, got %s", searchedMutableValue, lbls[0].Value)
		}
	}
}

func generateData(nbUsers, nbLabelsPerUser, nbValuesPerLabel int) dummy.MutableLabels {
	users := make([]string, 0, nbUsers)

	names := map[string]string{
		"group": "instance",
		"role":  "job",
	}

	associatedNames := make(map[string]map[string]string)

	for range nbUsers {
		user := uuid.New().String()
		users = append(users, user)

		associatedNames[user] = names
	}

	associatedValues := make(map[mutable.LabelKey]map[string][]string)

	for _, user := range users {
		for name := range names {
			key := mutable.LabelKey{
				Tenant: user,
				Name:   name,
			}

			values := make(map[string][]string)

			for j := range nbLabelsPerUser {
				mutableValue := fmt.Sprintf("%s-%d", name, j)

				var nonMutableValues []string
				for range nbValuesPerLabel {
					nonMutableValues = append(nonMutableValues, randomString(20))
				}

				values[mutableValue] = nonMutableValues
			}

			associatedValues[key] = values
		}
	}

	lbls := dummy.MutableLabels{
		AssociatedNames:  associatedNames,
		AssociatedValues: associatedValues,
	}

	return lbls
}

func randomString(length int) string {
	b := make([]byte, length)

	rand.New(rand.NewSource(time.Now().UnixNano())).Read(b)

	return hex.EncodeToString(b)[:length]
}

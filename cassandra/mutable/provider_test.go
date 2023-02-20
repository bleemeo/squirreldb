package mutable_test

import (
	"context"
	"fmt"
	"math/rand"
	"squirreldb/cassandra/mutable"
	"squirreldb/dummy"
	"squirreldb/logger"
	"testing"

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
	provider := mutable.NewProvider(context.Background(),
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

	for i := 0; i < b.N; i++ {
		lbls, err := provider.GetMutable(
			context.Background(),
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
	var users []string

	names := map[string]string{
		"group": "instance",
		"role":  "job",
	}

	associatedNames := make(map[string]map[string]string)

	for i := 0; i < nbUsers; i++ {
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

			for j := 0; j < nbLabelsPerUser; j++ {
				mutableValue := fmt.Sprintf("%s-%d", name, j)

				var nonMutableValues []string
				for k := 0; k < nbValuesPerLabel; k++ {
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

	rand.Read(b) //nolint:staticcheck // Deprecated.

	return fmt.Sprintf("%x", b)[:length]
}

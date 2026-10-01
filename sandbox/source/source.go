// Copyright 2015-2026 Bleemeo
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

// Package source contains what the sandbox data sources share: the description
// of a generated series (Series) and a storage.Querier serving a list of them.
// The sources themselves (recorded samples, PromQL expressions) live in the
// sub-packages.
package source

import (
	"context"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
)

// Series is a generated series. It has a point every Step milliseconds, aligned
// on the Unix epoch, and its value is computed on demand, so a series is
// stateless and infinite: the same timestamp always yields the same value.
type Series struct {
	Labels labels.Labels
	// Step is the interval between two points, in milliseconds. It must be > 0.
	Step int64
	// Value returns the value at timestamp ts (in milliseconds, a multiple of Step).
	Value func(ctx context.Context, ts int64) float64
}

// Merge returns the union of the given series lists, sorted by labels. When
// the same labels appear in more than one list, the first occurrence wins.
func Merge(lists ...[]Series) []Series {
	seen := make(map[string]struct{})
	result := make([]Series, 0)

	for _, list := range lists {
		for _, s := range list {
			key := s.Labels.String()
			if _, found := seen[key]; found {
				continue
			}

			seen[key] = struct{}{}

			result = append(result, s)
		}
	}

	slices.SortFunc(result, func(a, b Series) int {
		return labels.Compare(a.Labels, b.Labels)
	})

	return result
}

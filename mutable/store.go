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

package mutable

import "context"

// Store is the backend-agnostic persistence interface for mutable labels.
// It is implemented by the Cassandra backend (cassandra/mutable) and the
// ClickHouse backend (clickhouse/mutable).
type Store interface {
	AssociatedNames(ctx context.Context, tenant string) (map[string]string, error)
	AssociatedValues(ctx context.Context, tenant, name string) (map[string][]string, error)
	DeleteAssociatedName(ctx context.Context, tenant, name string) error
	DeleteAssociatedValues(ctx context.Context, label Label) error
	SetAssociatedName(ctx context.Context, label LabelWithName) error
	SetAssociatedValues(ctx context.Context, label LabelWithValues) error
	AllMutableLabels(ctx context.Context) ([]LabelWithName, error)
}
